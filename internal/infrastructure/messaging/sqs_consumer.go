package messaging

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"github.com/d-dionisio/backend-challenge/internal/application"
	"github.com/d-dionisio/backend-challenge/internal/application/ports"
	"github.com/d-dionisio/backend-challenge/internal/domain"
	"go.uber.org/fx"
)

type SQSConsumer struct {
	client         *sqs.Client
	eventsQueueURL string
	settings       ConsumerConfig
	useCase        *application.ProcessWager
	logger         *slog.Logger
}

func NewSQSConsumer(lifecycle fx.Lifecycle, connection Config, settings ConsumerConfig, useCase *application.ProcessWager) (*SQSConsumer, error) {
	if err := settings.validate(); err != nil {
		return nil, err
	}
	c := &SQSConsumer{settings: settings, eventsQueueURL: connection.QueueURL, useCase: useCase, logger: slog.New(slog.NewJSONHandler(os.Stdout, nil))}
	transport := http.DefaultTransport.(*http.Transport).Clone()
	lifecycle.Append(fx.Hook{
		OnStart: func(ctx context.Context) error {
			cfg, err := config.LoadDefaultConfig(ctx, config.WithRegion(connection.Region), config.WithHTTPClient(&http.Client{Transport: transport}))
			if err != nil {
				return errors.New("could not load consumer AWS configuration")
			}
			c.client = sqs.NewFromConfig(cfg, func(o *sqs.Options) {
				if connection.Endpoint != "" {
					o.BaseEndpoint = aws.String(connection.Endpoint)
				}
				o.RetryMaxAttempts = 1
			})
			attributes, err := c.client.GetQueueAttributes(ctx, &sqs.GetQueueAttributesInput{
				QueueUrl: aws.String(settings.QueueURL), AttributeNames: []types.QueueAttributeName{types.QueueAttributeNameFifoQueue, types.QueueAttributeNameRedrivePolicy},
			})
			if err != nil {
				transport.CloseIdleConnections()
				return errors.New("SQS input queue is unavailable or access is denied")
			}
			var redrive struct {
				DeadLetterTargetArn string `json:"deadLetterTargetArn"`
				MaxReceiveCount     int    `json:"maxReceiveCount"`
			}
			if attributes.Attributes["FifoQueue"] != "true" || json.Unmarshal([]byte(attributes.Attributes["RedrivePolicy"]), &redrive) != nil ||
				!strings.HasSuffix(redrive.DeadLetterTargetArn, ".fifo") || redrive.MaxReceiveCount != 5 {
				transport.CloseIdleConnections()
				return errors.New("SQS input requires FIFO DLQ and maxReceiveCount=5")
			}
			return nil
		},
		OnStop: func(context.Context) error { transport.CloseIdleConnections(); return nil },
	})
	return c, nil
}

// Readiness consulta o broker sem receber ou publicar mensagens.
func (c *SQSConsumer) CheckReady(ctx context.Context) error {
	for _, queue := range []string{c.settings.QueueURL, c.eventsQueueURL} {
		if queue == "" {
			continue
		}
		if _, err := c.client.GetQueueAttributes(ctx, &sqs.GetQueueAttributesInput{
			QueueUrl: aws.String(queue), AttributeNames: []types.QueueAttributeName{types.QueueAttributeNameQueueArn},
		}); err != nil {
			return err
		}
	}
	return nil
}

// Uma mensagem por busca: nenhuma mensagem fica aguardando sua vez em
// um lote local enquanto seu visibility timeout já está correndo.
func (c *SQSConsumer) Receive(ctx context.Context) (*types.Message, error) {
	receiveCtx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()
	result, err := c.client.ReceiveMessage(receiveCtx, &sqs.ReceiveMessageInput{
		QueueUrl: aws.String(c.settings.QueueURL), MaxNumberOfMessages: 1, WaitTimeSeconds: 10, VisibilityTimeout: 30,
		MessageSystemAttributeNames: []types.MessageSystemAttributeName{
			types.MessageSystemAttributeNameSenderId, types.MessageSystemAttributeNameMessageGroupId, types.MessageSystemAttributeNameApproximateReceiveCount,
		},
	})
	if err != nil {
		return nil, err
	}
	if len(result.Messages) == 0 {
		return nil, nil
	}
	return &result.Messages[0], nil
}

func (c *SQSConsumer) ChangeVisibility(ctx context.Context, message *types.Message, seconds int32) error {
	requestCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	_, err := c.client.ChangeMessageVisibility(requestCtx, &sqs.ChangeMessageVisibilityInput{
		QueueUrl: aws.String(c.settings.QueueURL), ReceiptHandle: message.ReceiptHandle, VisibilityTimeout: seconds,
	})
	return err
}

func (c *SQSConsumer) Handle(ctx context.Context, received *types.Message) error {
	// No shutdown, não dependemos do contexto já cancelado para liberar a
	// mensagem. Se o broker estiver indisponível, a reserva vence sozinha.
	defer func() {
		if ctx.Err() != nil {
			if err := c.ChangeVisibility(context.WithoutCancel(ctx), received, 0); err != nil {
				c.logger.Error("SQS visibility release failed")
			}
		}
	}()
	body := aws.ToString(received.Body)
	message, input, err := ParseWagerMessage(body)
	var result *application.WagerResult
	if err == nil {
		// SenderId é atributo de sistema do SQS, não um campo do JSON.
		sender := received.Attributes["SenderId"]
		provider := c.settings.ProviderBySender[sender]
		if provider == "" {
			provider = c.settings.ProviderBySender[strings.SplitN(sender, ":", 2)[0]]
		}
		if provider == "" || provider != input.ProviderID {
			err = application.ErrProviderNotAuthorized
		} else if received.Attributes["MessageGroupId"] != input.WalletID.String() {
			err = application.ErrInvalidMessage
		} else {
			processingCtx, cancel := context.WithTimeout(ctx, c.settings.ProcessingTimeout)
			result, err = c.useCase.ExecuteMessage(processingCtx, provider, input, message.MessageID, []byte(body))
			cancel()
		}
	}
	if err != nil {
		attempt, _ := strconv.Atoi(received.Attributes["ApproximateReceiveCount"])
		permanent := permanentMessageError(err)
		delay := messageRetryDelay(attempt)
		if permanent {
			delay = 0
		} // O redrive do broker encaminha à DLQ após 5 recebimentos.
		c.logger.Error("SQS treatment failed", "messageId", message.MessageID, "correlationId", input.CorrelationID,
			"providerId", input.ProviderID, "walletId", input.WalletID, "permanent", permanent, "attempts", attempt)
		if ctx.Err() != nil {
			return err
		}
		return errors.Join(err, c.ChangeVisibility(ctx, received, delay))
	}
	// ExecuteMessage só retorna sucesso depois do commit da inbox e do domínio.
	deleteCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	_, err = c.client.DeleteMessage(deleteCtx, &sqs.DeleteMessageInput{QueueUrl: aws.String(c.settings.QueueURL), ReceiptHandle: received.ReceiptHandle})
	if err != nil {
		return err
	}
	c.logger.Info("SQS treatment acknowledged", "messageId", message.MessageID, "correlationId", input.CorrelationID,
		"providerId", input.ProviderID, "walletId", input.WalletID, "transactionId", result.TransactionID, "status", result.Status, "replay", result.IdempotentReplay)
	return nil
}

func messageRetryDelay(attempt int) int32 {
	delay := int32(1)
	for i := 1; i < attempt && delay < 60; i++ {
		delay *= 2
	}
	if delay > 60 {
		return 60
	}
	return delay
}

func permanentMessageError(err error) bool {
	for _, permanent := range []error{
		application.ErrInvalidMessage, application.ErrProviderNotAuthorized, application.ErrIdempotencyConflict, application.ErrWalletIdentityMismatch,
		ports.ErrInboxConflict, domain.ErrInvalidMoney, domain.ErrMoneyOverflow, domain.ErrInvalidWagerKind, domain.ErrInvalidWagerTransaction,
		domain.ErrInvalidWagerMoney, domain.ErrReferenceRequired, domain.ErrExternalOpening, domain.ErrInvalidWagerReference,
	} {
		if errors.Is(err, permanent) {
			return true
		}
	}
	return false
}

var ConsumerModule = fx.Module("sqs-consumer", fx.Provide(NewConsumerConfig, NewSQSConsumer))
