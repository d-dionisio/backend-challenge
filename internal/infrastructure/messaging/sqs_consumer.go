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
	"github.com/d-dionisio/backend-challenge/internal/observability"
	"go.uber.org/fx"
)

type SQSConsumer struct {
	client         *sqs.Client
	eventsQueueURL string
	dlqQueueURL    string
	settings       ConsumerConfig
	useCase        *application.ProcessWager
	metrics        *observability.Metrics
	logger         *slog.Logger
}

func NewSQSConsumer(lifecycle fx.Lifecycle, connection Config, settings ConsumerConfig, useCase *application.ProcessWager, metrics *observability.Metrics) (*SQSConsumer, error) {
	if err := settings.validate(); err != nil {
		return nil, err
	}
	c := &SQSConsumer{settings: settings, eventsQueueURL: connection.QueueURL, useCase: useCase, metrics: metrics, logger: slog.New(slog.NewJSONHandler(os.Stdout, nil))}
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
				QueueUrl: aws.String(settings.QueueURL), AttributeNames: []types.QueueAttributeName{
					types.QueueAttributeNameFifoQueue, types.QueueAttributeNameQueueArn, types.QueueAttributeNameRedrivePolicy, types.QueueAttributeNamePolicy,
				},
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
			if validateQueuePolicy(attributes.Attributes["Policy"], "sqs:SendMessage") != nil ||
				validateNoWildcardAdministration(attributes.Attributes["Policy"]) != nil {
				transport.CloseIdleConnections()
				return errors.New("SQS input queue IAM policy is not least-privilege")
			}
			dlqURL, err := c.client.GetQueueUrl(ctx, &sqs.GetQueueUrlInput{QueueName: aws.String(queueNameFromARN(redrive.DeadLetterTargetArn))})
			if err != nil {
				transport.CloseIdleConnections()
				return errors.New("SQS DLQ is unavailable or access is denied")
			}
			dlqAttributes, err := c.client.GetQueueAttributes(ctx, &sqs.GetQueueAttributesInput{
				QueueUrl: dlqURL.QueueUrl, AttributeNames: []types.QueueAttributeName{types.QueueAttributeNameRedriveAllowPolicy},
			})
			if err != nil || validateRedriveAllowPolicy(dlqAttributes.Attributes["RedriveAllowPolicy"], attributes.Attributes["QueueArn"]) != nil {
				transport.CloseIdleConnections()
				return errors.New("SQS DLQ redrive policy is not restricted to the input queue")
			}
			c.dlqQueueURL = aws.ToString(dlqURL.QueueUrl)
			return nil
		},
		OnStop: func(context.Context) error { transport.CloseIdleConnections(); return nil },
	})
	return c, nil
}

func queueNameFromARN(arn string) string {
	parts := strings.Split(arn, ":")
	if len(parts) == 0 {
		return ""
	}
	return parts[len(parts)-1]
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
	attributes, err := c.client.GetQueueAttributes(ctx, &sqs.GetQueueAttributesInput{
		QueueUrl: aws.String(c.dlqQueueURL), AttributeNames: []types.QueueAttributeName{
			types.QueueAttributeNameApproximateNumberOfMessages, types.QueueAttributeNameApproximateNumberOfMessagesNotVisible, types.QueueAttributeNameApproximateNumberOfMessagesDelayed,
		},
	})
	if err != nil {
		return err
	}
	var depth int64
	for _, key := range []string{"ApproximateNumberOfMessages", "ApproximateNumberOfMessagesNotVisible", "ApproximateNumberOfMessagesDelayed"} {
		value, err := strconv.ParseInt(attributes.Attributes[key], 10, 64)
		if err != nil || value < 0 {
			return errors.New("invalid DLQ depth")
		}
		depth += value
	}
	c.metrics.SetDLQDepth(depth)
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
	started := time.Now()
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
		c.metrics.ObserveProcessingError(err, time.Since(started))
		permanent := permanentMessageError(err)
		delay := messageRetryDelay(attempt)
		if permanent {
			delay = 0
		} // O redrive do broker encaminha à DLQ após 5 recebimentos.
		if permanent || attempt >= 5 {
			c.metrics.ObserveDeadLetter(errorMetricReason(err))
		} else {
			c.metrics.ObserveMessageRetry(errorMetricReason(err))
		}
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
	c.metrics.ObserveWagerResult(string(result.Status), result.IdempotentReplay, time.Since(started))
	c.logger.Info("SQS treatment acknowledged", "messageId", message.MessageID, "correlationId", input.CorrelationID,
		"providerId", input.ProviderID, "walletId", input.WalletID, "transactionId", result.TransactionID, "status", result.Status, "replay", result.IdempotentReplay)
	return nil
}

func errorMetricReason(err error) string {
	switch {
	case errors.Is(err, application.ErrInvalidMessage):
		return "invalid_message"
	case errors.Is(err, application.ErrProviderNotAuthorized):
		return "provider_not_authorized"
	case errors.Is(err, application.ErrIdempotencyConflict):
		return "idempotency_conflict"
	case errors.Is(err, application.ErrWalletIdentityMismatch):
		return "wallet_identity_mismatch"
	case errors.Is(err, ports.ErrInboxConflict):
		return "inbox_conflict"
	case errors.Is(err, ports.ErrInboxIncomplete):
		return "inbox_incomplete"
	default:
		return "processing_error"
	}
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
