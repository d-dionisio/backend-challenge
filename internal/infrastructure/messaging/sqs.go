package messaging

import (
	"context"
	"errors"
	"net/http"
	"net/url"
	"os"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"github.com/d-dionisio/backend-challenge/internal/application/ports"
	"go.uber.org/fx"
)

type Config struct {
	Region   string
	Endpoint string
	QueueURL string
}

func NewConfig() (Config, error) {
	c := Config{Region: os.Getenv("AWS_REGION"), Endpoint: os.Getenv("SQS_ENDPOINT"), QueueURL: os.Getenv("SQS_EVENTS_QUEUE_URL")}
	if c.Region == "" {
		return Config{}, errors.New("AWS_REGION is required")
	}
	for _, setting := range []struct {
		value    string
		optional bool
	}{{c.QueueURL, false}, {c.Endpoint, true}} {
		if setting.optional && setting.value == "" {
			continue // Endpoint vazio usa o endpoint oficial da AWS.
		}
		u, err := url.Parse(setting.value)
		if err != nil || u.Host == "" || (u.Scheme != "http" && u.Scheme != "https") || u.User != nil {
			return Config{}, errors.New("invalid SQS endpoint or events queue URL")
		}
	}
	if strings.HasSuffix(c.QueueURL, ".fifo") {
		return Config{}, errors.New("events destination must be a standard SQS queue")
	}
	return c, nil
}

type SQSPublisher struct {
	client   *sqs.Client
	queueURL string
}

func NewSQSPublisher(lifecycle fx.Lifecycle, settings Config) *SQSPublisher {
	publisher := &SQSPublisher{queueURL: settings.QueueURL}
	transport := http.DefaultTransport.(*http.Transport).Clone()
	lifecycle.Append(fx.Hook{
		OnStart: func(ctx context.Context) error {
			// A cadeia padrão aceita variáveis AWS locais e roles na AWS.
			cfg, err := config.LoadDefaultConfig(ctx, config.WithRegion(settings.Region),
				config.WithHTTPClient(&http.Client{Transport: transport}))
			if err != nil {
				return errors.New("could not load AWS configuration")
			}
			publisher.client = sqs.NewFromConfig(cfg, func(options *sqs.Options) {
				if settings.Endpoint != "" {
					options.BaseEndpoint = aws.String(settings.Endpoint)
				}
				// O backoff persistente é responsabilidade da outbox.
				options.RetryMaxAttempts = 1
			})
			attributes, err := publisher.client.GetQueueAttributes(ctx, &sqs.GetQueueAttributesInput{
				QueueUrl: aws.String(settings.QueueURL), AttributeNames: []types.QueueAttributeName{types.QueueAttributeNamePolicy},
			})
			if err != nil {
				transport.CloseIdleConnections()
				return errors.New("SQS events queue is unavailable or access is denied")
			}
			if validateQueuePolicy(attributes.Attributes["Policy"], "sqs:SendMessage") != nil ||
				validateNoWildcardAdministration(attributes.Attributes["Policy"]) != nil {
				transport.CloseIdleConnections()
				return errors.New("SQS events queue IAM policy is not least-privilege")
			}
			return nil
		},
		OnStop: func(context.Context) error {
			transport.CloseIdleConnections()
			return nil
		},
	})
	return publisher
}

func (p *SQSPublisher) Publish(ctx context.Context, event *ports.PendingEvent) error {
	_, err := p.client.SendMessage(ctx, &sqs.SendMessageInput{
		QueueUrl: aws.String(p.queueURL), MessageBody: aws.String(string(event.Payload)),
	})
	return err
}

var Module = fx.Module("messaging", fx.Provide(NewConfig,
	fx.Annotate(NewSQSPublisher, fx.As(new(ports.EventPublisher)))))
