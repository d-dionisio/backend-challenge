package messaging

import (
	"encoding/json"
	"errors"
	"net/url"
	"os"
	"strings"
	"time"
)

type ConsumerConfig struct {
	QueueURL          string
	ProviderBySender  map[string]string
	ProcessingTimeout time.Duration
}

func NewConsumerConfig() (ConsumerConfig, error) {
	c := ConsumerConfig{QueueURL: os.Getenv("SQS_WAGERS_QUEUE_URL"), ProcessingTimeout: 5 * time.Second}
	if err := json.Unmarshal([]byte(os.Getenv("SQS_PROVIDER_BY_SENDER")), &c.ProviderBySender); err != nil {
		return c, errors.New("SQS_PROVIDER_BY_SENDER must map broker sender IDs to provider IDs")
	}
	if value, exists := os.LookupEnv("SQS_PROCESSING_TIMEOUT"); exists {
		var err error
		c.ProcessingTimeout, err = time.ParseDuration(value)
		if err != nil {
			return c, errors.New("invalid SQS_PROCESSING_TIMEOUT")
		}
	}
	return c, c.validate()
}

func (c ConsumerConfig) validate() error {
	u, err := url.Parse(c.QueueURL)
	if err != nil || u.Host == "" || (u.Scheme != "http" && u.Scheme != "https") || u.User != nil || !strings.HasSuffix(u.Path, ".fifo") {
		return errors.New("SQS_WAGERS_QUEUE_URL must identify a FIFO queue")
	}
	if len(c.ProviderBySender) == 0 {
		return errors.New("SQS_PROVIDER_BY_SENDER is required")
	}
	for sender, provider := range c.ProviderBySender {
		if strings.TrimSpace(sender) == "" || sender == "*" || strings.TrimSpace(provider) == "" {
			return errors.New("invalid SQS sender mapping")
		}
	}
	// Reserva fixa de 30s: deixa margem para receber a resposta e remover.
	if c.ProcessingTimeout <= 0 || c.ProcessingTimeout > 20*time.Second {
		return errors.New("SQS_PROCESSING_TIMEOUT must be positive and at most 20s")
	}
	return nil
}
