package messaging

import (
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/d-dionisio/backend-challenge/internal/application"
	"github.com/d-dionisio/backend-challenge/internal/application/ports"
)

const validWagerMessage = `{"messageId":"msg-1","type":"WagerTransactionRequested","occurredAt":"2026-09-08T12:00:00Z","data":{"providerId":"provider-a","externalTransactionId":"bet-1","idempotencyKey":"original-key","playerId":"0192f28f-5dc0-7d58-bdb2-814ad6a0f4a1","walletId":"0192f291-27dd-7d3f-8071-5f8685deef37","roundId":"round-1","gameId":"game-1","kind":"BET","money":{"amount":"25","currency":"BRL"}}}`

func TestWagerMessageParsing(t *testing.T) {
	message, input, err := ParseWagerMessage(validWagerMessage)
	if err != nil {
		t.Fatal(err)
	}
	if message.MessageID != "msg-1" || input.CausationID != "msg-1" || input.CorrelationID != "msg-1" || input.IdempotencyKey != "original-key" || input.Money.String() != "25.00" {
		t.Fatal("message identity or money changed")
	}
	for _, body := range []string{
		"null", validWagerMessage + "{}", strings.Replace(validWagerMessage, `"msg-1"`, `""`, 1),
		strings.Replace(validWagerMessage, `"WagerTransactionRequested"`, `"Other"`, 1),
		strings.Replace(validWagerMessage, `"amount":"25"`, `"amount":25.0`, 1),
		strings.Replace(validWagerMessage, `"amount":"25"`, `"amount":"NaN"`, 1),
		strings.Replace(validWagerMessage, `"amount":"25"`, `"amount":"-1"`, 1),
		strings.Replace(validWagerMessage, `"amount":"25"`, `"amount":"0.001"`, 1),
		strings.Replace(validWagerMessage, `"data":`, `"unknown":true,"data":`, 1),
	} {
		if _, _, err := ParseWagerMessage(body); !errors.Is(err, application.ErrInvalidMessage) {
			t.Fatalf("invalid message accepted: %s", body)
		}
	}
}

func TestConsumerPolicy(t *testing.T) {
	for attempt, want := range []int32{1, 2, 4, 8, 16, 32, 60, 60} {
		if got := messageRetryDelay(attempt + 1); got != want {
			t.Fatal(got, want)
		}
	}
	if !permanentMessageError(ports.ErrInboxConflict) || !permanentMessageError(application.ErrProviderNotAuthorized) || permanentMessageError(ports.ErrWalletNotFound) {
		t.Fatal("incorrect permanent/transient classification")
	}
	c := ConsumerConfig{QueueURL: "http://localhost:14566/input.fifo", ProviderBySender: map[string]string{"sender": "provider-a"}, ProcessingTimeout: 5 * time.Second}
	if err := c.validate(); err != nil {
		t.Fatal(err)
	}
	c.ProcessingTimeout = 30 * time.Second
	if c.validate() == nil {
		t.Fatal("processing must finish before visibility expires")
	}
	c.ProcessingTimeout = time.Second
	c.ProviderBySender = nil
	if c.validate() == nil {
		t.Fatal("missing sender mapping accepted")
	}
}
