package application

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"testing"

	"github.com/d-dionisio/backend-challenge/internal/domain"
	"github.com/google/uuid"
)

func hashInput(t *testing.T) ProcessWagerInput {
	t.Helper()
	money, err := domain.NewMoney("25", "BRL")
	if err != nil {
		t.Fatal(err)
	}
	return ProcessWagerInput{ProviderID: "provider-a", ExternalTransactionID: "bet-1", IdempotencyKey: "key-1",
		PlayerID: uuid.MustParse("0192f28f-5dc0-7d58-bdb2-814ad6a0f4a1"), WalletID: uuid.MustParse("0192f291-27dd-7d3f-8071-5f8685deef37"),
		RoundID: "round-1", GameID: "game-1", Kind: domain.WagerKindBet, Money: money}
}

func TestWagerCanonicalHash(t *testing.T) {
	input := hashInput(t)
	canonical := `{"externalTransactionId":"bet-1","gameId":"game-1","kind":"BET","money":{"amount":"25.00","currency":"BRL"},"playerId":"0192f28f-5dc0-7d58-bdb2-814ad6a0f4a1","providerId":"provider-a","roundId":"round-1","walletId":"0192f291-27dd-7d3f-8071-5f8685deef37"}`
	expected := sha256.Sum256([]byte(canonical))
	hash, err := input.payloadHash()
	if err != nil || hash != hex.EncodeToString(expected[:]) {
		t.Fatal(hash, err)
	}
	input.IdempotencyKey = "another-key"
	input.CorrelationID = "another-request"
	input.CausationID = "sqs-message"
	input.Money, err = domain.NewMoney("00025.0", "BRL")
	if err != nil {
		t.Fatal(err)
	}
	normalized, err := input.payloadHash()
	if err != nil || normalized != hash {
		t.Fatal("transport or decimal notation changed hash", err)
	}
}

func TestHashIncludesEveryBusinessField(t *testing.T) {
	input := hashInput(t)
	before, err := input.payloadHash()
	if err != nil {
		t.Fatal(err)
	}
	for _, change := range []func(*ProcessWagerInput){
		func(i *ProcessWagerInput) { i.ProviderID = "other" },
		func(i *ProcessWagerInput) { i.ExternalTransactionID = "other" },
		func(i *ProcessWagerInput) { i.PlayerID = uuid.New() },
		func(i *ProcessWagerInput) { i.WalletID = uuid.New() },
		func(i *ProcessWagerInput) { i.RoundID = "other" },
		func(i *ProcessWagerInput) { i.GameID = "other" },
		func(i *ProcessWagerInput) { i.Kind = domain.WagerKindWin },
		func(i *ProcessWagerInput) { i.Money, _ = domain.NewMoney("26", "BRL") },
		func(i *ProcessWagerInput) { i.Money, _ = domain.NewMoney("25", "USD") },
		func(i *ProcessWagerInput) { reference := "bet-previous"; i.ReferenceExternalTransactionID = &reference },
	} {
		changed := input
		change(&changed)
		after, err := changed.payloadHash()
		if err != nil || before == after {
			t.Fatal("business field omitted from hash", err)
		}
	}
}

func TestProcessWagerRejectsUnauthorizedBeforePersistence(t *testing.T) {
	unit := &failingUnitOfWork{}
	for _, provider := range []string{"", "provider-b"} {
		result, err := NewProcessWager(unit).Execute(context.Background(), provider, hashInput(t))
		if result != nil || !errors.Is(err, ErrProviderNotAuthorized) || unit.called {
			t.Fatal(result, err, unit.called)
		}
	}
}

func TestProcessWagerRejectsInvalidInputBeforePersistence(t *testing.T) {
	for _, change := range []func(*ProcessWagerInput){
		func(i *ProcessWagerInput) { i.IdempotencyKey = " " },
		func(i *ProcessWagerInput) { i.Kind = domain.WagerKindOpening },
		func(i *ProcessWagerInput) { i.Money = domain.Money{} },
		func(i *ProcessWagerInput) { i.WalletID = uuid.Nil },
		func(i *ProcessWagerInput) { i.Kind = domain.WagerKindRefund },
	} {
		input := hashInput(t)
		change(&input)
		unit := &failingUnitOfWork{}
		result, err := NewProcessWager(unit).Execute(context.Background(), input.ProviderID, input)
		if result != nil || err == nil || unit.called {
			t.Fatal(result, err, unit.called)
		}
	}
}
