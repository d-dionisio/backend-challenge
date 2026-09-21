package domain

import (
	"encoding/json"
	"errors"
	"math"
	"testing"
)

func TestMoneyParsingLimitsAndNormalization(t *testing.T) {
	for _, tc := range []struct{ input, want string }{
		{"0", "0.00"}, {"00025.5", "25.50"}, {"92233720368547758.07", "92233720368547758.07"},
	} {
		m, err := NewMoney(tc.input, "BRL")
		if err != nil || m.String() != tc.want {
			t.Fatalf("%s: %s, %v", tc.input, m.String(), err)
		}
	}
	for _, input := range []string{"92233720368547758.08", "92233720368547759", "999999999999999999999999"} {
		if _, err := NewMoney(input, "BRL"); !errors.Is(err, ErrMoneyOverflow) {
			t.Fatalf("%s: %v", input, err)
		}
	}
	for _, input := range []string{" ", " 1", "1 ", "+1", ".5", "1.", "1,00", "1.001", "-0.00", "1e2", "NaN", "Infinity"} {
		if _, err := NewMoney(input, "BRL"); !errors.Is(err, ErrInvalidMoney) {
			t.Fatalf("%s: %v", input, err)
		}
	}
}

func TestMoneySignedArithmetic(t *testing.T) {
	for _, tc := range []struct {
		name     string
		a, b     int64
		sub      bool
		want     int64
		overflow bool
	}{
		{"add positive overflow", math.MaxInt64, 1, false, 0, true},
		{"add negative overflow", math.MinInt64, -1, false, 0, true},
		{"subtract positive overflow", math.MinInt64, 1, true, 0, true},
		{"subtract negative overflow", math.MaxInt64, -1, true, 0, true},
		{"negative difference", 0, 1, true, -1, false},
		{"opposite limits", math.MinInt64, math.MaxInt64, false, -1, false},
		{"subtract minimum from minimum", math.MinInt64, math.MinInt64, true, 0, false},
		{"maximum", math.MaxInt64 - 1, 1, false, math.MaxInt64, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			a, _ := MoneyFromMinorUnits(tc.a, "BRL")
			b, _ := MoneyFromMinorUnits(tc.b, "BRL")
			var got Money
			var err error
			if tc.sub {
				got, err = a.Sub(b)
			} else {
				got, err = a.Add(b)
			}
			if tc.overflow {
				if !errors.Is(err, ErrMoneyOverflow) {
					t.Fatalf("expected overflow, got %v", err)
				}
				return
			}
			if err != nil || got.Amount() != tc.want {
				t.Fatalf("got %d, %v", got.Amount(), err)
			}
		})
	}
}

func TestMoneyNegativeFormattingAndNegation(t *testing.T) {
	for _, tc := range []struct {
		amount int64
		want   string
	}{{-1, "-0.01"}, {-125, "-1.25"}, {math.MinInt64, "-92233720368547758.08"}} {
		m, _ := MoneyFromMinorUnits(tc.amount, "BRL")
		if m.String() != tc.want {
			t.Fatalf("got %s", m.String())
		}
		negated, err := m.Negate()
		if tc.amount == math.MinInt64 {
			if !errors.Is(err, ErrMoneyOverflow) {
				t.Fatal(err)
			}
		} else if err != nil || negated.Amount() != -tc.amount {
			t.Fatalf("negation: %v, %v", negated, err)
		}
	}
}

func TestMoneyValidationAndSerialization(t *testing.T) {
	for _, currency := range []string{"", "brl", " BRL", "XXX", "JPY", "EUR", "INVALID"} {
		if _, err := NewMoney("1", currency); !errors.Is(err, ErrInvalidMoney) {
			t.Fatal(currency, err)
		}
		if _, err := ZeroMoney(currency); !errors.Is(err, ErrInvalidMoney) {
			t.Fatal(currency, err)
		}
		if _, err := MoneyFromMinorUnits(1, currency); !errors.Is(err, ErrInvalidMoney) {
			t.Fatal(currency, err)
		}
	}
	brl, _ := NewMoney("25", "BRL")
	usd, _ := NewMoney("25", "USD")
	for _, other := range []Money{usd, {}} {
		want := ErrCurrencyMismatch
		if other.Currency() == "" {
			want = ErrInvalidMoney
		}
		if _, err := brl.Add(other); !errors.Is(err, want) {
			t.Fatal(err)
		}
		if _, err := brl.Sub(other); !errors.Is(err, want) {
			t.Fatal(err)
		}
		if _, err := brl.LessThan(other); !errors.Is(err, want) {
			t.Fatal(err)
		}
	}
	if (Money{}).IsZero() {
		t.Fatal("uninitialized money is not valid zero")
	}
	if _, err := (Money{}).Negate(); !errors.Is(err, ErrInvalidMoney) {
		t.Fatal(err)
	}
	if _, err := json.Marshal(Money{}); !errors.Is(err, ErrInvalidMoney) {
		t.Fatal(err)
	}
	encoded, err := json.Marshal(brl)
	if err != nil || string(encoded) != `{"amount":"25.00","currency":"BRL"}` {
		t.Fatalf("%s: %v", encoded, err)
	}
	zero, err := ZeroMoney("BRL")
	if err != nil || !zero.IsZero() {
		t.Fatal(zero, err)
	}
}
