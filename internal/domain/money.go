package domain

import (
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"regexp"
	"strconv"
	"strings"
)

var (
	ErrInvalidMoney     = errors.New("invalid money")
	ErrCurrencyMismatch = errors.New("currency mismatch")
	ErrMoneyOverflow    = errors.New("money overflow")
)

type Money struct {
	amount   int64
	currency string
}

var moneyPattern = regexp.MustCompile(`^\d+(\.\d{1,2})?$`)

func NewMoney(value string, currency string) (Money, error) {
	if !validCurrency(currency) {
		return Money{}, ErrInvalidMoney
	}

	if !moneyPattern.MatchString(value) {
		return Money{}, ErrInvalidMoney
	}

	parts := strings.Split(value, ".")

	whole, err := strconv.ParseInt(parts[0], 10, 64)
	if err != nil {
		return Money{}, ErrMoneyOverflow
	}

	var cents int64

	if len(parts) == 2 {
		decimal := parts[1]
		if len(decimal) == 1 {
			decimal += "0"
		}

		cents, err = strconv.ParseInt(decimal, 10, 64)

		if err != nil {
			return Money{}, ErrInvalidMoney
		}
	}

	if whole > (math.MaxInt64-cents)/100 {
		return Money{}, ErrMoneyOverflow
	}

	amount := whole*100 + cents

	return Money{
		amount:   amount,
		currency: currency,
	}, nil
}

func ZeroMoney(currency string) (Money, error) {
	return MoneyFromMinorUnits(0, currency)
}

// BRL é a moeda principal. USD permite testar operações com moedas diferentes.
func validCurrency(currency string) bool {
	return currency == "BRL" || currency == "USD"
}

func (m Money) Validate() error {
	if !validCurrency(m.currency) {
		return ErrInvalidMoney
	}
	return nil
}

// Recebe centavos do banco ou de cálculos internos, que podem ser negativos.
func MoneyFromMinorUnits(amount int64, currency string) (Money, error) {
	if !validCurrency(currency) {
		return Money{}, ErrInvalidMoney
	}
	return Money{amount: amount, currency: currency}, nil
}

func (m Money) compatible(other Money) error {
	if m.Validate() != nil || other.Validate() != nil {
		return ErrInvalidMoney
	}
	if m.currency != other.currency {
		return ErrCurrencyMismatch
	}
	return nil
}

func (m Money) Amount() int64 {
	return m.amount
}

func (m Money) Currency() string {
	return m.currency
}

func (m Money) Add(other Money) (Money, error) {
	if err := m.compatible(other); err != nil {
		return Money{}, err
	}

	if other.amount > 0 && m.amount > math.MaxInt64-other.amount {
		return Money{}, ErrMoneyOverflow
	}
	if other.amount < 0 && m.amount < math.MinInt64-other.amount {
		return Money{}, ErrMoneyOverflow
	}

	return Money{
		amount:   m.amount + other.amount,
		currency: m.currency,
	}, nil
}

func (m Money) Sub(other Money) (Money, error) {
	if err := m.compatible(other); err != nil {
		return Money{}, err
	}
	if other.amount > 0 && m.amount < math.MinInt64+other.amount {
		return Money{}, ErrMoneyOverflow
	}
	if other.amount < 0 && m.amount > math.MaxInt64+other.amount {
		return Money{}, ErrMoneyOverflow
	}

	return Money{
		amount:   m.amount - other.amount,
		currency: m.currency,
	}, nil
}

func (m Money) IsZero() bool {
	return m.Validate() == nil && m.amount == 0
}

func (m Money) IsPositive() bool {
	return m.Validate() == nil && m.amount > 0
}

func (m Money) LessThan(other Money) (bool, error) {
	if err := m.compatible(other); err != nil {
		return false, err
	}

	return m.amount < other.amount, nil
}

func (m Money) String() string {
	whole := m.amount / 100
	cents := m.amount % 100
	if m.amount < 0 {
		return fmt.Sprintf("-%d.%02d", -whole, -cents)
	}
	return fmt.Sprintf("%d.%02d", whole, cents)
}

func (m Money) Negate() (Money, error) {
	if err := m.Validate(); err != nil {
		return Money{}, err
	}
	if m.amount == math.MinInt64 {
		return Money{}, ErrMoneyOverflow
	}
	return Money{amount: -m.amount, currency: m.currency}, nil
}

func (m Money) MarshalJSON() ([]byte, error) {
	if err := m.Validate(); err != nil {
		return nil, err
	}
	value := struct {
		Amount   string `json:"amount"`
		Currency string `json:"currency"`
	}{
		Amount:   m.String(),
		Currency: m.currency,
	}
	return json.Marshal(value)
}
