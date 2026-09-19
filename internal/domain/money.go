package domain

import (
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
	if currency == "" {
		return Money{}, ErrInvalidMoney
	}

	if !moneyPattern.MatchString(value) {
		return Money{}, ErrInvalidMoney
	}

	parts := strings.Split(value, ".")

	whole, err := strconv.ParseInt(parts[0], 10, 64)
	if err != nil {
		return Money{}, ErrInvalidMoney
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

	if whole > math.MaxInt64/100 {
		return Money{}, ErrMoneyOverflow
	}

	amount := whole*100 + cents

	if amount < 0 {
		return Money{}, ErrInvalidMoney
	}

	return Money{
		amount:   amount,
		currency: currency,
	}, nil
}

func ZeroMoney(currency string) Money {
	return Money{
		amount:   0,
		currency: currency,
	}
}

func (m Money) Amount() int64 {
	return m.amount
}

func (m Money) Currency() string {
	return m.currency
}

func (m Money) Add(other Money) (Money, error) {
	if m.currency != other.currency {
		return Money{}, ErrCurrencyMismatch
	}

	if other.amount > 0 && m.amount > math.MaxInt64-other.amount {
		return Money{}, ErrMoneyOverflow
	}

	return Money{
		amount:   m.amount + other.amount,
		currency: m.currency,
	}, nil
}

func (m Money) Sub(other Money) (Money, error) {
	if m.currency != other.currency {
		return Money{}, ErrCurrencyMismatch
	}

	return Money{
		amount:   m.amount - other.amount,
		currency: m.currency,
	}, nil
}

func (m Money) IsZero() bool {
	return m.amount == 0
}

func (m Money) IsPositive() bool {
	return m.amount > 0
}

func (m Money) LessThan(other Money) (bool, error) {
	if m.currency != other.currency {
		return false, ErrCurrencyMismatch
	}

	return m.amount < other.amount, nil
}

func (m Money) String() string {
	whole := m.amount / 100
	cents := m.amount % 100

	return fmt.Sprintf("%d.%02d", whole, cents)
}
