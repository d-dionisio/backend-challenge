package domain

import "testing"

func TestNewMoney(t *testing.T) {
	money, err := NewMoney("25.00", "BRL")
	if err != nil {
		t.Fatal(err)
	}

	if money.Amount() != 2500 {
		t.Fatalf("expected 2500, got %d", money.Amount())
	}

	if money.Currency() != "BRL" {
		t.Fatalf("expected BRL, got %s", money.Currency())
	}
}

func TestMoneyWithoutDecimals(t *testing.T) {
	money, err := NewMoney("25", "BRL")
	if err != nil {
		t.Fatal(err)
	}

	if money.Amount() != 2500 {
		t.Fatalf("expected 2500, got %d", money.Amount())
	}
}

func TestMoneyOneDecimal(t *testing.T) {
	money, err := NewMoney("25.5", "BRL")
	if err != nil {
		t.Fatal(err)
	}

	if money.Amount() != 2550 {
		t.Fatalf("expected 2550, got %d", money.Amount())
	}
}

func TestInvalidMoney(t *testing.T) {
	tests := []string{
		"",
		"-10.00",
		"10.999",
		"NaN",
		"Infinity",
		"1e10",
	}

	for _, value := range tests {
		_, err := NewMoney(value, "BRL")

		if err == nil {
			t.Fatalf("expected error for %s", value)
		}
	}
}

func TestCurrencyMismath(t *testing.T) {
	brl, _ := NewMoney("10.00", "BRL")
	usd, _ := NewMoney("10.00", "USD")

	_, err := brl.Add(usd)

	if err == nil {
		t.Fatal("expected currency mismatch")
	}
}
