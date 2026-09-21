package application

import (
	"math"
	"testing"
	"time"
)

func TestReferenceBackoff(t *testing.T) {
	policy := ReferenceRetryPolicy{MaxAttempts: 10, InitialDelay: time.Second, MaxDelay: time.Minute}
	for index, want := range []time.Duration{time.Second, 2 * time.Second, 4 * time.Second, 8 * time.Second, 16 * time.Second, 32 * time.Second, time.Minute, time.Minute} {
		if got := policy.delay(index + 1); got != want {
			t.Fatal(index, got, want)
		}
	}
	policy.InitialDelay = time.Duration(math.MaxInt64/2 + 1)
	policy.MaxDelay = time.Duration(math.MaxInt64)
	if got := policy.delay(1000); got != policy.MaxDelay {
		t.Fatal("backoff overflow", got)
	}
}

func TestReferenceRetryPolicyValidation(t *testing.T) {
	for _, policy := range []ReferenceRetryPolicy{
		{}, {MaxAttempts: 1, InitialDelay: time.Second, MaxDelay: time.Millisecond},
		{MaxAttempts: 1001, InitialDelay: time.Second, MaxDelay: time.Minute},
	} {
		if err := policy.Validate(); err == nil {
			t.Fatal("invalid retry policy accepted")
		}
	}
}
