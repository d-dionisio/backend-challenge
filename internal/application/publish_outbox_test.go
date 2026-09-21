package application

import (
	"testing"
	"time"
)

func TestOutboxBackoff(t *testing.T) {
	p := OutboxPolicy{LeaseDuration: 30 * time.Second, InitialDelay: time.Second, MaxDelay: time.Minute}
	for attempt, want := range []time.Duration{time.Second, 2 * time.Second, 4 * time.Second, 8 * time.Second, 16 * time.Second, 32 * time.Second, time.Minute, time.Minute} {
		if got := p.delay(attempt + 1); got != want {
			t.Fatalf("attempt %d: got %s want %s", attempt+1, got, want)
		}
	}
	p.InitialDelay = time.Duration(1 << 62)
	p.MaxDelay = time.Duration(1<<63 - 1)
	if got := p.delay(2147483647); got != p.MaxDelay {
		t.Fatalf("backoff overflow: %s", got)
	}
	for _, invalid := range []OutboxPolicy{{}, {LeaseDuration: time.Second, InitialDelay: time.Second, MaxDelay: time.Millisecond}} {
		if invalid.Validate() == nil {
			t.Fatal("invalid policy accepted")
		}
	}
}
