package workers

import "testing"

func TestOutboxConfig(t *testing.T) {
	for name, value := range map[string]string{
		"OUTBOX_POLL_INTERVAL": "1s", "OUTBOX_ATTEMPT_TIMEOUT": "5s", "OUTBOX_LEASE_DURATION": "30s",
		"OUTBOX_INITIAL_DELAY": "1s", "OUTBOX_MAX_DELAY": "1m",
	} {
		t.Setenv(name, value)
	}
	if _, err := NewOutboxConfig(); err != nil {
		t.Fatal(err)
	}
	for _, invalid := range []struct{ name, value string }{
		{"OUTBOX_LEASE_DURATION", "5s"}, {"OUTBOX_POLL_INTERVAL", "0s"},
		{"OUTBOX_INITIAL_DELAY", "2m"}, {"OUTBOX_ATTEMPT_TIMEOUT", "invalid"},
	} {
		t.Run(invalid.name, func(t *testing.T) {
			t.Setenv(invalid.name, invalid.value)
			if _, err := NewOutboxConfig(); err == nil {
				t.Fatal("invalid config accepted")
			}
		})
	}
}
