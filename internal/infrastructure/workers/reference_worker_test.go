package workers

import "testing"

func referenceEnvironment(t *testing.T) {
	t.Helper()
	t.Setenv("REFERENCE_POLL_INTERVAL", "1s")
	t.Setenv("REFERENCE_ATTEMPT_TIMEOUT", "5s")
	t.Setenv("REFERENCE_MAX_ATTEMPTS", "10")
	t.Setenv("REFERENCE_INITIAL_DELAY", "1s")
	t.Setenv("REFERENCE_MAX_DELAY", "1m")
}

func TestReferenceConfig(t *testing.T) {
	referenceEnvironment(t)
	if _, err := NewReferenceConfig(); err != nil {
		t.Fatal(err)
	}
	for _, setting := range []struct{ name, value string }{
		{"REFERENCE_POLL_INTERVAL", "0s"},
		{"REFERENCE_ATTEMPT_TIMEOUT", "invalid"},
		{"REFERENCE_MAX_ATTEMPTS", "0"},
		{"REFERENCE_MAX_ATTEMPTS", "1001"},
		{"REFERENCE_MAX_ATTEMPTS", "invalid"},
		{"REFERENCE_INITIAL_DELAY", "2m"},
		{"REFERENCE_MAX_DELAY", "-1s"},
	} {
		t.Run(setting.name+setting.value, func(t *testing.T) {
			t.Setenv(setting.name, setting.value)
			if _, err := NewReferenceConfig(); err == nil {
				t.Fatal("invalid worker config accepted")
			}
		})
	}
}
