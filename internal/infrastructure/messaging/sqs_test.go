package messaging

import "testing"

func TestSQSConfig(t *testing.T) {
	t.Setenv("AWS_REGION", "us-east-1")
	t.Setenv("SQS_ENDPOINT", "http://localhost:4566")
	t.Setenv("SQS_EVENTS_QUEUE_URL", "http://localhost:4566/queue/us-east-1/000000000000/wager-events")
	if _, err := NewConfig(); err != nil {
		t.Fatal(err)
	}
	for _, invalid := range []struct{ name, value string }{
		{"AWS_REGION", ""}, {"SQS_EVENTS_QUEUE_URL", ""}, {"SQS_ENDPOINT", "://bad"},
		{"SQS_EVENTS_QUEUE_URL", "http://localhost:4566/input.fifo"},
	} {
		t.Run(invalid.name, func(t *testing.T) {
			t.Setenv(invalid.name, invalid.value)
			if _, err := NewConfig(); err == nil {
				t.Fatal("invalid config accepted")
			}
		})
	}
	t.Setenv("SQS_ENDPOINT", "")
	if _, err := NewConfig(); err != nil {
		t.Fatal("AWS default endpoint rejected", err)
	}
}
