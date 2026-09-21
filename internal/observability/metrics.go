package observability

import (
	"fmt"
	"net/http"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

type Metrics struct {
	mu sync.RWMutex

	reconciliationDivergences atomic.Uint64
	processingLatency         durationSummary
	outboxLag                 durationSummary

	wagerResults         map[string]uint64
	duplicateMessages    map[string]uint64
	messageRetries       map[string]uint64
	deadLetterMessages   map[string]uint64
	concurrencyConflicts map[string]uint64
	outboxPublishResults map[string]uint64
	outboxPublishRetries map[string]uint64
	outboxClaimFailures  uint64
}

type durationSummary struct {
	count uint64
	sum   uint64
	max   uint64
}

func NewMetrics() *Metrics {
	return &Metrics{
		wagerResults:         make(map[string]uint64),
		duplicateMessages:    make(map[string]uint64),
		messageRetries:       make(map[string]uint64),
		deadLetterMessages:   make(map[string]uint64),
		concurrencyConflicts: make(map[string]uint64),
		outboxPublishResults: make(map[string]uint64),
		outboxPublishRetries: make(map[string]uint64),
	}
}

func (m *Metrics) ObserveWagerResult(status string, replay bool, latency time.Duration) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.wagerResults[label(status)]++
	if replay {
		m.duplicateMessages["idempotent_replay"]++
	}
	m.processingLatency.observe(latency)
}

func (m *Metrics) ObserveDuplicateMessage(source string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.duplicateMessages[label(source)]++
}

func (m *Metrics) ObserveMessageRetry(reason string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.messageRetries[label(reason)]++
}

func (m *Metrics) ObserveDeadLetter(reason string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.deadLetterMessages[label(reason)]++
}

func (m *Metrics) ObserveConcurrencyConflict(scope string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.concurrencyConflicts[label(scope)]++
}

func (m *Metrics) ObserveOutboxPublish(result string, attempts int, occurredAt time.Time) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.outboxPublishResults[label(result)]++
	if result == "retry" {
		m.outboxPublishRetries[label(fmt.Sprintf("attempt_%d", attempts))]++
	}
	if !occurredAt.IsZero() {
		m.outboxLag.observe(time.Since(occurredAt))
	}
}

func (m *Metrics) ObserveOutboxClaimFailure() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.outboxClaimFailures++
}

func (m *Metrics) AddReconciliationDivergence() {
	m.reconciliationDivergences.Add(1)
}

func (m *Metrics) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
	w.Header().Set("Cache-Control", "no-store")

	m.mu.RLock()
	defer m.mu.RUnlock()

	_, _ = fmt.Fprintf(w, "# HELP wallet_reconciliation_divergences_total Reconciliation checks that found a balance divergence.\n# TYPE wallet_reconciliation_divergences_total counter\nwallet_reconciliation_divergences_total %d\n", m.reconciliationDivergences.Load())
	writeCounterMap(w, "wager_results_total", "Wager processing results by final status.", "status", m.wagerResults)
	writeCounterMap(w, "wager_duplicates_total", "Duplicate wager requests or messages detected.", "source", m.duplicateMessages)
	writeCounterMap(w, "wager_retries_total", "Wager message retries scheduled before acknowledgement.", "reason", m.messageRetries)
	writeCounterMap(w, "wager_dlq_total", "Wager messages left for broker DLQ redrive.", "reason", m.deadLetterMessages)
	writeCounterMap(w, "wager_concurrency_conflicts_total", "Concurrent write conflicts detected while processing wagers.", "scope", m.concurrencyConflicts)
	writeCounterMap(w, "outbox_publish_results_total", "Outbox publishing attempts by result.", "result", m.outboxPublishResults)
	writeCounterMap(w, "outbox_publish_retries_total", "Outbox publish retries by attempt number.", "attempt", m.outboxPublishRetries)
	_, _ = fmt.Fprintf(w, "# HELP outbox_claim_failures_total Outbox claim attempts that failed before reserving an event.\n# TYPE outbox_claim_failures_total counter\noutbox_claim_failures_total %d\n", m.outboxClaimFailures)
	writeDurationSummary(w, "outbox_lag_seconds", "Delay between event occurrence and outbox publication attempt.", m.outboxLag)
	writeDurationSummary(w, "wager_processing_latency_seconds", "Time spent processing wager requests or messages.", m.processingLatency)
}

func (s *durationSummary) observe(value time.Duration) {
	if value < 0 {
		value = 0
	}
	micros := uint64(value.Microseconds())
	s.count++
	s.sum += micros
	if micros > s.max {
		s.max = micros
	}
}

func writeCounterMap(w http.ResponseWriter, name, help, labelName string, values map[string]uint64) {
	_, _ = fmt.Fprintf(w, "# HELP %s %s\n# TYPE %s counter\n", name, help, name)
	for _, key := range sortedKeys(values) {
		_, _ = fmt.Fprintf(w, "%s{%s=%q} %d\n", name, labelName, key, values[key])
	}
}

func writeDurationSummary(w http.ResponseWriter, name, help string, value durationSummary) {
	_, _ = fmt.Fprintf(w, "# HELP %s %s\n# TYPE %s summary\n", name, help, name)
	_, _ = fmt.Fprintf(w, "%s_count %d\n", name, value.count)
	_, _ = fmt.Fprintf(w, "%s_sum %.6f\n", name, float64(value.sum)/1_000_000)
	_, _ = fmt.Fprintf(w, "%s_max %.6f\n", name, float64(value.max)/1_000_000)
}

func sortedKeys(values map[string]uint64) []string {
	keys := make([]string, 0, len(values))
	for key := range values {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}

func label(value string) string {
	value = strings.TrimSpace(strings.ToLower(value))
	if value == "" {
		return "unknown"
	}
	value = strings.Map(func(r rune) rune {
		if r >= 'a' && r <= 'z' || r >= '0' && r <= '9' || r == '_' || r == '-' {
			return r
		}
		return '_'
	}, value)
	return value
}
