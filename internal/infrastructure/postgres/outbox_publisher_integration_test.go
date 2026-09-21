//go:build integration

package postgres

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/http/httputil"
	"net/url"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"github.com/d-dionisio/backend-challenge/internal/application"
	"github.com/d-dionisio/backend-challenge/internal/application/ports"
	"github.com/d-dionisio/backend-challenge/internal/domain"
	"github.com/d-dionisio/backend-challenge/internal/infrastructure/messaging"
	"github.com/d-dionisio/backend-challenge/internal/infrastructure/workers"
	"github.com/d-dionisio/backend-challenge/internal/observability"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"go.uber.org/fx"
)

var testOutboxPolicy = application.OutboxPolicy{LeaseDuration: time.Minute, InitialDelay: time.Hour, MaxDelay: 2 * time.Hour}

func outboxQueue(t *testing.T, ctx context.Context) (*sqs.Client, string) {
	t.Helper()
	endpoint := os.Getenv("TEST_SQS_ENDPOINT")
	if endpoint == "" {
		t.Fatal("integration requires TEST_SQS_ENDPOINT")
	}
	client := sqs.NewFromConfig(aws.Config{Region: "us-east-1", Credentials: credentials.NewStaticCredentialsProvider("test", "test", "")}, func(o *sqs.Options) {
		o.BaseEndpoint = aws.String(endpoint)
		o.RetryMaxAttempts = 1
	})
	queue, err := client.CreateQueue(ctx, &sqs.CreateQueueInput{QueueName: aws.String("outbox-test-" + uuid.NewString())})
	if err != nil {
		t.Fatal(err)
	}
	queueURL, err := url.Parse(aws.ToString(queue.QueueUrl))
	if err != nil {
		t.Fatal(err)
	}
	base, err := url.Parse(endpoint)
	if err != nil {
		t.Fatal(err)
	}
	// O teste em Docker acessa o mesmo LocalStack por host.docker.internal.
	queueURL.Scheme, queueURL.Host = base.Scheme, base.Host
	address := queueURL.String()
	configureTestQueuePolicy(t, ctx, client, address)
	t.Cleanup(func() {
		cleanup, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if _, err := client.DeleteQueue(cleanup, &sqs.DeleteQueueInput{QueueUrl: aws.String(address)}); err != nil {
			t.Error(err)
		}
	})
	return client, address
}

func outboxSender(t *testing.T, queueURL string) *messaging.SQSPublisher {
	t.Helper()
	t.Setenv("AWS_ACCESS_KEY_ID", "test")
	t.Setenv("AWS_SECRET_ACCESS_KEY", "test")
	t.Setenv("AWS_SESSION_TOKEN", "")
	var sender *messaging.SQSPublisher
	app := fx.New(fx.Supply(messaging.Config{Region: "us-east-1", Endpoint: os.Getenv("TEST_SQS_ENDPOINT"), QueueURL: queueURL}),
		fx.Provide(messaging.NewSQSPublisher), fx.Populate(&sender), fx.NopLogger)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := app.Start(ctx); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := app.Stop(ctx); err != nil {
			t.Error(err)
		}
	})
	return sender
}

func seedOutboxEvent(t *testing.T, ctx context.Context, pool *pgxpool.Pool) {
	t.Helper()
	// LOSS gera exatamente um evento e não muda o saldo.
	wallet := openWagerWallet(t, ctx, pool, "0")
	input := wagerInput(t, wallet, domain.WagerKindLoss, "0", "loss-1")
	input.CorrelationID = "outbox-correlation"
	if _, err := application.NewProcessWager(NewUnitOfWork(pool)).Execute(ctx, "provider-a", input); err != nil {
		t.Fatal(err)
	}
}

func readOutboxMessages(t *testing.T, ctx context.Context, client *sqs.Client, queueURL string, want int) []types.Message {
	t.Helper()
	var messages []types.Message
	for len(messages) < want {
		result, err := client.ReceiveMessage(ctx, &sqs.ReceiveMessageInput{QueueUrl: aws.String(queueURL), MaxNumberOfMessages: 10, WaitTimeSeconds: 1, VisibilityTimeout: 60})
		if err != nil {
			t.Fatal(err)
		}
		messages = append(messages, result.Messages...)
	}
	if len(messages) != want {
		t.Fatalf("got %d messages want %d", len(messages), want)
	}
	return messages
}

func TestOutboxTwoPublishersAndImmutableSnapshot(t *testing.T) {
	pool := storageTestPool(t)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	client, queueURL := outboxQueue(t, ctx)
	sender := outboxSender(t, queueURL)
	seedOutboxEvent(t, ctx, pool)
	type outcome struct {
		event *ports.PendingEvent
		err   error
	}
	results := make(chan outcome, 2)
	start := make(chan struct{})
	for i := 0; i < 2; i++ {
		go func() {
			<-start
			event, err := application.NewPublishOutbox(NewOutboxPublisherRepository(pool), sender).Execute(ctx, testOutboxPolicy)
			results <- outcome{event, err}
		}()
	}
	close(start)
	var published *ports.PendingEvent
	for i := 0; i < 2; i++ {
		got := <-results
		if got.err != nil {
			t.Fatal(got.err)
		}
		if got.event != nil {
			if published != nil {
				t.Fatal("two publishers acquired the same event")
			}
			published = got.event
		}
	}
	if published == nil {
		t.Fatal("event not published")
	}
	messages := readOutboxMessages(t, ctx, client, queueURL, 1)
	if aws.ToString(messages[0].Body) != string(published.Payload) {
		t.Fatal("snapshot was changed")
	}
	var done bool
	if err := pool.QueryRow(ctx, `SELECT published_at IS NOT NULL AND lock_token IS NULL AND attempts=1 FROM outbox`).Scan(&done); err != nil || !done {
		t.Fatal(done, err)
	}
	var envelope struct {
		EventID       uuid.UUID `json:"eventId"`
		CorrelationID string    `json:"correlationId"`
	}
	if err := json.Unmarshal([]byte(aws.ToString(messages[0].Body)), &envelope); err != nil {
		t.Fatal(err)
	}
	if envelope.EventID != published.EventID || envelope.CorrelationID != "outbox-correlation" {
		t.Fatal("identity or correlation changed")
	}
}

type failedEventPublisher struct{}

func (failedEventPublisher) Publish(context.Context, *ports.PendingEvent) error {
	return errors.New("simulated send failure")
}

func TestOutboxRetryPersistsAndRecovers(t *testing.T) {
	pool := storageTestPool(t)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	client, queueURL := outboxQueue(t, ctx)
	sender := outboxSender(t, queueURL)
	seedOutboxEvent(t, ctx, pool)
	repository := NewOutboxPublisherRepository(pool)
	failed, err := application.NewPublishOutbox(repository, failedEventPublisher{}).Execute(ctx, testOutboxPolicy)
	if err == nil || failed == nil {
		t.Fatal("expected send failure")
	}
	var scheduled bool
	if err := pool.QueryRow(ctx, `SELECT attempts=1 AND next_attempt_at>clock_timestamp()+interval '59 minutes' AND published_at IS NULL AND lock_token IS NULL FROM outbox`).Scan(&scheduled); err != nil || !scheduled {
		t.Fatal(scheduled, err)
	}
	// Outra instância também respeita o agendamento persistido.
	next := application.NewPublishOutbox(NewOutboxPublisherRepository(pool), sender)
	if event, err := next.Execute(ctx, testOutboxPolicy); err != nil || event != nil {
		t.Fatal("retry ran early", err)
	}
	if _, err := pool.Exec(ctx, `UPDATE outbox SET next_attempt_at=clock_timestamp()-interval '1 second'`); err != nil {
		t.Fatal(err)
	}
	event, err := next.Execute(ctx, testOutboxPolicy)
	if err != nil || event == nil {
		t.Fatal(err)
	}
	if event.Attempts != 2 || event.EventID != failed.EventID {
		t.Fatal("retry lost identity or attempt count")
	}
	readOutboxMessages(t, ctx, client, queueURL, 1)
}

func TestOutboxCannotSeeUncommittedEvent(t *testing.T) {
	pool := storageTestPool(t)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	wallet, err := domain.NewWallet(uuid.New(), storageMoney(t, "100"))
	if err != nil {
		t.Fatal(err)
	}
	forced := errors.New("rollback")
	err = NewUnitOfWork(pool).WithinTransaction(ctx, func(r ports.Repositories) error {
		if err := storeOpening(ctx, r, wallet); err != nil {
			return err
		}
		event, err := NewOutboxPublisherRepository(pool).Claim(ctx, time.Minute)
		if err != nil {
			return err
		}
		if event != nil {
			return errors.New("publisher saw an uncommitted event")
		}
		return forced
	})
	if !errors.Is(err, forced) {
		t.Fatal(err)
	}
	if countRows(t, pool, "outbox") != 0 {
		t.Fatal("rolled back event persisted")
	}
}

func TestOutboxExpiredOwnerCannotConfirmOrReschedule(t *testing.T) {
	pool := storageTestPool(t)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	seedOutboxEvent(t, ctx, pool)
	repo := NewOutboxPublisherRepository(pool)
	old, err := repo.Claim(ctx, time.Minute)
	if err != nil || old == nil {
		t.Fatal(err)
	}
	if event, err := repo.Claim(ctx, time.Minute); err != nil || event != nil {
		t.Fatal("active lease stolen", err)
	}
	if _, err := pool.Exec(ctx, `UPDATE outbox SET locked_until=clock_timestamp()-interval '1 second'`); err != nil {
		t.Fatal(err)
	}
	if err := repo.MarkPublished(ctx, old); !errors.Is(err, ports.ErrOutboxLeaseLost) {
		t.Fatal(err)
	}
	newOwner, err := repo.Claim(ctx, time.Minute)
	if err != nil || newOwner == nil {
		t.Fatal(err)
	}
	if old.LockToken == newOwner.LockToken {
		t.Fatal("reservation token reused")
	}
	if err := repo.MarkPublished(ctx, old); !errors.Is(err, ports.ErrOutboxLeaseLost) {
		t.Fatal(err)
	}
	if err := repo.Retry(ctx, old, time.Hour); !errors.Is(err, ports.ErrOutboxLeaseLost) {
		t.Fatal(err)
	}
	if err := repo.Retry(ctx, newOwner, time.Second); err != nil {
		t.Fatal(err)
	}
}

// Este teste só trabalha quando iniciado como subprocesso pelo teste abaixo.
func TestOutboxPublisherCrashHelper(t *testing.T) {
	mode := os.Getenv("OUTBOX_CRASH_MODE")
	if mode == "" {
		t.Skip("subprocess helper")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	config, err := pgxpool.ParseConfig(os.Getenv("TEST_DATABASE_URL"))
	if err != nil {
		t.Fatal(err)
	}
	config.ConnConfig.RuntimeParams["search_path"] = os.Getenv("OUTBOX_TEST_SCHEMA")
	pool, err := pgxpool.NewWithConfig(ctx, config)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	sender := outboxSender(t, os.Getenv("OUTBOX_TEST_QUEUE"))
	event, err := NewOutboxPublisherRepository(pool).Claim(ctx, time.Minute)
	if err != nil || event == nil {
		t.Fatal(err)
	}
	if mode == "after-send" {
		if err := sender.Publish(ctx, event); err != nil {
			t.Fatal(err)
		}
	}
	fmt.Println("OUTBOX_CRASH_READY")
	<-ctx.Done() // O processo pai interrompe antes de confirmar a publicação.
	t.Fatal("parent did not interrupt child")
}

func TestOutboxRecoveryAfterPublisherProcessKilled(t *testing.T) {
	for _, mode := range []string{"before-send", "after-send"} {
		t.Run(mode, func(t *testing.T) {
			pool := storageTestPool(t)
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
			client, queueURL := outboxQueue(t, ctx)
			sender := outboxSender(t, queueURL)
			seedOutboxEvent(t, ctx, pool)
			command := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestOutboxPublisherCrashHelper$", "-test.v")
			command.Env = append(os.Environ(), "OUTBOX_CRASH_MODE="+mode, "OUTBOX_TEST_QUEUE="+queueURL,
				"OUTBOX_TEST_SCHEMA="+pool.Config().ConnConfig.RuntimeParams["search_path"])
			command.Stderr = os.Stderr
			stdout, err := command.StdoutPipe()
			if err != nil {
				t.Fatal(err)
			}
			if err := command.Start(); err != nil {
				t.Fatal(err)
			}
			defer command.Process.Kill()
			ready := false
			var childOutput strings.Builder
			scanner := bufio.NewScanner(stdout)
			for scanner.Scan() {
				childOutput.WriteString(scanner.Text() + "\n")
				if scanner.Text() == "OUTBOX_CRASH_READY" {
					ready = true
					break
				}
			}
			if err := command.Process.Kill(); err != nil && ready {
				t.Fatal(err)
			}
			_ = command.Wait()
			if !ready {
				t.Fatalf("child failed: %s", childOutput.String())
			}
			repo := NewOutboxPublisherRepository(pool)
			if event, err := repo.Claim(ctx, time.Minute); err != nil || event != nil {
				t.Fatal("reservation did not survive process death", err)
			}
			// Avança somente o prazo do registro de teste, evitando dormir um minuto.
			if _, err := pool.Exec(ctx, `UPDATE outbox SET locked_until=clock_timestamp()-interval '1 second'`); err != nil {
				t.Fatal(err)
			}
			event, err := application.NewPublishOutbox(repo, sender).Execute(ctx, testOutboxPolicy)
			if err != nil || event == nil {
				t.Fatal(err)
			}
			want := 1
			if mode == "after-send" {
				want = 2
			}
			messages := readOutboxMessages(t, ctx, client, queueURL, want)
			ids := make(map[string]bool)
			for _, message := range messages {
				if aws.ToString(message.Body) != string(event.Payload) {
					t.Fatal("republication changed snapshot")
				}
				ids[aws.ToString(message.MessageId)] = true
			}
			if len(ids) != want {
				t.Fatal("expected distinct SQS deliveries")
			}
			var done bool
			if err := pool.QueryRow(ctx, `SELECT published_at IS NOT NULL AND attempts=2 FROM outbox`).Scan(&done); err != nil || !done {
				t.Fatal(done, err)
			}
		})
	}
}

func TestOutboxFullFxLifecycle(t *testing.T) {
	schemaPool := storageTestPool(t)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	client, queueURL := outboxQueue(t, ctx)
	seedOutboxEvent(t, ctx, schemaPool)
	t.Setenv("AWS_ACCESS_KEY_ID", "test")
	t.Setenv("AWS_SECRET_ACCESS_KEY", "test")
	t.Setenv("AWS_SESSION_TOKEN", "")
	databaseURL, err := url.Parse(os.Getenv("TEST_DATABASE_URL"))
	if err != nil {
		t.Fatal(err)
	}
	query := databaseURL.Query()
	query.Set("search_path", schemaPool.Config().ConnConfig.RuntimeParams["search_path"])
	databaseURL.RawQuery = query.Encode()
	var workerPool *pgxpool.Pool
	app := fx.New(Module, application.Module, messaging.Module, workers.Module, fx.Provide(observability.NewMetrics),
		fx.Replace(Config{DatabaseURL: databaseURL.String()},
			messaging.Config{Region: "us-east-1", Endpoint: os.Getenv("TEST_SQS_ENDPOINT"), QueueURL: queueURL},
			workers.OutboxConfig{PollInterval: 10 * time.Millisecond, AttemptTimeout: 5 * time.Second,
				Policy: application.OutboxPolicy{LeaseDuration: time.Minute, InitialDelay: time.Second, MaxDelay: time.Minute}},
			workers.ReferenceConfig{PollInterval: time.Second, AttemptTimeout: 5 * time.Second,
				Policy: application.ReferenceRetryPolicy{MaxAttempts: 10, InitialDelay: time.Second, MaxDelay: time.Minute}}),
		fx.Populate(&workerPool), fx.NopLogger)
	if err := app.Start(ctx); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		cleanup, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := app.Stop(cleanup); err != nil {
			t.Error(err)
		}
	})
	readOutboxMessages(t, ctx, client, queueURL, 1)
	for {
		var done bool
		if err := schemaPool.QueryRow(ctx, `SELECT published_at IS NOT NULL FROM outbox`).Scan(&done); err != nil {
			t.Fatal(err)
		}
		if done {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if err := app.Stop(ctx); err != nil {
		t.Fatal(err)
	}
	if err := workerPool.Ping(ctx); err == nil {
		t.Fatal("Fx did not close the pool after stopping the workers")
	}
}

func TestOutboxShutdownDuringSQSSend(t *testing.T) {
	pool := storageTestPool(t)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	client, queueURL := outboxQueue(t, ctx)
	seedOutboxEvent(t, ctx, pool)
	endpoint, err := url.Parse(os.Getenv("TEST_SQS_ENDPOINT"))
	if err != nil {
		t.Fatal(err)
	}
	proxy := httputil.NewSingleHostReverseProxy(endpoint)
	sending := make(chan struct{}, 1)
	// A consulta inicial chega ao LocalStack real. Só o envio fica parado
	// no proxy para testar cancelamento durante uma chamada HTTP em andamento.
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasSuffix(r.Header.Get("X-Amz-Target"), ".SendMessage") {
			_, _ = io.Copy(io.Discard, r.Body)
			sending <- struct{}{}
			<-r.Context().Done()
			return
		}
		proxy.ServeHTTP(w, r)
	}))
	defer server.Close()
	t.Setenv("AWS_ACCESS_KEY_ID", "test")
	t.Setenv("AWS_SECRET_ACCESS_KEY", "test")
	t.Setenv("AWS_SESSION_TOKEN", "")
	app := fx.New(fx.Supply(messaging.Config{Region: "us-east-1", Endpoint: server.URL, QueueURL: queueURL},
		workers.OutboxConfig{PollInterval: time.Second, AttemptTimeout: 10 * time.Second,
			Policy: application.OutboxPolicy{LeaseDuration: time.Minute, InitialDelay: time.Second, MaxDelay: time.Minute}}),
		fx.Provide(fx.Annotate(messaging.NewSQSPublisher, fx.As(new(ports.EventPublisher))),
			func() ports.OutboxPublisherRepository { return NewOutboxPublisherRepository(pool) }, application.NewPublishOutbox),
		fx.Provide(observability.NewMetrics), fx.Invoke(workers.RegisterOutboxWorker), fx.NopLogger)
	if err := app.Start(ctx); err != nil {
		t.Fatal(err)
	}
	defer func() {
		cleanup, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := app.Stop(cleanup); err != nil {
			t.Error(err)
		}
	}()
	select {
	case <-sending:
	case <-ctx.Done():
		t.Fatal("publisher did not start sending")
	}
	stopCtx, stopCancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer stopCancel()
	if err := app.Stop(stopCtx); err != nil {
		t.Fatal(err)
	}
	var pending bool
	if err := pool.QueryRow(ctx, `SELECT published_at IS NULL AND lock_token IS NOT NULL FROM outbox`).Scan(&pending); err != nil || !pending {
		t.Fatal(pending, err)
	}
	if pool.Stat().AcquiredConns() != 0 {
		t.Fatal("publisher retained a database connection")
	}
	if _, err := pool.Exec(ctx, `UPDATE outbox SET locked_until=clock_timestamp()-interval '1 second'`); err != nil {
		t.Fatal(err)
	}
	sender := outboxSender(t, queueURL)
	if event, err := application.NewPublishOutbox(NewOutboxPublisherRepository(pool), sender).Execute(ctx, testOutboxPolicy); err != nil || event == nil {
		t.Fatal(err)
	}
	readOutboxMessages(t, ctx, client, queueURL, 1)
}
