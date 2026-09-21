//go:build integration

package postgres

import (
	"bufio"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
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
	"github.com/d-dionisio/backend-challenge/internal/infrastructure/auth"
	"github.com/d-dionisio/backend-challenge/internal/infrastructure/messaging"
	"github.com/d-dionisio/backend-challenge/internal/infrastructure/workers"
	"github.com/d-dionisio/backend-challenge/internal/observability"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"go.uber.org/fx"
)

func wagerQueues(t *testing.T, ctx context.Context) (*sqs.Client, string, string) {
	t.Helper()
	endpoint := os.Getenv("TEST_SQS_ENDPOINT")
	if endpoint == "" {
		t.Fatal("integration requires TEST_SQS_ENDPOINT")
	}
	client := sqs.NewFromConfig(aws.Config{Region: "us-east-1", Credentials: credentials.NewStaticCredentialsProvider("test", "test", "")}, func(o *sqs.Options) { o.BaseEndpoint = aws.String(endpoint); o.RetryMaxAttempts = 1 })
	create := func(name string, attributes map[string]string) string {
		queue, err := client.CreateQueue(ctx, &sqs.CreateQueueInput{QueueName: aws.String(name), Attributes: attributes})
		if err != nil {
			t.Fatal(err)
		}
		address, err := url.Parse(aws.ToString(queue.QueueUrl))
		if err != nil {
			t.Fatal(err)
		}
		base, err := url.Parse(endpoint)
		if err != nil {
			t.Fatal(err)
		}
		address.Scheme, address.Host = base.Scheme, base.Host
		queueURL := address.String()
		t.Cleanup(func() {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			if _, err := client.DeleteQueue(ctx, &sqs.DeleteQueueInput{QueueUrl: aws.String(queueURL)}); err != nil {
				t.Error(err)
			}
		})
		return queueURL
	}
	dlq := create("wager-test-dlq-"+uuid.NewString()+".fifo", map[string]string{"FifoQueue": "true"})
	attributes, err := client.GetQueueAttributes(ctx, &sqs.GetQueueAttributesInput{QueueUrl: aws.String(dlq), AttributeNames: []types.QueueAttributeName{types.QueueAttributeNameQueueArn}})
	if err != nil {
		t.Fatal(err)
	}
	redrive, err := json.Marshal(map[string]any{"deadLetterTargetArn": attributes.Attributes["QueueArn"], "maxReceiveCount": 5})
	if err != nil {
		t.Fatal(err)
	}
	queue := create("wager-test-"+uuid.NewString()+".fifo", map[string]string{"FifoQueue": "true", "ContentBasedDeduplication": "false", "VisibilityTimeout": "30", "RedrivePolicy": string(redrive)})
	sourceARN := configureTestQueuePolicy(t, ctx, client, queue)
	allow, err := json.Marshal(map[string]any{"redrivePermission": "byQueue", "sourceQueueArns": []string{sourceARN}})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := client.SetQueueAttributes(ctx, &sqs.SetQueueAttributesInput{QueueUrl: aws.String(dlq), Attributes: map[string]string{"RedriveAllowPolicy": string(allow)}}); err != nil {
		t.Fatal(err)
	}
	return client, queue, dlq
}

func startWagerConsumer(t *testing.T, pool *pgxpool.Pool, queueURL string, worker bool) (*messaging.SQSConsumer, *fx.App) {
	t.Helper()
	t.Setenv("AWS_ACCESS_KEY_ID", "test")
	t.Setenv("AWS_SECRET_ACCESS_KEY", "test")
	t.Setenv("AWS_SESSION_TOKEN", "")
	var consumer *messaging.SQSConsumer
	options := []fx.Option{
		fx.Provide(observability.NewMetrics),
		fx.Supply(messaging.Config{Region: "us-east-1", Endpoint: os.Getenv("TEST_SQS_ENDPOINT")},
			messaging.ConsumerConfig{QueueURL: queueURL, ProviderBySender: map[string]string{"000000000000": "provider-a"}, ProcessingTimeout: 5 * time.Second}),
		fx.Provide(func(lifecycle fx.Lifecycle) *application.ProcessWager {
			if worker {
				// Neste teste o worker recebe seu próprio pool, com o mesmo
				// fechamento após o worker que existe na composição principal.
				lifecycle.Append(fx.Hook{OnStop: func(context.Context) error { pool.Close(); return nil }})
			}
			return application.NewProcessWager(NewUnitOfWork(pool))
		}, messaging.NewSQSConsumer),
		fx.Populate(&consumer), fx.NopLogger,
	}
	if worker {
		options = append(options, workers.WagerModule)
	}
	app := fx.New(options...)
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
	return consumer, app
}

func wagerMessageBody(t *testing.T, input application.ProcessWagerInput, messageID string) string {
	t.Helper()
	message := messaging.WagerMessage{MessageID: messageID, Type: "WagerTransactionRequested", OccurredAt: time.Date(2026, 9, 8, 12, 0, 0, 0, time.UTC)}
	message.Data.ProviderID = input.ProviderID
	message.Data.ExternalTransactionID = input.ExternalTransactionID
	message.Data.IdempotencyKey = input.IdempotencyKey
	message.Data.PlayerID = input.PlayerID
	message.Data.WalletID = input.WalletID
	message.Data.RoundID = input.RoundID
	message.Data.GameID = input.GameID
	message.Data.Kind = input.Kind
	message.Data.Money.Amount = input.Money.String()
	message.Data.Money.Currency = input.Money.Currency()
	message.Data.ReferenceExternalTransactionID = input.ReferenceExternalTransactionID
	body, err := json.Marshal(message)
	if err != nil {
		t.Fatal(err)
	}
	return string(body)
}

func sendWagerMessage(t *testing.T, ctx context.Context, client *sqs.Client, queueURL, body, group string) {
	t.Helper()
	// Um ID de transporte novo força o broker a entregar duplicatas do mesmo
	// envelope. Assim o teste exercita a inbox, não a deduplicação FIFO.
	_, err := client.SendMessage(ctx, &sqs.SendMessageInput{QueueUrl: aws.String(queueURL), MessageBody: aws.String(body), MessageGroupId: aws.String(group), MessageDeduplicationId: aws.String(uuid.NewString())})
	if err != nil {
		t.Fatal(err)
	}
}

func receiveWagerMessage(t *testing.T, ctx context.Context, consumer *messaging.SQSConsumer) *types.Message {
	t.Helper()
	message, err := consumer.Receive(ctx)
	if err != nil || message == nil {
		t.Fatal("message not received", err)
	}
	return message
}

func TestSQSInboxRedeliveryAndSharedIdempotency(t *testing.T) {
	pool := storageTestPool(t)
	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Second)
	defer cancel()
	client, queue, _ := wagerQueues(t, ctx)
	consumer, _ := startWagerConsumer(t, pool, queue, false)
	wallet := openWagerWallet(t, ctx, pool, "100")
	input := wagerInput(t, wallet, domain.WagerKindBet, "25", "bet-1")
	body := wagerMessageBody(t, input, "message-1")
	var transportIDs []string
	for i := 0; i < 2; i++ {
		sendWagerMessage(t, ctx, client, queue, body, wallet.ID().String())
		message := receiveWagerMessage(t, ctx, consumer)
		transportIDs = append(transportIDs, aws.ToString(message.MessageId))
		if i == 0 {
			// Disputa entre o adaptador SQS e a entrada compartilhada que será
			// usada pelo HTTP. A unicidade depende do banco, não do FIFO.
			results := make(chan error, 2)
			start := make(chan struct{})
			go func() { <-start; results <- consumer.Handle(ctx, message) }()
			go func() {
				<-start
				_, err := application.NewProcessWager(NewUnitOfWork(pool)).Execute(ctx, "provider-a", input)
				results <- err
			}()
			close(start)
			for j := 0; j < 2; j++ {
				if err := <-results; err != nil {
					t.Fatal(err)
				}
			}
		} else if err := consumer.Handle(ctx, message); err != nil {
			t.Fatal(err)
		}
	}
	if transportIDs[0] == transportIDs[1] {
		t.Fatal("broker deduplicated the test deliveries")
	}
	assertWalletLedger(t, ctx, pool, wallet.ID(), 7500)
	if countRows(t, pool, "inbox") != 1 || countRows(t, pool, "wallet_ledger_entries") != 2 || countRows(t, pool, "outbox") != 4 {
		t.Fatal("redelivery repeated financial effects")
	}
	useCase := application.NewProcessWager(NewUnitOfWork(pool))
	if _, err := useCase.Execute(ctx, "provider-a", wagerInput(t, wallet, domain.WagerKindWin, "10", "win-1")); err != nil {
		t.Fatal(err)
	}
	replay, err := useCase.Execute(ctx, "provider-a", input)
	if err != nil || !replay.IdempotentReplay || replay.Balance.String() != "75.00" {
		t.Fatal(replay, err)
	}
	// Mesmo envelope com conteúdo alterado: a inbox deve impedir tratamento.
	input.Money = storageMoney(t, "30")
	changed := wagerMessageBody(t, input, "message-1")
	_, err = useCase.ExecuteMessage(ctx, "provider-a", input, "message-1", []byte(changed))
	if !errors.Is(err, ports.ErrInboxConflict) {
		t.Fatal(err)
	}
	assertWalletLedger(t, ctx, pool, wallet.ID(), 8500)
}

func TestSQSAtomicRollbackAndTransientRetry(t *testing.T) {
	pool := storageTestPool(t)
	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Second)
	defer cancel()
	client, queue, _ := wagerQueues(t, ctx)
	consumer, _ := startWagerConsumer(t, pool, queue, false)
	wallet := openWagerWallet(t, ctx, pool, "100")
	if _, err := pool.Exec(ctx, `CREATE FUNCTION fail_inbox_completion() RETURNS TRIGGER AS $$ BEGIN RAISE EXCEPTION 'test failure'; END; $$ LANGUAGE plpgsql;
		CREATE TRIGGER fail_inbox_completion BEFORE UPDATE ON inbox FOR EACH ROW EXECUTE FUNCTION fail_inbox_completion();`); err != nil {
		t.Fatal(err)
	}
	body := wagerMessageBody(t, wagerInput(t, wallet, domain.WagerKindBet, "25", "bet-1"), "message-1")
	sendWagerMessage(t, ctx, client, queue, body, wallet.ID().String())
	first := receiveWagerMessage(t, ctx, consumer)
	if err := consumer.Handle(ctx, first); err == nil {
		t.Fatal("expected transaction failure")
	}
	assertWalletLedger(t, ctx, pool, wallet.ID(), 10000)
	if countRows(t, pool, "inbox") != 0 || countRows(t, pool, "wager_transactions") != 1 || countRows(t, pool, "outbox") != 2 {
		t.Fatal("failed transaction left financial or inbox records")
	}
	if _, err := pool.Exec(ctx, `DROP TRIGGER fail_inbox_completion ON inbox`); err != nil {
		t.Fatal(err)
	}
	// Nova composição simula retomada por outra instância, com o mesmo banco.
	secondConsumer, _ := startWagerConsumer(t, pool, queue, false)
	second := receiveWagerMessage(t, ctx, secondConsumer)
	if second.Attributes["ApproximateReceiveCount"] != "2" || aws.ToString(first.MessageId) != aws.ToString(second.MessageId) {
		t.Fatal("message was not redelivered")
	}
	if err := secondConsumer.Handle(ctx, second); err != nil {
		t.Fatal(err)
	}
	assertWalletLedger(t, ctx, pool, wallet.ID(), 7500)
}

func TestSQSPermanentErrorsReachDLQ(t *testing.T) {
	for _, scenario := range []string{"invalid-json", "unauthorized-provider", "external-opening"} {
		t.Run(scenario, func(t *testing.T) {
			pool := storageTestPool(t)
			ctx, cancel := context.WithTimeout(context.Background(), 25*time.Second)
			defer cancel()
			client, queue, dlq := wagerQueues(t, ctx)
			consumer, _ := startWagerConsumer(t, pool, queue, false)
			wallet := openWagerWallet(t, ctx, pool, "100")
			input := wagerInput(t, wallet, domain.WagerKindBet, "25", "bet-1")
			if scenario == "unauthorized-provider" {
				input.ProviderID = "provider-b"
			}
			if scenario == "external-opening" {
				input.Kind = domain.WagerKindOpening
			}
			body := wagerMessageBody(t, input, "message-1")
			if scenario == "invalid-json" {
				body = "{invalid"
			}
			sendWagerMessage(t, ctx, client, queue, body, wallet.ID().String())
			for i := 0; i < 5; i++ {
				message := receiveWagerMessage(t, ctx, consumer)
				if err := consumer.Handle(ctx, message); err == nil {
					t.Fatal("invalid message accepted")
				}
			}
			// A próxima busca dispara o redrive do broker.
			if _, err := client.ReceiveMessage(ctx, &sqs.ReceiveMessageInput{QueueUrl: aws.String(queue), WaitTimeSeconds: 1}); err != nil {
				t.Fatal(err)
			}
			messages := readOutboxMessages(t, ctx, client, dlq, 1)
			if aws.ToString(messages[0].Body) != body {
				t.Fatal("DLQ lost original envelope")
			}
			assertWalletLedger(t, ctx, pool, wallet.ID(), 10000)
			if countRows(t, pool, "inbox") != 0 || countRows(t, pool, "wager_transactions") != 1 {
				t.Fatal("invalid input affected financial storage")
			}
		})
	}
}

func TestSQSTerminalRejectionAndPendingReferenceAreAcknowledged(t *testing.T) {
	pool := storageTestPool(t)
	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Second)
	defer cancel()
	client, queue, _ := wagerQueues(t, ctx)
	consumer, _ := startWagerConsumer(t, pool, queue, false)
	wallet := openWagerWallet(t, ctx, pool, "100")
	bet := wagerInput(t, wallet, domain.WagerKindBet, "200", "too-large")
	refund := wagerInput(t, wallet, domain.WagerKindRefund, "25", "early-refund")
	reference := "late-bet"
	refund.ReferenceExternalTransactionID = &reference
	for _, input := range []application.ProcessWagerInput{bet, refund} {
		sendWagerMessage(t, ctx, client, queue, wagerMessageBody(t, input, input.ExternalTransactionID), wallet.ID().String())
		if err := consumer.Handle(ctx, receiveWagerMessage(t, ctx, consumer)); err != nil {
			t.Fatal(err)
		}
	}
	var completed int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM inbox WHERE completed_at IS NOT NULL`).Scan(&completed); err != nil || completed != 2 {
		t.Fatal(completed, err)
	}
	var rejected, pending int
	if err := pool.QueryRow(ctx, `SELECT count(*) FILTER(WHERE status='REJECTED'),count(*) FILTER(WHERE status='PENDING_REFERENCE') FROM wager_transactions`).Scan(&rejected, &pending); err != nil || rejected != 1 || pending != 1 {
		t.Fatal(rejected, pending, err)
	}
	if _, err := application.NewProcessWager(NewUnitOfWork(pool)).Execute(ctx, "provider-a", wagerInput(t, wallet, domain.WagerKindBet, "25", reference)); err != nil {
		t.Fatal(err)
	}
	if _, err := application.NewRetryReferences(NewUnitOfWork(pool)).Execute(ctx, application.ReferenceRetryPolicy{MaxAttempts: 2, InitialDelay: time.Second, MaxDelay: time.Minute}); err != nil {
		t.Fatal(err)
	}
	assertWalletLedger(t, ctx, pool, wallet.ID(), 10000)
	result, err := client.ReceiveMessage(ctx, &sqs.ReceiveMessageInput{QueueUrl: aws.String(queue), WaitTimeSeconds: 1, VisibilityTimeout: 0})
	if err != nil || len(result.Messages) != 0 {
		t.Fatal("completed messages were not deleted", err)
	}
}

func TestSQSTransientExhaustionReachesDLQ(t *testing.T) {
	pool := storageTestPool(t)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	client, queue, dlq := wagerQueues(t, ctx)
	consumer, _ := startWagerConsumer(t, pool, queue, false)
	// Carteira ainda ausente: erro transitório que pode se resolver numa nova entrega.
	wallet, err := domain.NewWallet(uuid.New(), storageMoney(t, "100"))
	if err != nil {
		t.Fatal(err)
	}
	body := wagerMessageBody(t, wagerInput(t, wallet, domain.WagerKindBet, "25", "bet-1"), "missing-wallet")
	sendWagerMessage(t, ctx, client, queue, body, wallet.ID().String())
	for i := 0; i < 5; i++ {
		message := receiveWagerMessage(t, ctx, consumer)
		if err := consumer.Handle(ctx, message); !errors.Is(err, ports.ErrWalletNotFound) {
			t.Fatal(err)
		}
		// Acelera apenas o relógio do broker neste teste; o cálculo do backoff
		// é verificado separadamente e a reentrega anterior usa o atraso real.
		if err := consumer.ChangeVisibility(ctx, message, 0); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := client.ReceiveMessage(ctx, &sqs.ReceiveMessageInput{QueueUrl: aws.String(queue), WaitTimeSeconds: 1}); err != nil {
		t.Fatal(err)
	}
	messages := readOutboxMessages(t, ctx, client, dlq, 1)
	if aws.ToString(messages[0].Body) != body || countRows(t, pool, "inbox") != 0 {
		t.Fatal("exhausted message incorrectly completed")
	}
}

func TestSQSConsumerCrashHelper(t *testing.T) {
	if os.Getenv("SQS_CRASH_HELPER") != "1" {
		t.Skip("subprocess helper")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	config, err := pgxpool.ParseConfig(os.Getenv("TEST_DATABASE_URL"))
	if err != nil {
		t.Fatal(err)
	}
	config.ConnConfig.RuntimeParams["search_path"] = os.Getenv("SQS_TEST_SCHEMA")
	pool, err := pgxpool.NewWithConfig(ctx, config)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	consumer, _ := startWagerConsumer(t, pool, os.Getenv("SQS_TEST_QUEUE"), false)
	received := receiveWagerMessage(t, ctx, consumer)
	body := aws.ToString(received.Body)
	message, input, err := messaging.ParseWagerMessage(body)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := application.NewProcessWager(NewUnitOfWork(pool)).ExecuteMessage(ctx, "provider-a", input, message.MessageID, []byte(body)); err != nil {
		t.Fatal(err)
	}
	// Ponto de falha: commit concluído, DeleteMessage ainda não foi chamado.
	fmt.Println("SQS_COMMITTED " + base64.RawStdEncoding.EncodeToString([]byte(aws.ToString(received.ReceiptHandle))))
	<-ctx.Done()
	t.Fatal("parent did not interrupt child")
}

func TestSQSRecoveryAfterCommitBeforeDelete(t *testing.T) {
	pool := storageTestPool(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	client, queue, _ := wagerQueues(t, ctx)
	consumer, _ := startWagerConsumer(t, pool, queue, false)
	wallet := openWagerWallet(t, ctx, pool, "100")
	body := wagerMessageBody(t, wagerInput(t, wallet, domain.WagerKindBet, "25", "bet-1"), "message-1")
	sendWagerMessage(t, ctx, client, queue, body, wallet.ID().String())
	command := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestSQSConsumerCrashHelper$", "-test.v")
	command.Env = append(os.Environ(), "SQS_CRASH_HELPER=1", "SQS_TEST_QUEUE="+queue,
		"SQS_TEST_SCHEMA="+pool.Config().ConnConfig.RuntimeParams["search_path"])
	command.Stderr = os.Stderr
	stdout, err := command.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	if err := command.Start(); err != nil {
		t.Fatal(err)
	}
	defer command.Process.Kill()
	var output strings.Builder
	var receipt string
	scanner := bufio.NewScanner(stdout)
	for scanner.Scan() {
		line := scanner.Text()
		output.WriteString(line + "\n")
		if strings.HasPrefix(line, "SQS_COMMITTED ") {
			decoded, err := base64.RawStdEncoding.DecodeString(strings.TrimPrefix(line, "SQS_COMMITTED "))
			if err != nil {
				t.Fatal(err)
			}
			receipt = string(decoded)
			break
		}
	}
	if err := command.Process.Kill(); err != nil && receipt != "" {
		t.Fatal(err)
	}
	_ = command.Wait()
	if receipt == "" {
		t.Fatalf("child failed: %s", output.String())
	}
	assertWalletLedger(t, ctx, pool, wallet.ID(), 7500)
	if err := consumer.ChangeVisibility(ctx, &types.Message{ReceiptHandle: aws.String(receipt)}, 0); err != nil {
		t.Fatal(err)
	}
	received := receiveWagerMessage(t, ctx, consumer)
	if received.Attributes["ApproximateReceiveCount"] != "2" {
		t.Fatal("expected actual redelivery after process death")
	}
	if err := consumer.Handle(ctx, received); err != nil {
		t.Fatal(err)
	}
	assertWalletLedger(t, ctx, pool, wallet.ID(), 7500)
	if countRows(t, pool, "inbox") != 1 || countRows(t, pool, "wallet_ledger_entries") != 2 || countRows(t, pool, "outbox") != 4 {
		t.Fatal("restart repeated financial effects")
	}
}

func TestSQSWorkerShutdownRollsBackAndReleasesVisibility(t *testing.T) {
	pool := storageTestPool(t)
	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Second)
	defer cancel()
	client, queue, _ := wagerQueues(t, ctx)
	wallet := openWagerWallet(t, ctx, pool, "100")
	if _, err := pool.Exec(ctx, `CREATE FUNCTION slow_consumer_event() RETURNS TRIGGER AS $$ BEGIN PERFORM pg_sleep(10); RETURN NEW; END; $$ LANGUAGE plpgsql;
		CREATE TRIGGER slow_consumer_event BEFORE INSERT ON outbox FOR EACH ROW EXECUTE FUNCTION slow_consumer_event();`); err != nil {
		t.Fatal(err)
	}
	body := wagerMessageBody(t, wagerInput(t, wallet, domain.WagerKindBet, "25", "bet-1"), "message-1")
	sendWagerMessage(t, ctx, client, queue, body, wallet.ID().String())
	config := pool.Config().Copy()
	applicationName := "consumer-" + uuid.NewString()
	config.ConnConfig.RuntimeParams["application_name"] = applicationName
	workerPool, err := pgxpool.NewWithConfig(ctx, config)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(workerPool.Close)
	_, app := startWagerConsumer(t, workerPool, queue, true)
	deadline := time.Now().Add(4 * time.Second)
	for {
		var busy bool
		if err := pool.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE application_name=$1 AND wait_event='PgSleep')`, applicationName).Scan(&busy); err != nil {
			t.Fatal(err)
		}
		if busy {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("consumer did not reach in-flight transaction")
		}
		time.Sleep(10 * time.Millisecond)
	}
	stopCtx, stopCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer stopCancel()
	if err := app.Stop(stopCtx); err != nil {
		t.Fatal(err)
	}
	if workerPool.Ping(ctx) == nil {
		t.Fatal("worker pool was not closed")
	}
	assertWalletLedger(t, ctx, pool, wallet.ID(), 10000)
	if countRows(t, pool, "inbox") != 0 || countRows(t, pool, "outbox") != 2 {
		t.Fatal("shutdown committed partial work")
	}
	if _, err := pool.Exec(ctx, `DROP TRIGGER slow_consumer_event ON outbox`); err != nil {
		t.Fatal(err)
	}
	consumer, _ := startWagerConsumer(t, pool, queue, false)
	// Não espera os 30s da reserva: o shutdown deve ter liberado a visibilidade.
	retryCtx, retryCancel := context.WithTimeout(ctx, 3*time.Second)
	defer retryCancel()
	message := receiveWagerMessage(t, retryCtx, consumer)
	if err := consumer.Handle(ctx, message); err != nil {
		t.Fatal(err)
	}
	assertWalletLedger(t, ctx, pool, wallet.ID(), 7500)
}

func TestSQSFullFxComposition(t *testing.T) {
	pool := storageTestPool(t)
	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Second)
	defer cancel()
	client, queue, _ := wagerQueues(t, ctx)
	_, eventsQueue := outboxQueue(t, ctx)
	wallet := openWagerWallet(t, ctx, pool, "100")
	sendWagerMessage(t, ctx, client, queue, wagerMessageBody(t, wagerInput(t, wallet, domain.WagerKindBet, "25", "bet-1"), "message-1"), wallet.ID().String())
	t.Setenv("AWS_ACCESS_KEY_ID", "test")
	t.Setenv("AWS_SECRET_ACCESS_KEY", "test")
	t.Setenv("AWS_SESSION_TOKEN", "")
	dsn, err := url.Parse(os.Getenv("TEST_DATABASE_URL"))
	if err != nil {
		t.Fatal(err)
	}
	query := dsn.Query()
	query.Set("search_path", pool.Config().ConnConfig.RuntimeParams["search_path"])
	dsn.RawQuery = query.Encode()
	var workerPool *pgxpool.Pool
	app := fx.New(auth.Module, Module, application.Module, messaging.Module, messaging.ConsumerModule, workers.Module, workers.WagerModule, fx.Provide(observability.NewMetrics),
		fx.Replace(oidcTestConfig(t), Config{DatabaseURL: dsn.String()}, messaging.Config{Region: "us-east-1", Endpoint: os.Getenv("TEST_SQS_ENDPOINT"), QueueURL: eventsQueue},
			messaging.ConsumerConfig{QueueURL: queue, ProviderBySender: map[string]string{"000000000000": "provider-a"}, ProcessingTimeout: 5 * time.Second}),
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
	for {
		var completed, published int
		if err := pool.QueryRow(ctx, `SELECT (SELECT count(*) FROM inbox WHERE completed_at IS NOT NULL),(SELECT count(*) FROM outbox WHERE published_at IS NOT NULL)`).Scan(&completed, &published); err != nil {
			t.Fatal(err)
		}
		if completed == 1 && published == 4 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if err := app.Stop(ctx); err != nil {
		t.Fatal(err)
	}
	if workerPool.Ping(ctx) == nil {
		t.Fatal("Fx did not close its pool")
	}
	assertWalletLedger(t, ctx, pool, wallet.ID(), 7500)
}
