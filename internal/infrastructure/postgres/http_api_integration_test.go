//go:build integration

package postgres

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/d-dionisio/backend-challenge/internal/application"
	"github.com/d-dionisio/backend-challenge/internal/domain"
	"github.com/d-dionisio/backend-challenge/internal/infrastructure/auth"
	"github.com/d-dionisio/backend-challenge/internal/infrastructure/httpapi"
	"github.com/d-dionisio/backend-challenge/internal/infrastructure/messaging"
	"github.com/d-dionisio/backend-challenge/internal/infrastructure/workers"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"go.uber.org/fx"
)

type httpFixture struct {
	pool, apiPool               *pgxpool.Pool
	app                         *fx.App
	address, queue, eventsQueue string
	client                      *sqs.Client
	consumer                    *messaging.SQSConsumer
}

func startHTTPAPI(t *testing.T, withWorkers bool) *httpFixture {
	t.Helper()
	f := &httpFixture{pool: storageTestPool(t)}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	f.client, f.queue, _ = wagerQueues(t, ctx)
	// A fila de saída pode ser removida pelo teste de readiness.
	events, err := f.client.CreateQueue(ctx, &sqs.CreateQueueInput{QueueName: aws.String("http-events-" + uuid.NewString())})
	if err != nil {
		t.Fatal(err)
	}
	eventsURL, err := url.Parse(aws.ToString(events.QueueUrl))
	if err != nil {
		t.Fatal(err)
	}
	endpoint, err := url.Parse(os.Getenv("TEST_SQS_ENDPOINT"))
	if err != nil {
		t.Fatal(err)
	}
	eventsURL.Scheme, eventsURL.Host = endpoint.Scheme, endpoint.Host
	f.eventsQueue = eventsURL.String()
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if f.eventsQueue != "" {
			if _, err := f.client.DeleteQueue(ctx, &sqs.DeleteQueueInput{QueueUrl: aws.String(f.eventsQueue)}); err != nil {
				t.Error(err)
			}
		}
	})
	t.Setenv("AWS_ACCESS_KEY_ID", "test")
	t.Setenv("AWS_SECRET_ACCESS_KEY", "test")
	t.Setenv("AWS_SESSION_TOKEN", "")
	dsn, err := url.Parse(os.Getenv("TEST_DATABASE_URL"))
	if err != nil {
		t.Fatal(err)
	}
	query := dsn.Query()
	query.Set("search_path", f.pool.Config().ConnConfig.RuntimeParams["search_path"])
	query.Set("application_name", "http-"+uuid.NewString())
	dsn.RawQuery = query.Encode()
	var server *httpapi.Server
	options := []fx.Option{auth.Module, Module, application.Module, messaging.Module, messaging.ConsumerModule,
		fx.Replace(oidcTestConfig(t), Config{DatabaseURL: dsn.String()}, httpapi.Config{Address: "127.0.0.1:0"},
			messaging.Config{Region: "us-east-1", Endpoint: os.Getenv("TEST_SQS_ENDPOINT"), QueueURL: f.eventsQueue},
			messaging.ConsumerConfig{QueueURL: f.queue, ProviderBySender: map[string]string{"000000000000": "provider-a"}, ProcessingTimeout: 5 * time.Second}),
		fx.Populate(&f.apiPool, &f.consumer, &server), fx.NopLogger}
	if withWorkers {
		options = append(options, workers.Module, workers.WagerModule)
	}
	options = append(options, httpapi.Module)
	f.app = fx.New(options...)
	if err := f.app.Start(ctx); err != nil {
		t.Fatal(err)
	}
	f.address = "http://" + server.Address()
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if err := f.app.Stop(ctx); err != nil {
			t.Error(err)
		}
	})
	return f
}

// Retorna erro em vez de usar Fatal: também pode executar numa goroutine.
func httpCall(ctx context.Context, method, address, token, key, body string) (int, string, error) {
	r, err := http.NewRequestWithContext(ctx, method, address, strings.NewReader(body))
	if err != nil {
		return 0, "", err
	}
	r.Header.Set("Content-Type", "application/json")
	r.Header.Set("X-Correlation-Id", "http-test")
	r.Header.Set("X-Provider-Id", "provider-b") // Não pode substituir o claim autenticado.
	if token != "" {
		r.Header.Set("Authorization", "Bearer "+token)
	}
	if key != "" {
		r.Header.Set("Idempotency-Key", key)
	}
	response, err := (&http.Client{Timeout: 15 * time.Second}).Do(r)
	if err != nil {
		return 0, "", err
	}
	defer response.Body.Close()
	result, err := io.ReadAll(response.Body)
	if err != nil {
		return 0, "", err
	}
	if response.Header.Get("X-Correlation-Id") != "http-test" {
		return 0, "", fmt.Errorf("lost correlation ID")
	}
	if response.StatusCode == 401 && response.Header.Get("WWW-Authenticate") == "" {
		return 0, "", fmt.Errorf("missing authentication challenge")
	}
	return response.StatusCode, string(result), nil
}

func expectHTTP(t *testing.T, method, address, token, key, body string, status int) string {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	actual, result, err := httpCall(ctx, method, address, token, key, body)
	if err != nil || actual != status {
		t.Fatalf("%s %s: status=%d, expected=%d, body=%s, err=%v", method, address, actual, status, result, err)
	}
	return result
}

func httpWagerBody(t *testing.T, input application.ProcessWagerInput) string {
	t.Helper()
	body, err := json.Marshal(struct {
		ProviderID string           `json:"providerId"`
		ExternalID string           `json:"externalTransactionId"`
		PlayerID   uuid.UUID        `json:"playerId"`
		WalletID   uuid.UUID        `json:"walletId"`
		RoundID    string           `json:"roundId"`
		GameID     string           `json:"gameId"`
		Kind       domain.WagerKind `json:"kind"`
		Money      domain.Money     `json:"money"`
		Reference  *string          `json:"referenceExternalTransactionId,omitempty"`
	}{input.ProviderID, input.ExternalTransactionID, input.PlayerID, input.WalletID, input.RoundID, input.GameID, input.Kind, input.Money, input.ReferenceExternalTransactionID})
	if err != nil {
		t.Fatal(err)
	}
	return string(body)
}

type httpWagerResult struct {
	TransactionID uuid.UUID                         `json:"transactionId"`
	Status        string                            `json:"status"`
	Balance       struct{ Amount, Currency string } `json:"balance"`
	FailureCode   string                            `json:"failureCode"`
	Replay        bool                              `json:"idempotentReplay"`
}

func decodeHTTPWager(t *testing.T, body string) httpWagerResult {
	t.Helper()
	var result httpWagerResult
	if err := json.Unmarshal([]byte(body), &result); err != nil {
		t.Fatal(err)
	}
	return result
}

func TestHTTPWalletsAndAuthorization(t *testing.T) {
	f := startHTTPAPI(t, false)
	internal, provider := keycloakToken(t, "wallet-service"), keycloakToken(t, "provider-a")
	player := uuid.New()
	body := fmt.Sprintf(`{"playerId":%q,"initialBalance":{"amount":"100.00","currency":"BRL"}}`, player)
	for _, token := range []string{"", "invalid", keycloakToken(t, "wrong-audience")} {
		expectHTTP(t, "POST", f.address+"/wallets", token, "", body, 401)
	}
	expired := keycloakToken(t, "provider-expiring")
	time.Sleep(3 * time.Second)
	expectHTTP(t, "POST", f.address+"/wagering/transactions", expired, "key", "{}", 401)
	expectHTTP(t, "POST", f.address+"/wallets", provider, "", body, 403)
	if countRows(t, f.pool, "wallets") != 0 {
		t.Fatal("unauthorized request created wallet")
	}
	created := expectHTTP(t, "POST", f.address+"/wallets", internal, "", body, 201)
	var wallet struct {
		ID, PlayerID uuid.UUID
		Balance      struct{ Amount, Currency string }
		Version      int64
	}
	if err := json.Unmarshal([]byte(created), &wallet); err != nil {
		t.Fatal(err)
	}
	if wallet.ID == uuid.Nil || wallet.PlayerID != player || wallet.Balance.Amount != "100.00" || wallet.Version != 1 {
		t.Fatal(created)
	}
	read := expectHTTP(t, "GET", f.address+"/wallets/"+wallet.ID.String(), internal, "", "", 200)
	if read != created {
		t.Fatal("GET wallet differs from opening")
	}
	expectHTTP(t, "POST", f.address+"/wallets", internal, "", body, 409)
	expectHTTP(t, "GET", f.address+"/wallets/"+wallet.ID.String(), provider, "", "", 403)
	expectHTTP(t, "GET", f.address+"/wallets/"+uuid.NewString(), internal, "", "", 404)
	expectHTTP(t, "GET", f.address+"/wallets/invalid", internal, "", "", 400)
	for _, invalid := range []string{"null", `{}`, strings.Replace(body, `"100.00"`, `100.00`, 1), strings.Replace(body, `"100.00"`, `"-1.00"`, 1)} {
		expectHTTP(t, "POST", f.address+"/wallets", internal, "", invalid, 400)
	}
	zero := fmt.Sprintf(`{"playerId":%q,"initialBalance":{"amount":"0.00","currency":"BRL"}}`, uuid.New())
	expectHTTP(t, "POST", f.address+"/wallets", internal, "", zero, 201)
	if countRows(t, f.pool, "wallets") != 2 || countRows(t, f.pool, "wager_transactions") != 1 || countRows(t, f.pool, "wallet_ledger_entries") != 1 || countRows(t, f.pool, "outbox") != 2 {
		t.Fatal("opening or denied requests produced incorrect records")
	}
}

func TestHTTPWagerContractsAndReplay(t *testing.T) {
	f := startHTTPAPI(t, false)
	ctx := context.Background()
	wallet := openWagerWallet(t, ctx, f.pool, "100")
	provider := keycloakToken(t, "provider-a")
	other := keycloakToken(t, "provider-b")
	input := wagerInput(t, wallet, domain.WagerKindBet, "25", "http-bet")
	input.IdempotencyKey = "key-chosen-by-client"
	body := httpWagerBody(t, input)
	path := f.address + "/wagering/transactions"
	expectHTTP(t, "POST", path, keycloakToken(t, "wallet-service"), input.IdempotencyKey, body, 403)
	expectHTTP(t, "POST", path, other, input.IdempotencyKey, body, 403)
	expectHTTP(t, "POST", path, provider, "", body, 400)
	for _, invalid := range []string{"null", strings.Replace(body, `"25.00"`, `25.00`, 1), strings.Replace(body, `"25.00"`, `"1e2"`, 1), strings.Replace(body, `"BET"`, `"OPENING"`, 1)} {
		expectHTTP(t, "POST", path, provider, input.IdempotencyKey, invalid, 400)
	}
	result := decodeHTTPWager(t, expectHTTP(t, "POST", path, provider, input.IdempotencyKey, body, 200))
	if result.Status != "PROCESSED" || result.Balance.Amount != "75.00" || result.Replay {
		t.Fatal(result)
	}
	win := wagerInput(t, wallet, domain.WagerKindWin, "10", "http-win")
	expectHTTP(t, "POST", path, provider, win.IdempotencyKey, httpWagerBody(t, win), 200)
	// Money normalizado e metadados de transporte não mudam o hash financeiro.
	replay := decodeHTTPWager(t, expectHTTP(t, "POST", path, provider, input.IdempotencyKey, strings.Replace(body, `"25.00"`, `"25"`, 1), 200))
	if !replay.Replay || replay.TransactionID != result.TransactionID || replay.Balance.Amount != "75.00" {
		t.Fatal(replay)
	}
	expectHTTP(t, "POST", path, provider, input.IdempotencyKey, strings.Replace(body, `"25.00"`, `"26.00"`, 1), 409)
	expectHTTP(t, "POST", path, provider, "another-key", body, 409)
	expectHTTP(t, "POST", path, other, input.IdempotencyKey, body, 403) // Inclusive no replay.
	idPath := path + "/" + result.TransactionID.String()
	byID := decodeHTTPWager(t, expectHTTP(t, "GET", idPath, provider, "", "", 200))
	if byID.Balance.Amount != "75.00" {
		t.Fatal(byID)
	}
	expectHTTP(t, "GET", idPath, other, "", "", 404)
	expectHTTP(t, "GET", f.address+"/providers/provider-a/wagering/transactions/http-bet", other, "", "", 403)
	byExternal := decodeHTTPWager(t, expectHTTP(t, "GET", f.address+"/providers/provider-a/wagering/transactions/http-bet", provider, "", "", 200))
	if byExternal.TransactionID != result.TransactionID {
		t.Fatal(byExternal)
	}
	expectHTTP(t, "GET", path+"/"+uuid.NewString(), provider, "", "", 404)
	expectHTTP(t, "GET", path+"/invalid", provider, "", "", 400)
	for _, scenario := range []struct {
		kind                  domain.WagerKind
		amount, id, reference string
		status                int
		state, failure        string
	}{
		{domain.WagerKindBet, "200", "rejected", "", 422, "REJECTED", "INSUFFICIENT_BALANCE"},
		{domain.WagerKindRefund, "25", "pending", "missing-bet", 202, "PENDING_REFERENCE", ""},
		{domain.WagerKindLoss, "0", "loss", "", 200, "PROCESSED", ""},
	} {
		operation := wagerInput(t, wallet, scenario.kind, scenario.amount, scenario.id)
		if scenario.reference != "" {
			operation.ReferenceExternalTransactionID = &scenario.reference
		}
		payload := httpWagerBody(t, operation)
		created := decodeHTTPWager(t, expectHTTP(t, "POST", path, provider, operation.IdempotencyKey, payload, scenario.status))
		read := decodeHTTPWager(t, expectHTTP(t, "GET", path+"/"+created.TransactionID.String(), provider, "", "", 200))
		if read.Status != scenario.state || read.FailureCode != scenario.failure {
			t.Fatal(read)
		}
		repeated := decodeHTTPWager(t, expectHTTP(t, "POST", path, provider, operation.IdempotencyKey, payload, scenario.status))
		if !repeated.Replay || repeated.TransactionID != created.TransactionID {
			t.Fatal(repeated)
		}
	}
	assertWalletLedger(t, ctx, f.pool, wallet.ID(), 8500)
	if countRows(t, f.pool, "wallet_ledger_entries") != 3 {
		t.Fatal("replay, rejection or LOSS created a ledger entry")
	}
	stored, err := NewWalletRepository(f.pool).FindByID(ctx, wallet.ID())
	if err != nil || stored.Version() != 3 {
		t.Fatal("non-financial operation changed version", err)
	}
	var key string
	if err := f.pool.QueryRow(ctx, `SELECT idempotency_key FROM wager_transactions WHERE id=$1`, result.TransactionID).Scan(&key); err != nil || key != input.IdempotencyKey {
		t.Fatal("header was not persisted", err)
	}
}

func TestHTTPAndSQSSameOperation(t *testing.T) {
	f := startHTTPAPI(t, false)
	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Second)
	defer cancel()
	wallet := openWagerWallet(t, ctx, f.pool, "100")
	input := wagerInput(t, wallet, domain.WagerKindBet, "25", "same-bet")
	body := httpWagerBody(t, input)
	token := keycloakToken(t, "provider-a")
	envelope := wagerMessageBody(t, input, "same-envelope")
	sendWagerMessage(t, ctx, f.client, f.queue, envelope, wallet.ID().String())
	message := receiveWagerMessage(t, ctx, f.consumer)
	start := make(chan struct{})
	results := make(chan error, 2)
	go func() { <-start; results <- f.consumer.Handle(ctx, message) }()
	go func() {
		<-start
		status, body, err := httpCall(ctx, "POST", f.address+"/wagering/transactions", token, input.IdempotencyKey, body)
		if err == nil && status != 200 {
			err = fmt.Errorf("HTTP status %d: %s", status, body)
		}
		results <- err
	}()
	close(start)
	for i := 0; i < 2; i++ {
		if err := <-results; err != nil {
			t.Fatal(err)
		}
	}
	// Nova entrega real, com outra deduplicação FIFO e o mesmo messageId da inbox.
	sendWagerMessage(t, ctx, f.client, f.queue, envelope, wallet.ID().String())
	if err := f.consumer.Handle(ctx, receiveWagerMessage(t, ctx, f.consumer)); err != nil {
		t.Fatal(err)
	}
	replay := decodeHTTPWager(t, expectHTTP(t, "POST", f.address+"/wagering/transactions", token, input.IdempotencyKey, body, 200))
	if !replay.Replay || replay.Balance.Amount != "75.00" {
		t.Fatal(replay)
	}
	assertWalletLedger(t, ctx, f.pool, wallet.ID(), 7500)
	if countRows(t, f.pool, "inbox") != 1 || countRows(t, f.pool, "wallet_ledger_entries") != 2 || countRows(t, f.pool, "outbox") != 4 {
		t.Fatal("HTTP/SQS duplicated financial effects")
	}
}

func TestHTTPReadiness(t *testing.T) {
	f := startHTTPAPI(t, false)
	expectHTTP(t, "GET", f.address+"/health/live", "", "", "", 200)
	expectHTTP(t, "GET", f.address+"/health/ready", "", "", "", 200)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if _, err := f.client.DeleteQueue(ctx, &sqs.DeleteQueueInput{QueueUrl: aws.String(f.eventsQueue)}); err != nil {
		t.Fatal(err)
	}
	f.eventsQueue = "" // Já removida; evita repetir a exclusão no cleanup.
	result := expectHTTP(t, "GET", f.address+"/health/ready", "", "", "", 503)
	if !strings.Contains(result, `"sqs":"unavailable"`) || !strings.Contains(result, `"postgres":"ok"`) {
		t.Fatal(result)
	}
	f.apiPool.Close()
	result = expectHTTP(t, "GET", f.address+"/health/ready", "", "", "", 503)
	if !strings.Contains(result, `"postgres":"unavailable"`) {
		t.Fatal(result)
	}
	expectHTTP(t, "GET", f.address+"/health/live", "", "", "", 200)
}

func TestHTTPTransientFailureAndRetry(t *testing.T) {
	f := startHTTPAPI(t, false)
	ctx := context.Background()
	wallet := openWagerWallet(t, ctx, f.pool, "100")
	input := wagerInput(t, wallet, domain.WagerKindBet, "25", "retry-bet")
	body := httpWagerBody(t, input)
	token := keycloakToken(t, "provider-a")
	// Falha SQL durante a gravação da outbox, antes do commit financeiro.
	if _, err := f.pool.Exec(ctx, `CREATE FUNCTION fail_http_event() RETURNS TRIGGER AS $$ BEGIN RAISE EXCEPTION 'test unavailable' USING ERRCODE='08006'; END; $$ LANGUAGE plpgsql;
		CREATE TRIGGER fail_http_event BEFORE INSERT ON outbox FOR EACH ROW EXECUTE FUNCTION fail_http_event();`); err != nil {
		t.Fatal(err)
	}
	result := expectHTTP(t, "POST", f.address+"/wagering/transactions", token, input.IdempotencyKey, body, 503)
	if !strings.Contains(result, `"code":"DEPENDENCY_UNAVAILABLE"`) {
		t.Fatal(result)
	}
	assertWalletLedger(t, ctx, f.pool, wallet.ID(), 10000)
	if countRows(t, f.pool, "wager_transactions") != 1 || countRows(t, f.pool, "outbox") != 2 {
		t.Fatal("transient failure committed partial work")
	}
	if _, err := f.pool.Exec(ctx, `DROP TRIGGER fail_http_event ON outbox`); err != nil {
		t.Fatal(err)
	}
	expectHTTP(t, "POST", f.address+"/wagering/transactions", token, input.IdempotencyKey, body, 200)
	assertWalletLedger(t, ctx, f.pool, wallet.ID(), 7500)
}

func TestHTTPAndSQSDistinctBetsCompeteForBalance(t *testing.T) {
	f := startHTTPAPI(t, false)
	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Second)
	defer cancel()
	wallet := openWagerWallet(t, ctx, f.pool, "100")
	httpBet := wagerInput(t, wallet, domain.WagerKindBet, "80", "http-80")
	sqsBet := wagerInput(t, wallet, domain.WagerKindBet, "80", "sqs-80")
	body := httpWagerBody(t, httpBet)
	token := keycloakToken(t, "provider-a")
	sendWagerMessage(t, ctx, f.client, f.queue, wagerMessageBody(t, sqsBet, "sqs-80"), wallet.ID().String())
	message := receiveWagerMessage(t, ctx, f.consumer)
	start, results := make(chan struct{}), make(chan error, 2)
	go func() { <-start; results <- f.consumer.Handle(ctx, message) }()
	go func() {
		<-start
		status, response, err := httpCall(ctx, "POST", f.address+"/wagering/transactions", token, httpBet.IdempotencyKey, body)
		if err == nil && status != 200 && status != 422 {
			err = fmt.Errorf("HTTP status %d: %s", status, response)
		}
		results <- err
	}()
	close(start)
	for i := 0; i < 2; i++ {
		if err := <-results; err != nil {
			t.Fatal(err)
		}
	}
	var processed, rejected, debits int
	if err := f.pool.QueryRow(ctx, `SELECT count(*) FILTER(WHERE status='PROCESSED'), count(*) FILTER(WHERE status='REJECTED' AND failure_code='INSUFFICIENT_BALANCE') FROM wager_transactions WHERE kind='BET'`).Scan(&processed, &rejected); err != nil {
		t.Fatal(err)
	}
	if err := f.pool.QueryRow(ctx, `SELECT count(*) FROM wallet_ledger_entries WHERE direction='DEBIT'`).Scan(&debits); err != nil {
		t.Fatal(err)
	}
	if processed != 1 || rejected != 1 || debits != 1 {
		t.Fatal(processed, rejected, debits)
	}
	for _, input := range []application.ProcessWagerInput{httpBet, sqsBet} {
		status, body, err := httpCall(ctx, "POST", f.address+"/wagering/transactions", token, input.IdempotencyKey, httpWagerBody(t, input))
		if err != nil || (status != 200 && status != 422) {
			t.Fatal(status, body, err)
		}
		if !decodeHTTPWager(t, body).Replay {
			t.Fatal("retry was not a replay")
		}
	}
	assertWalletLedger(t, ctx, f.pool, wallet.ID(), 2000)
	if countRows(t, f.pool, "wallet_ledger_entries") != 2 {
		t.Fatal("replay changed ledger")
	}
}

func TestHTTPFullFxAndGracefulShutdown(t *testing.T) {
	f := startHTTPAPI(t, true)
	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Second)
	defer cancel()
	wallet := openWagerWallet(t, ctx, f.pool, "100")
	input := wagerInput(t, wallet, domain.WagerKindBet, "25", "slow-bet")
	body := httpWagerBody(t, input)
	token := keycloakToken(t, "provider-a")
	if _, err := f.pool.Exec(ctx, `CREATE FUNCTION slow_http_wager() RETURNS TRIGGER AS $$ BEGIN PERFORM pg_sleep(1); RETURN NEW; END; $$ LANGUAGE plpgsql;
		CREATE TRIGGER slow_http_wager BEFORE INSERT ON wager_transactions FOR EACH ROW EXECUTE FUNCTION slow_http_wager();`); err != nil {
		t.Fatal(err)
	}
	result := make(chan error, 1)
	go func() {
		status, body, err := httpCall(ctx, "POST", f.address+"/wagering/transactions", token, input.IdempotencyKey, body)
		if err == nil && status != 200 {
			err = fmt.Errorf("HTTP status %d: %s", status, body)
		}
		result <- err
	}()
	deadline := time.Now().Add(5 * time.Second)
	for {
		var busy bool
		if err := f.pool.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE application_name=$1 AND wait_event='PgSleep')`, f.apiPool.Config().ConnConfig.RuntimeParams["application_name"]).Scan(&busy); err != nil {
			t.Fatal(err)
		}
		if busy {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("request did not start its SQL transaction")
		}
		time.Sleep(10 * time.Millisecond)
	}
	if err := f.app.Stop(ctx); err != nil {
		t.Fatal(err)
	}
	if err := <-result; err != nil {
		t.Fatal(err)
	}
	if f.apiPool.Ping(ctx) == nil {
		t.Fatal("pool was not closed after HTTP shutdown")
	}
	assertWalletLedger(t, ctx, f.pool, wallet.ID(), 7500)
	if countRows(t, f.pool, "wallet_ledger_entries") != 2 || countRows(t, f.pool, "outbox") != 4 {
		t.Fatal("shutdown lost committed work")
	}
	if _, _, err := httpCall(ctx, "GET", f.address+"/health/live", "", "", ""); err == nil {
		t.Fatal("HTTP accepted request after shutdown")
	}
}
