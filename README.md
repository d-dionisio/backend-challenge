# Processamento distribuído de apostas em Go

API HTTP e consumidor SQS para carteiras e apostas, com PostgreSQL como
autoridade financeira. HTTP e mensageria compartilham o mesmo caso de uso,
idempotência persistente, ledger imutável, inbox e outbox transacionais.

Veja [ARCHITECTURE.md](ARCHITECTURE.md) para decisões, garantias e limites.

## Executar com Docker

Requisitos: Docker Engine com containers Linux e Docker Compose v2. O build
usa Go 1.26.3; não é necessário instalar Go para executar os containers.

Na raiz do repositório, com as portas padrão livres:

```powershell
docker compose up -d --build --wait
docker compose ps -a
Invoke-RestMethod http://localhost:8080/health/ready
```

O Compose fornece valores locais padrão, sem exigir `.env`. Se precisar
personalizar portas, copie `.env.example` para `.env`. O Compose usa somente
as variáveis referenciadas no YAML; ele não repassa automaticamente todas
as variáveis desse arquivo ao container da aplicação.

| Serviço | Acesso pelo host | Função |
| --- | --- | --- |
| app | `http://localhost:8080` | API, consumidor SQS, outbox e retry de referências |
| PostgreSQL | `localhost:5432` | Banco `wagering`, usuário/senha `postgres` |
| LocalStack | `http://localhost:14566` | SQS FIFO, DLQ FIFO e fila de eventos standard |
| Keycloak | `http://localhost:18080` | Realm `wagering`, clientes locais importados |
| migrate | Sem porta | Aplica migrations e termina com código zero |

A aplicação aguarda migrations concluídas e dependências saudáveis. As filas
só ficam prontas após a configuração das policies. O issuer OIDC do Compose
é `http://keycloak:8080/realms/wagering`, inclusive nos tokens obtidos pela
porta publicada do Keycloak. `localhost` dentro de um container aponta para
esse próprio container.

```powershell
docker compose logs --tail 100 app migrate
docker compose stop
docker compose down
```

`down` preserva o volume do PostgreSQL; `down -v` apaga esse volume. Keycloak
e LocalStack não têm persistência configurada: ao recriar seus containers,
o realm é importado novamente e as filas são recriadas. As credenciais e o
modo `start-dev` destinam-se à execução local.

### Banco antigo ou portas ocupadas

Um banco cujas tabelas foram criadas manualmente pode falhar com
`relation "wallets" already exists` e deixar o histórico do migrate dirty.
Confira o schema antes de reparar a versão; não marque migrations como
aplicadas sem essa verificação. Para testar sem alterar o banco antigo,
use outro projeto e portas (PowerShell):

```powershell
$env:APP_PORT = '18081'
$env:POSTGRES_PORT = '15432'
$env:KEYCLOAK_PORT = '18082'
$env:LOCALSTACK_PORT = '14567'
docker compose -p wagering-validation up -d --build --wait
Invoke-RestMethod http://localhost:18081/health/ready
```

Use o mesmo `-p wagering-validation` nos comandos seguintes. Esses valores
valem para a sessão atual; ajuste as URLs dos exemplos abaixo ou execute-os
em outra sessão com as portas padrão.

## Exemplo HTTP completo

Execute em PowerShell. Dinheiro usa strings decimais, nunca números JSON.
O cliente `wallet-service` tem acesso interno; `provider-a` só pode operar
como `provider-a`. Os segredos abaixo são os do realm local versionado.

```powershell
$api = 'http://localhost:8080'
$tokenURL = 'http://localhost:18080/realms/wagering/protocol/openid-connect/token'
$internalToken = Invoke-RestMethod -Method Post -Uri $tokenURL -Body @{
  grant_type='client_credentials'; client_id='wallet-service'; client_secret='local-wallet-service-secret'
}
$internalHeaders = @{Authorization=('Bearer ' + $internalToken.access_token)}
$playerID = [guid]::NewGuid().ToString()
$walletBody = @{playerId=$playerID; initialBalance=@{amount='100.00';currency='BRL'}} | ConvertTo-Json
$wallet = Invoke-RestMethod -Method Post -Uri "$api/wallets" -Headers $internalHeaders -ContentType 'application/json' -Body $walletBody

$providerToken = Invoke-RestMethod -Method Post -Uri $tokenURL -Body @{
  grant_type='client_credentials'; client_id='provider-a'; client_secret='local-provider-a-secret'
}
$externalID = [guid]::NewGuid().ToString()
$providerHeaders = @{
  Authorization=('Bearer ' + $providerToken.access_token)
  'Idempotency-Key'=$externalID
  'X-Correlation-Id'=[guid]::NewGuid().ToString()
}
$betBody = @{
  providerId='provider-a'; externalTransactionId=$externalID
  playerId=$playerID; walletId=$wallet.id; roundId='round-1'; gameId='game-1'
  kind='BET'; money=@{amount='25.00';currency='BRL'}
} | ConvertTo-Json
$result = Invoke-RestMethod -Method Post -Uri "$api/wagering/transactions" -Headers $providerHeaders -ContentType 'application/json' -Body $betBody
$result
# Mesmo corpo e chave: replay, sem novo débito; saldo continua em 75.00.
Invoke-RestMethod -Method Post -Uri "$api/wagering/transactions" -Headers $providerHeaders -ContentType 'application/json' -Body $betBody
Invoke-RestMethod -Uri "$api/wallets/$($wallet.id)/ledger?limit=10" -Headers $internalHeaders
Invoke-RestMethod -Method Post -Uri "$api/wallets/$($wallet.id)/reconciliation" -Headers $internalHeaders
```

| Método e rota | Identidade | Resultado |
| --- | --- | --- |
| `POST /wallets` | Interna | Cria carteira; 201 |
| `GET /wallets/{walletId}` | Interna | Saldo e versão |
| `GET /wallets/{walletId}/ledger?limit=50&cursor=...` | Interna | Página do ledger; limite de 1 a 100 |
| `POST /wallets/{walletId}/reconciliation` | Interna | Compara saldo e ledger sem corrigir dados |
| `POST /wagering/transactions` | Provedor | Exige `Idempotency-Key` |
| `GET /wagering/transactions/{transactionId}` | Provedor | Consulta no escopo do provedor |
| `GET /providers/{providerId}/wagering/transactions/{externalTransactionId}` | Provedor | Consulta por identidade externa |
| `GET /health/live` | Pública | Processo HTTP ativo |
| `GET /health/ready` | Pública | PostgreSQL e SQS acessíveis |
| `GET /metrics` | Pública | Métricas em formato Prometheus |

Uma aposta processada retorna 200; pendência de referência, 202; rejeição
financeira, 422; estado FAILED, 500. Conflitos de idempotência retornam 409.
O resultado inclui `transactionId`, `status`, `idempotentReplay` e, quando
aplicáveis, `balance` e `failureCode`. Use o cursor devolvido pelo servidor
para buscar a próxima página do ledger.

Erros de entrada e infraestrutura usam `{"code":"CODIGO"}`; resultados
financeiros persistidos usam o corpo de transação, incluindo `failureCode`.

| HTTP | Código/corpo | Como tratar |
| --- | --- | --- |
| 400 | `INVALID_REQUEST`, `INVALID_LEDGER_PAGE`, `IDEMPOTENCY_KEY_REQUIRED`, `WALLET_IDENTITY_MISMATCH` | Corrigir a entrada |
| 401 | `UNAUTHENTICATED` | Obter credencial válida |
| 403 | `FORBIDDEN` | Usar a identidade autorizada |
| 404 | `NOT_FOUND` | Conferir o identificador e o escopo do provedor |
| 409 | `IDEMPOTENCY_CONFLICT`, `WALLET_ALREADY_EXISTS` | Não repetir com dados incompatíveis |
| 413 | `REQUEST_TOO_LARGE` | Reduzir o corpo para até 1 MiB |
| 415 | `UNSUPPORTED_MEDIA_TYPE` | Enviar Content-Type application/json |
| 422 | Resultado com `status=REJECTED` e `failureCode` | Resultado definitivo; replay não tenta novamente |
| 202 | Resultado com `status=PENDING_REFERENCE` | Consultar até resolução durável pelo worker |
| 503 | `DEPENDENCY_UNAVAILABLE`, `IDENTITY_PROVIDER_UNAVAILABLE`, `SHUTTING_DOWN` | Retry com backoff, preservando chave e conteúdo |
| 500 | `INTERNAL_ERROR`, ou resultado persistido FAILED | Investigar; consultar a operação antes de decidir retry |
| 500 | `INVALID_FINANCIAL_STATE` | Investigar inconsistência ou resultado fora dos limites monetários |

Os códigos de rejeição definitivos são:

| failureCode | Significado |
| --- | --- |
| `INSUFFICIENT_BALANCE` | BET sem saldo |
| `REVERSAL_INSUFFICIENT_BALANCE` | ROLLBACK de crédito sem saldo para debitar |
| `INVALID_REFERENCE` | Referência incompatível em identidade, tipo ou valor |
| `REFERENCE_UNSUCCESSFUL` | Referência terminou em rejeição ou falha |
| `ALREADY_REVERSED` | A origem já recebeu uma reversão bem-sucedida |
| `MONEY_OVERFLOW` | O movimento excederia o intervalo de Money |
| `REFERENCE_NOT_FOUND` | Limite de tentativas atingido sem referência processável |

Alterar uma operação rejeitada exige uma nova identidade de operação e uma
nova chave; reutilizar sua chave com outro conteúdo é conflito.

## Entrada SQS

Fila `wager-transactions.fifo`: `MessageGroupId` deve ser o UUID da carteira;
forneça `MessageDeduplicationId` ao publicar. A deduplicação financeira
persiste além da janela de deduplicação do SQS.

```json
{
  "messageId": "request-123",
  "type": "WagerTransactionRequested",
  "occurredAt": "2026-09-21T12:00:00Z",
  "correlationId": "flow-123",
  "data": {
    "providerId": "provider-a",
    "externalTransactionId": "bet-123",
    "idempotencyKey": "bet-123",
    "playerId": "11111111-1111-4111-8111-111111111111",
    "walletId": "22222222-2222-4222-8222-222222222222",
    "roundId": "round-1",
    "gameId": "game-1",
    "kind": "BET",
    "money": {"amount": "25.00", "currency": "BRL"}
  }
}
```

Substitua os UUIDs por uma carteira existente e seu jogador. O `SenderId`
do broker deve estar autorizado em `SQS_PROVIDER_BY_SENDER`; o campo JSON
`providerId` sozinho não concede autorização. A DLQ é
`wager-transactions-dlq.fifo`, com redrive após cinco recebimentos. Eventos
de saída vão para `wager-events` e podem ser duplicados; consumidores devem
deduplicar por `eventId`.

Cada recebimento reserva a mensagem por 30s. O processamento tem timeout
padrão de 5s, configurável até 20s, deixando margem para o ACK. JSON inválido,
identidade não autorizada e conflitos permanentes não geram movimento:
a mensagem permanece para redrive. Falhas transitórias usam atrasos de
1s, 2s, 4s etc., limitados a 60s e cinco recebimentos.

Todos os tipos de evento usam a mesma fila de saída standard. Consumidores
roteiam por `eventType` e `version=1`, persistem a deduplicação por `eventId`
e confirmam somente após seu próprio tratamento durável. O envelope contém
`eventId`, `eventType`, `aggregateId` (carteira), `correlationId`,
`causationId` opcional, `occurredAt` UTC e `data`. Os contratos concretos
estão em [events.go](internal/domain/events.go): processamento, rejeição,
referência pendente e mudança de saldo. `WalletBalanceChanged.data` inclui
walletId, transactionId, direction, money, balanceBefore, balanceAfter e
walletVersion. A fila standard não garante ordem; consumidores não devem
usar a ordem de chegada para substituir cegamente o saldo de uma projeção.

## Migrations: aplicação e reversão

O Compose aplica `up` antes da aplicação. Para executar explicitamente:

```powershell
docker compose run --rm migrate
```

As migrations usam SQL explícito e histórico do golang-migrate. A reversão
da migration 2 remove inbox e outbox: execute o exemplo abaixo somente em
um banco descartável de teste ou após um plano de preservação dos dados.
Pare a aplicação enquanto reverte e reaplica o schema:

```powershell
docker compose stop app
docker compose run --rm migrate -path /migrations -database 'postgres://postgres:postgres@postgres:5432/wagering?sslmode=disable' down 1
docker compose run --rm migrate
docker compose up -d app
```

O exemplo usa as credenciais padrão. Com projeto isolado, acrescente o mesmo
`-p` em todos os comandos. As verificações de down/up também são executadas
em schemas temporários pelos testes de migrations.

## Configuração e execução fora do container

[.env.example](.env.example) lista as variáveis. Go não carrega `.env`
automaticamente: exporte as variáveis no shell antes de `go run ./cmd/api`.
Para essa modalidade, inicie somente as dependências, configure
`KEYCLOAK_HOSTNAME=http://localhost:18080` antes de iniciar Keycloak, use
`OIDC_ISSUER_URL=http://localhost:18080/realms/wagering` na API e execute
`docker compose run --rm migrate`. Não use esse issuer localhost no serviço
`app` do Compose.

| Grupo | Variáveis e padrões relevantes |
| --- | --- |
| Banco/HTTP | `DATABASE_URL`, `HTTP_ADDRESS` (`:8080`) |
| OIDC | `OIDC_ISSUER_URL`, `OIDC_AUDIENCE`, `OIDC_CLIENT_ID`, `OIDC_CLIENT_SECRET` |
| SQS | `AWS_REGION`, credenciais AWS, `SQS_ENDPOINT`, URLs das duas filas, `SQS_PROVIDER_BY_SENDER`, `SQS_PROCESSING_TIMEOUT` (5s) |
| Referências | Poll 1s, timeout 5s, 10 tentativas, atraso inicial 1s e máximo 1m (`REFERENCE_*`) |
| Outbox | Poll 1s, timeout 5s, lease 30s, atraso inicial 1s e máximo 1m (`OUTBOX_*`) |

Para alterar os workers no Compose, acrescente suas variáveis em
`services.app.environment` ou em um override; hoje o serviço usa os padrões.

## Testes e evidências

Com Go 1.26.3 instalado, testes sem infraestrutura:

```powershell
go test ./... -timeout 120s
go test -race ./... -timeout 180s
go vet ./...
```

Para reproduzir o cenário financeiro com três processos sem Go no host,
após iniciar o projeto Compose padrão:

```powershell
docker build --target build -t wagering-tests .
docker run --rm --network backend-challenge_default -e TEST_DATABASE_URL=postgres://postgres:postgres@postgres:5432/wagering?sslmode=disable wagering-tests go test -tags=integration ./internal/infrastructure/postgres -run '^TestProcessWagerThreeIndependentProcessesCompeteForBalance$' -count=1 -timeout 60s
```

O nome da rede depende do projeto; com `-p wagering-validation`, use
`wagering-validation_default`. O teste cria um schema isolado e o remove ao
terminar. Três processos conectados são liberados por uma barreira e disputam
R$ 100,00 com apostas de R$ 40,00: duas aprovadas, uma rejeitada, saldo de
R$ 20,00, ledger e outbox consistentes.

A suíte integrada completa usa a tag `integration`, `TEST_DATABASE_URL`,
`TEST_SQS_ENDPOINT` e `TEST_OIDC_ISSUER_URL`. O issuer precisa ser acessível
do processo de teste e igual ao anunciado no discovery do Keycloak.
Os testes incluem duplicatas, reversões, referências fora de ordem,
falhas transacionais, recuperação de processos e concorrência HTTP/SQS.

Para executar a suíte completa com race detector em Linux, sem instalar Go
ou compilador C no host, use a imagem `wagering-tests` construída acima:

```powershell
docker run --rm --network backend-challenge_default -e TEST_DATABASE_URL=postgres://postgres:postgres@postgres:5432/wagering?sslmode=disable -e TEST_SQS_ENDPOINT=http://localstack:4566 -e TEST_OIDC_ISSUER_URL=http://keycloak:8080/realms/wagering wagering-tests sh -c 'apk add --no-cache gcc musl-dev && go test -race -tags=integration ./... -count=1 -timeout 300s && go vet -tags=integration ./...'
```

Para executar no host, exporte as três variáveis `TEST_*` com os endpoints
acessíveis e configure o hostname do Keycloak conforme a seção anterior:

```powershell
$env:TEST_DATABASE_URL = 'postgres://postgres:postgres@localhost:5432/wagering?sslmode=disable'
$env:TEST_SQS_ENDPOINT = 'http://localhost:14566'
$env:TEST_OIDC_ISSUER_URL = 'http://localhost:18080/realms/wagering'
go test -race -tags=integration ./... -count=1 -timeout 300s
```

`-race` no host requer CGO e compilador C compatível. O comando Docker instala
esse compilador apenas no container temporário de testes.

Seleções reproduzíveis após preparar as mesmas dependências:

```powershell
# Três processos, duas apostas de 80, 50 duplicatas, replays após reinício
# e progresso de carteiras independentes enquanto outra está bloqueada.
go test -race -tags=integration ./internal/infrastructure/postgres -run '^TestThreeProcesses' -count=1 -timeout 180s
# Queda após commit/antes do ACK e antes/depois da publicação na outbox.
go test -race -tags=integration ./internal/infrastructure/postgres -run 'TestSQSRecoveryAfterCommitBeforeDelete|TestOutboxRecoveryAfterPublisherProcessKilled' -count=1 -timeout 180s
# Pendências, expiração, disputa entre workers e encerramento.
go test -race -tags=integration ./internal/infrastructure/postgres -run 'TestReference|TestTwoReference|TestHTTPFullFx|TestSQSWorkerShutdown|TestOutboxShutdown' -count=1 -timeout 180s
```

As simulações encerram somente os processos filhos criados pelos próprios
testes; não é necessário matar manualmente a aplicação do Compose. O cenário
das duas apostas de 80 usa dois processos na mesma carteira e o terceiro em
outra. A bateria de 50 duplicatas distribui chamadas concorrentes entre três
processos. Novos trios repetem as operações para verificar persistência após
reinício. O teste de carteiras independentes mantém uma carteira bloqueada
até comprovar o commit das outras duas.

Validação registrada na etapa Docker: build, migrations 1 e 2, serviços
saudáveis, criação de carteira autenticada, publicação dos dois eventos de
abertura e teste com três processos passaram. Após a revisão, passaram também
`go test ./...` e `go test -race -tags=integration ./... -count=1 -timeout 300s`
contra PostgreSQL, Keycloak e LocalStack reais. As fixtures provisionam policies
das filas e redrive restrito, e fornecem as métricas nas composições Fx dos workers.

Na consolidação da documentação, o exemplo HTTP acima também foi executado
no projeto isolado, ajustando apenas as portas: aposta e replay retornaram
o mesmo transactionId e saldo de 75.00; ledger e reconciliação responderam.

Os registros históricos em `docs/` são ignorados pelo Git; os documentos
consolidados na raiz são a referência versionável da entrega.
