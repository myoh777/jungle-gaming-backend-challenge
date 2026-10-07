# Wagering Wallet Service

Serviço em Go (Uber Fx + PostgreSQL + SQS + Keycloak) que processa operações de provedores de jogos (BET, WIN, LOSS, REFUND e ROLLBACK) por HTTP e SQS. Mantém saldo, ledger auditável, idempotência persistente, inbox e transactional outbox.

As decisões de projeto estão em [ARCHITECTURE.md](ARCHITECTURE.md), incluindo o estado de verificação e os itens incompletos.

## Pré-requisitos

- Go **1.27** (declarado em `go.mod` e no `Dockerfile`).
- Docker com Docker Compose v2. No Windows: Docker Desktop com **WSL2** (`wsl --install` como administrador, depois reiniciar).
- Para `go test -race` no Windows: um compilador C no `PATH` (por exemplo, `winget install BrechtSanders.WinLibs.POSIX.UCRT`) e `CGO_ENABLED=1`.
- `make` é opcional; todos os alvos do `Makefile` têm o comando equivalente abaixo.

## Subir tudo

```bash
docker compose up --build
```

O Compose sobe:

| Serviço | Porta | Função |
|---|---|---|
| `postgres` | 5432 | banco `wagering` (app) e `wagering_test` (testes) |
| `keycloak` | 8081 | IdP OIDC; realm `wagering` importado de `deploy/keycloak/wagering-realm.json` (admin/admin) |
| `localstack` | 4566 | SQS; filas criadas por `deploy/localstack/init-sqs.sh` |
| `migrate` | – | `migrate up`, roda uma vez antes do app |
| `app` | 8080 | o serviço |

Para rodar **três instâncias** (8080, 8090 e 8091) sobre o mesmo banco, filas e IdP:

```bash
docker compose --profile multi up --build
```

No Windows, se `curl localhost:...` travar, use `127.0.0.1` (ver "Testes de integração").

Para parar, use `docker compose --profile multi stop` (mantém os dados) ou `docker compose --profile multi down` (remove os containers e mantém o volume).

> **Atenção:** `make down` executa `docker compose --profile multi down -v`. O `-v` **apaga o volume `pgdata`**, ou seja, todos os dados do PostgreSQL (`wagering` e `wagering_test`). Use-o só quando quiser recomeçar do zero.

Para rodar o serviço fora do Docker, suba só as dependências, aplique as migrations e inicie o servidor com as variáveis de `.env.example`:

```bash
docker compose up -d --wait postgres keycloak localstack
export $(grep -v '^#' .env.example | xargs)   # bash
go run ./cmd/migrate up
go run ./cmd/server
```

### Filas

| Fila | Tipo | Uso |
|---|---|---|
| `wager-transactions.fifo` | FIFO, visibility 30s, redrive `maxReceiveCount=5` | entrada de operações |
| `wager-transactions-dlq.fifo` | FIFO | mensagens inválidas, permanentes ou esgotadas |
| `wallet-events` | padrão | eventos publicados pela outbox (deduplicar por `eventId`) |

### Migrations

```bash
DATABASE_URL=postgres://wagering:wagering@localhost:5432/wagering?sslmode=disable go run ./cmd/migrate up
DATABASE_URL=postgres://wagering:wagering@localhost:5432/wagering?sslmode=disable go run ./cmd/migrate down 1
```

Os arquivos ficam em `migrations/NNNN_nome.{up,down}.sql`, embutidos no binário. Cada versão aplicada é registrada em `schema_migrations`, e execuções concorrentes são serializadas por advisory lock. No container, use `docker compose run --rm migrate` (up) ou `docker compose run --rm --entrypoint /app/migrate migrate down 1`.

## Variáveis de ambiente

Todas estão em [.env.example](.env.example). As principais:

| Variável | Descrição |
|---|---|
| `DATABASE_URL` | conexão PostgreSQL (obrigatória) |
| `AWS_ENDPOINT_URL`, `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_REGION` | SQS (LocalStack localmente) |
| `SQS_WAGER_QUEUE`, `SQS_WAGER_DLQ`, `SQS_EVENTS_QUEUE` | nomes das filas |
| `SQS_PROVIDER_SIGNING_KEYS` ou `SQS_PROVIDER_SIGNING_KEYS_FILE` | chaves HMAC (uma por provedor) para verificar a assinatura do gateway nas mensagens SQS; JSON `{"providerId": "chave"}` com pelo menos 32 bytes por chave; obrigatória com `CONSUMER_ENABLED=true` (ver "Enviar pelo SQS") |
| `OIDC_ISSUER` | `iss` esperado (`http://localhost:8081/realms/wagering`) |
| `OIDC_JWKS_URL` | onde buscar as chaves (no Compose: rede interna `keycloak:8080`) |
| `OIDC_AUDIENCE` | `aud` esperado (`wagering-api`) |
| `CONSUMER_ENABLED`, `PUBLISHER_ENABLED`, `REFERENCE_WORKER_ENABLED` | liga/desliga cada worker |
| `REFERENCE_MAX_ATTEMPTS`, `REFERENCE_TTL`, `REFERENCE_RETRY_BASE/MAX` | política de referências pendentes |
| `SHUTDOWN_TIMEOUT` | prazo do encerramento gracioso |

## Autenticação (Keycloak)

Os clientes locais são criados pelo import do realm. Os segredos abaixo valem **apenas localmente**:

| client_id | segredo | role | provider_id |
|---|---|---|---|
| `provider-alpha` | `provider-alpha-local-secret` | `wagering-provider` | `alpha` |
| `provider-beta` | `provider-beta-local-secret` | `wagering-provider` | `beta` |
| `provider-alpha-short` | `provider-alpha-short-local-secret` | `wagering-provider` (token de 3s) | `alpha` |
| `wallet-admin` | `wallet-admin-local-secret` | `wallet-internal` | – |
| `no-role-client` | `no-role-client-local-secret` | nenhuma | – |

```bash
token() {
  curl -s -d grant_type=client_credentials -d client_id=$1 -d client_secret=$2 \
    http://localhost:8081/realms/wagering/protocol/openid-connect/token | sed -E 's/.*"access_token":"([^"]+)".*/\1/'
}
ADMIN=$(token wallet-admin wallet-admin-local-secret)
ALPHA=$(token provider-alpha provider-alpha-local-secret)
```

## API

Todos os endpoints de negócio exigem `Authorization: Bearer <token>`. Os erros seguem o formato `{"error":{"code","message","field?"}}`.

| Método e rota | Quem pode | Respostas |
|---|---|---|
| `POST /wallets` | interno | 201, 400, 401, 403, 409 `WALLET_ALREADY_EXISTS` |
| `GET /wallets/{walletId}` | interno | 200, 404 |
| `GET /wallets/{walletId}/ledger?cursor=&limit=50` | interno | 200 `{entries, nextCursor?}` (limite 1..200, ordem estável por sequência; cursor opaco), 400 (limite fora da faixa, cursor ou query string inválidos), 404 |
| `POST /wallets/{walletId}/reconciliation` | interno | 200 `{walletId, storedBalance, calculatedBalance, difference, consistent, checkedEntries}` |
| `POST /wagering/transactions` | provedor (o `providerId` do corpo tem de ser o do token) | ver abaixo |
| `GET /wagering/transactions/{transactionId}` | provedor dono ou interno | 200, 404 (inclusive para transação de outro provedor) |
| `GET /providers/{providerId}/wagering/transactions/{externalTransactionId}` | provedor `providerId` ou interno | 200, 403, 404 |
| `GET /health/live`, `GET /health/ready`, `GET /metrics` | público | readiness verifica PostgreSQL e SQS |

Respostas de `POST /wagering/transactions`:

| HTTP | Significado |
|---|---|
| 200 | `PROCESSED` (nova ou replay) |
| 202 | `PENDING_REFERENCE` |
| 422 | `REJECTED` + `failureCode` (persistida e replayável) |
| 400 | `INVALID_INPUT` (nada persistido) |
| 401 / 403 | não autenticado / não autorizado (nada persistido) |
| 404 | `WALLET_NOT_FOUND` |
| 409 | `IDEMPOTENCY_KEY_REUSED` ou `DUPLICATE_EXTERNAL_TRANSACTION` |
| 503 | `TEMPORARILY_UNAVAILABLE` (repetir com a mesma `Idempotency-Key`) |

Corpo de resposta (todas as consultas de transação):

```json
{
  "transactionId": "…", "origin": "EXTERNAL", "providerId": "alpha", "externalTransactionId": "bet-1",
  "walletId": "…", "playerId": "player-1", "roundId": "r1", "gameId": "g1", "kind": "BET",
  "money": {"amount": "25.00", "currency": "BRL"},
  "referenceExternalTransactionId": "…", "referenceTransactionId": "…",
  "status": "PROCESSED", "failureCode": "…",
  "balanceAfter": {"amount": "75.00", "currency": "BRL"}, "walletVersion": 2,
  "referenceAttempts": 1, "nextReferenceAttemptAt": "…",
  "idempotentReplay": false, "createdAt": "…", "updatedAt": "…"
}
```

### Exemplos

```bash
# criar carteira com 100.00 (cria OPENING + crédito no ledger + eventos)
WALLET=$(curl -s -X POST localhost:8080/wallets -H "Authorization: Bearer $ADMIN" -H 'Content-Type: application/json' \
  -d '{"playerId":"player-1","initialBalance":{"amount":"100.00","currency":"BRL"}}' | sed -E 's/.*"walletId":"([^"]+)".*/\1/')

# aposta
curl -s -X POST localhost:8080/wagering/transactions -H "Authorization: Bearer $ALPHA" \
  -H 'Idempotency-Key: bet-1-key' -H 'Content-Type: application/json' \
  -d "{\"providerId\":\"alpha\",\"externalTransactionId\":\"bet-1\",\"playerId\":\"player-1\",\"walletId\":\"$WALLET\",
       \"roundId\":\"r1\",\"gameId\":\"g1\",\"kind\":\"BET\",\"money\":{\"amount\":\"25.00\",\"currency\":\"BRL\"}}"

# refund da aposta
curl -s -X POST localhost:8080/wagering/transactions -H "Authorization: Bearer $ALPHA" \
  -H 'Idempotency-Key: refund-1-key' -H 'Content-Type: application/json' \
  -d "{\"providerId\":\"alpha\",\"externalTransactionId\":\"refund-1\",\"playerId\":\"player-1\",\"walletId\":\"$WALLET\",
       \"roundId\":\"r1\",\"gameId\":\"g1\",\"kind\":\"REFUND\",\"money\":{\"amount\":\"25.00\",\"currency\":\"BRL\"},
       \"referenceExternalTransactionId\":\"bet-1\"}"

curl -s localhost:8080/wallets/$WALLET/ledger?limit=50 -H "Authorization: Bearer $ADMIN"
curl -s -X POST localhost:8080/wallets/$WALLET/reconciliation -H "Authorization: Bearer $ADMIN"
curl -s localhost:8080/providers/alpha/wagering/transactions/bet-1 -H "Authorization: Bearer $ALPHA"
```

### Enviar pelo SQS

Os provedores não publicam na fila. Um **gateway interno confiável** autentica o provedor e publica a operação assinada com HMAC-SHA-256, usando a chave daquele provedor. As chaves são gerenciadas pelo serviço e pelo gateway e nunca vão para os provedores. O consumidor verifica a assinatura com a chave do `providerId` declarado antes de qualquer efeito. Mensagens sem assinatura válida vão para a DLQ sem movimentar nada. O contrato da assinatura (forma canônica e vetor de teste) está em [ARCHITECTURE.md](ARCHITECTURE.md#sqs-e-inbox).

Localmente, `cmd/sqssign` faz o papel do gateway: lê o envelope no stdin, assina com `SQS_SIGNING_KEY` e imprime o envelope com `signature`. A chave abaixo é a chave **fictícia** de `alpha`, em `deploy/local/fake-sqs-signing-keys.json`:

```bash
export SQS_SIGNING_KEY='LOCAL-ONLY-FAKE-KEY-provider-alpha-do-not-use'   # fictícia, só local
BODY=$(go run ./cmd/sqssign <<EOF
{"messageId":"msg-1","type":"WagerTransactionRequested","occurredAt":"2026-01-01T00:00:00Z",
 "data":{"idempotencyKey":"bet-2-key","providerId":"alpha","externalTransactionId":"bet-2",
 "playerId":"player-1","walletId":"$WALLET","roundId":"r1","gameId":"g1","kind":"BET",
 "money":{"amount":"10.00","currency":"BRL"}}}
EOF
)
docker compose exec localstack awslocal sqs send-message \
  --queue-url http://localhost:4566/000000000000/wager-transactions.fifo \
  --message-group-id "$WALLET" --message-deduplication-id msg-1 --message-body "$BODY"

# eventos publicados pela outbox
docker compose exec localstack awslocal sqs receive-message --queue-url http://localhost:4566/000000000000/wallet-events --max-number-of-messages 10
# DLQ (inclui mensagens sem assinatura válida)
docker compose exec localstack awslocal sqs receive-message --queue-url http://localhost:4566/000000000000/wager-transactions-dlq.fifo
```

Use `MessageGroupId = walletId` e `MessageDeduplicationId = messageId`. A deduplicação real é feita pela inbox da aplicação.

**Chaves reais:** nunca no repositório. Guarde o JSON de chaves em um cofre de segredos (por exemplo, AWS Secrets Manager) e entregue-o ao serviço e ao gateway como `SQS_PROVIDER_SIGNING_KEYS` ou como arquivo montado em `SQS_PROVIDER_SIGNING_KEYS_FILE`. Gere cada chave com, por exemplo, `openssl rand -base64 48`. Modelos de política IAM de privilégio mínimo (só o gateway publica na fila de entrada), **não aplicados nem testados**, estão em [deploy/aws/](deploy/aws/README.md). O LocalStack não aplica nem valida IAM.

## Testes

```bash
gofmt -l .                       # deve imprimir nada
go vet ./...
go vet -tags integration ./...
go test ./...                    # unitários (sem infraestrutura)
go test -race ./...              # Windows: CGO_ENABLED=1 e gcc no PATH
```

### Testes de integração

Usam PostgreSQL, Keycloak e LocalStack reais (build tag `integration`). Cada teste cria filas próprias e usa carteiras e jogadores únicos no banco `wagering_test`.

```bash
docker compose up -d --wait postgres keycloak localstack
go test -tags integration -count=1 -p 1 -timeout 15m ./test/integration/...
go test -race -tags integration -count=1 -p 1 -timeout 20m ./test/integration/...
```

Variáveis opcionais: `TEST_DATABASE_URL`, `TEST_ADMIN_DATABASE_URL`, `TEST_AWS_ENDPOINT_URL`, `TEST_KEYCLOAK_URL` (onde buscar tokens e chaves), `TEST_OIDC_ISSUER` (o `iss` esperado, padrão `http://localhost:8081/realms/wagering`, fixado por `KC_HOSTNAME`) e `TEST_LOG_LEVEL`. Os padrões usam `127.0.0.1` em vez de `localhost`, porque no Windows `localhost` pode resolver para `::1`, onde outro processo (por exemplo, um relay do WSL) aceita a conexão e não responde. Se `go run ./cmd/server` travar ao conectar com os valores de `.env.example`, troque `localhost` por `127.0.0.1` em `DATABASE_URL`, `AWS_ENDPOINT_URL` e `OIDC_JWKS_URL`, mas mantenha `OIDC_ISSUER`.

| Arquivo | Cobre |
|---|---|
| `schema_test.go` | migrations up/down/up; constraints; ledger imutável (UPDATE, DELETE, TRUNCATE); unicidade de abertura e de reversões |
| `wallet_test.go` | abertura atômica (carteira + OPENING + ledger + 2 eventos); saldo zero; duplicada → 409; paginação; reconciliação consistente e divergente |
| `wager_test.go` | fluxo BET/WIN/LOSS; replay com saldo original; conflitos de idempotência; política de zeros; reversões; 50 apostas iguais em paralelo; carteiras em paralelo |
| `multiprocess_test.go` | **3 processos do binário real**; duas apostas de 80,00 numa carteira de 100,00 enviadas a todos; reenvio |
| `auth_test.go` | credenciais ausentes, inválidas e expiradas; isolamento entre provedores; endpoints internos; ausência de efeitos |
| `sqs_test.go` | inbox com mensagens repetidas; crash entre commit e delete; DLQ; retries transitórios; esgotamento → DLQ; HTTP × SQS com a mesma chave; assinatura ausente, malformada, de outro provedor, de provedor sem chave ou com conteúdo alterado → DLQ sem efeito (o LocalStack não valida IAM) |
| `outbox_test.go` | commit sem publisher; 2 publishers concorrentes; crash entre publicação e confirmação (republicação com o mesmo `eventId`); retry com broker falhando |
| `references_test.go` | REFUND antes da BET; expiração; retomada por outra instância após reinício |
| `lifecycle_test.go` | start/stop do Fx, listener e pool liberados; start falha com IdP ou fila ausente |

## Simular falhas e recuperação

Este roteiro descreve o comportamento esperado. Só o cenário de várias instâncias foi executado; os demais não foram (ver os itens 9 e 10 de [Itens incompletos](ARCHITECTURE.md#itens-incompletos-e-riscos)).

- **Várias instâncias:** `docker compose --profile multi up --build` e envie a mesma `Idempotency-Key` para `:8080`, `:8090` e `:8091`.
- **Crash do consumidor/publisher:** `docker compose kill app` no meio do processamento e depois `docker compose up -d app`. As mensagens não confirmadas voltam após o visibility timeout e são deduplicadas pela inbox; os eventos com lease expirado são republicados com o mesmo `eventId`.
- **Banco indisponível:** `docker compose stop postgres` → `/health/ready` passa a 503, o HTTP responde 503 e as mensagens SQS são retentadas com backoff. `docker compose start postgres` normaliza.
- **Referência atrasada:** envie um REFUND antes da BET (resposta 202), reinicie o app (`docker compose restart app`) e envie a BET. O worker conclui o REFUND.
- **Encerramento gracioso:** `docker compose stop app` (SIGTERM). Os logs mostram os workers parando, o HTTP drenando e o pool fechando.
