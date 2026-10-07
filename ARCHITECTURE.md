# Arquitetura

Serviço Go que processa operações financeiras de provedores de jogos (BET, WIN, LOSS, REFUND, ROLLBACK) recebidas por HTTP e por SQS, mantendo saldo, ledger, idempotência persistente e eventos corretos com várias instâncias, duplicatas, concorrência, falhas e reinícios.

> **Estado da verificação (2026-10-07):** o código compila, `go vet` passa (com e sem `-tags integration`) e os testes unitários passam com `-race`. Os 28 testes de integração (PostgreSQL 17, Keycloak 26.0.7 e LocalStack 4.4 reais, via Docker Desktop/WSL2 no Windows 11) passam com e sem `-race`, sem data races, incluindo a verificação da assinatura do gateway na entrada SQS. A imagem do `Dockerfile`, a stack completa (`docker compose up --build`) e o perfil `multi` foram executados manualmente (item 9); depois da assinatura SQS, as três instâncias foram reconstruídas e a entrada SQS assinada foi verificada na stack em execução. Detalhes e limitações em [Itens incompletos](#itens-incompletos-e-riscos).

## Camadas e pacotes

```
cmd/server            main: carrega config, monta Fx, Run() (SIGINT/SIGTERM)
cmd/migrate           up | down [N]
cmd/sqssign           assina um envelope SQS como o gateway (uso local e referência)
migrations/           SQL versionado (NNNN_nome.up.sql / .down.sql), embutido no binário
internal/money        value object Money (int64 em centavos)          ┐
internal/domain/wallet    agregado Wallet + LedgerEntry                  │ domínio: sem HTTP, SQS,
internal/domain/wagering  WagerTransaction, máquina de estados,          │ PostgreSQL ou Fx
                          regras de liquidação, OPENING, eventos, hash  ┘
internal/app          casos de uso (WagerService, WalletService, ReferenceResolver) e portas Store/Tx
internal/postgres     implementação das portas com pgx + SQL explícito; migrator; repositório da outbox
internal/httpapi      handlers net/http, autenticação/autorização, health, métricas
internal/auth         verificação de JWT (OIDC/JWKS) do Keycloak
internal/sqsauth      assinatura HMAC-SHA-256 do gateway: forma canônica, verificação, chaves
internal/sqsconsumer  consumidor da fila FIFO (assinatura, inbox, retry, DLQ)
internal/outbox       publisher da outbox (lease, retry, recuperação)
internal/pendingref   worker de referências pendentes
internal/sqsx         cliente SQS, resolução de filas, sender de eventos
internal/config       variáveis de ambiente + validação
internal/observability logger JSON (slog) e métricas Prometheus
internal/fxapp        módulos Fx e ciclo de vida
test/integration      testes com infraestrutura real (build tag `integration`)
```

A dependência é unidirecional: `domain` ← `app` ← adaptadores (`postgres`, `httpapi`, `sqsconsumer`, `outbox`) ← `fxapp`. O domínio não importa nenhum pacote de infraestrutura nem `app`.

## Dinheiro

- `money.Money` é imutável: `int64` em **unidades mínimas (centavos)** e `Currency` ISO 4217. Não há `float32`/`float64` em nenhum ponto do caminho financeiro: parsing, domínio, SQL (`bigint`) ou JSON (strings).
- **Escala fixa de 2 casas.** As moedas aceitas (BRL, USD, EUR) têm 2 dígitos de unidade mínima, o que torna a escala fixa válida. Os cenários usam BRL; existe teste de incompatibilidade de moedas.
- **Limites:** `[-MaxInt64, +MaxInt64]` centavos, ou seja ±92.233.720.368.547.758,07. `math.MinInt64` é rejeitado, então a negação nunca estoura. `Add`, `Sub`, `Negate` e o parsing detectam overflow e retornam `ErrOverflow`.
- **Parsing externo** (`ParseNonNegative`): aceita `"25"`, `"25.5"` e `"25.50"`, sempre normalizados para 2 casas. Rejeita vazio, sinal (`-`/`+`), `NaN`, `Infinity`, notação científica, espaços, vírgula, zeros à esquerda (`"007"`), `".5"`, `"5."` e mais de 2 casas (`ErrScaleExceeded`, sem arredondamento). No JSON o valor é obrigatoriamente uma *string*: um número JSON falha na decodificação.
- Valores negativos existem apenas internamente (`Parse`, diferenças de reconciliação). O saldo da carteira nunca é negativo: o agregado impede, e o `CHECK (balance_amount >= 0)` impede no banco.
- **Mapeamento no PostgreSQL:** `bigint` (centavos) + `char(3)` (moeda), em `wallets`, `wager_transactions` e `wallet_ledger_entries`.

## Domínio

### Wallet
- Contém `id`, `playerId`, `currency`, `balance`, `version` e timestamps. `New` cria com saldo 0 e versão 1. `NewWithOpeningBalance` cria já com o saldo de abertura e **versão 1**: abertura e criação são um único evento. `Rehydrate` só valida (saldo ≥ 0, versão ≥ 1), sem aplicar efeitos.
- `Debit`/`Credit` são a única forma de mudar o saldo. Cada um retorna a `LedgerEntry` correspondente e incrementa a versão. Débito que deixaria saldo negativo retorna `ErrInsufficientFunds` sem alterar nada.
- `NewLedgerEntry` valida `balanceAfter = balanceBefore ± amount` conforme a direção, `amount > 0`, moedas iguais e saldos ≥ 0.

### WagerTransaction e máquina de estados

```
PENDING ──► PROCESSED | REJECTED | FAILED | PENDING_REFERENCE
PENDING_REFERENCE ──► PENDING_REFERENCE (retry) | PROCESSED | REJECTED | FAILED
PROCESSED, REJECTED, FAILED: terminais
```

- `NewExternal` cria em `PENDING`, após validar a política de zeros e a estrutura. `Rehydrate` reconstrói sem transição, efeito ou evento. As transições são métodos não exportados, acionados por `Settle` e por `OpenWallet`. `MarkFailed` é exportado para o worker.
- **PENDING nunca é commitado sozinho.** Operações sem dependência são decididas e gravadas já no estado final dentro da mesma transação SQL. Não existe aceite assíncrono; por isso não há "PENDING órfão" a retomar. O único estado não terminal persistido é `PENDING_REFERENCE`, retomado de forma durável (ver Referências).
- **FAILED** é usado pelo worker de referências quando a liquidação falha com erro **não transitório** (por exemplo, violação de invariante no banco). A transação é marcada `FAILED/INTERNAL_PERMANENT_ERROR` para auditoria, em vez de ser retentada indefinidamente. No caminho síncrono, uma falha de infraestrutura faz *rollback* e nada é gravado (HTTP 503/500; no SQS a mensagem volta à fila).
- **Falha transitória:** perda de conexão, timeout, `40001` serialization, `40P01` deadlock, `55P03` lock timeout, conflito de versão. São retentadas internamente (até 3 vezes) e depois expostas como `ErrUnavailable` (HTTP 503 / retry no SQS).
- **Falha permanente:** entrada inválida, conflito de idempotência, carteira inexistente, hash divergente na inbox. Não são retentadas (HTTP 4xx / DLQ).
- **OPENING:** `origin = INTERNAL`, sem provedor, chave de idempotência, hash, rodada, jogo ou referência. O schema impõe esse formato (`wager_transactions_origin_shape`) e no máximo um OPENING por carteira (índice único parcial).

### Regras (`wagering.Settle`)
| Tipo | Valor | Movimento | Referência |
|---|---|---|---|
| BET | > 0 | débito; saldo insuficiente → `INSUFFICIENT_FUNDS` | não permitida |
| WIN | > 0 | crédito | opcional; deve ser BET da mesma rodada |
| LOSS | = 0.00 | nenhum (sem ledger, sem mudança de versão, só `WagerTransactionProcessed`) | não permitida |
| REFUND | > 0, = valor da referência | crédito | obrigatória; deve ser BET |
| ROLLBACK | > 0, = valor da referência | inverso da referência (BET → crédito; WIN/REFUND → débito); saldo insuficiente → `REVERSAL_INSUFFICIENT_FUNDS` | obrigatória; BET, WIN ou REFUND |

A operação e a referência precisam concordar em provedor (a referência é resolvida por `(providerId, referenceExternalTransactionId)`), jogador, carteira, moeda e rodada; caso contrário → `REFERENCE_MISMATCH`.

**Política de reversões:** cada transação referenciada admite **no máximo uma reversão bem-sucedida, contando REFUND e ROLLBACK juntos**. Assim, REFUND+REFUND, ROLLBACK+ROLLBACK e REFUND+ROLLBACK sobre a mesma aposta resultam em `REFERENCE_ALREADY_REVERSED`, e o mesmo débito nunca é devolvido duas vezes. Um ROLLBACK **do REFUND** (que tem outra referência) é permitido e debita de novo; depois dele a aposta continua sem poder ser reembolsada outra vez, porque já tem uma reversão PROCESSED. A regra é verificada sob o lock da carteira e garantida no banco pelo índice único parcial `wager_transactions_one_reversal_per_reference`.

### Códigos de falha (estáveis)
`INSUFFICIENT_FUNDS`, `REVERSAL_INSUFFICIENT_FUNDS`, `WALLET_PLAYER_MISMATCH` (não expõe o saldo da carteira alheia), `CURRENCY_MISMATCH`, `REFERENCE_NOT_FOUND`, `REFERENCE_NOT_RESOLVED` (a referência existe, mas continuou pendente até expirar), `REFERENCE_NOT_PROCESSED` (a referência terminou REJECTED/FAILED), `REFERENCE_MISMATCH`, `REFERENCE_KIND_NOT_ALLOWED`, `REVERSAL_AMOUNT_MISMATCH`, `REFERENCE_ALREADY_REVERSED`, `INTERNAL_PERMANENT_ERROR` (FAILED).

## Persistência, transações e concorrência

- **Biblioteca:** `pgx/v5` (`pgxpool`) com SQL explícito em `internal/postgres`, sem ORM. As migrations são próprias (`internal/postgres/migrate.go`): cada uma roda em uma transação e o conjunto usa `pg_advisory_lock` para serializar execuções concorrentes. A migração simultânea por várias instâncias não foi verificada (ver item 9 em [Itens incompletos](#itens-incompletos-e-riscos)).
- **Limite de transação:** cada caso de uso é **uma** transação `READ COMMITTED` (`Store.InTx`), com `SET LOCAL lock_timeout = '5s'`. No processamento de uma operação, dentro dessa transação:
  1. (SQS) `INSERT` na inbox com `ON CONFLICT DO NOTHING`;
  2. busca de idempotência por `(provider_id, idempotency_key)` e `(provider_id, external_transaction_id)`;
  3. `SELECT … FROM wallets WHERE id = $1 FOR NO KEY UPDATE`;
  4. leitura da referência e de reversões existentes;
  5. `wagering.Settle` (puro, em memória);
  6. `INSERT` da transação com `ON CONFLICT DO NOTHING`;
  7. `UPDATE wallets … WHERE id = $1 AND version = $expected`;
  8. `INSERT` no ledger;
  9. `INSERT` dos eventos na outbox;
  10. vínculo da inbox à transação.

  Tudo é commitado junto ou nada é.
- **Lost updates:** os escritores da mesma carteira são serializados pelo lock de linha. Como segunda barreira, o `UPDATE` exige a versão esperada; se nenhuma linha for afetada, `ErrConcurrentModification` dispara um retry. `FOR NO KEY UPDATE` (e não `FOR UPDATE`) evita deadlock com os `FOR KEY SHARE` tomados pelas FKs de `wager_transactions`/`wallet_ledger_entries`.
- **Sem lock global:** carteiras diferentes travam linhas diferentes e avançam em paralelo. A ordem de locks é sempre **carteira → transação** (também no worker), o que evita ciclos.
- **Invariantes no banco:** `UNIQUE(player_id, currency)`; `UNIQUE(provider_id, external_transaction_id)`; `UNIQUE(provider_id, idempotency_key)`; `CHECK balance_amount >= 0`; `CHECK` de formato INTERNAL/EXTERNAL; `CHECK` da política de zeros; `CHECK` de status↔failure_code; índice único de OPENING por carteira; índice único de reversão bem-sucedida por referência; no ledger, `UNIQUE(wallet_id, transaction_id)`, FKs compostas `(wallet_id, currency) → wallets` e `(transaction_id, wallet_id) → wager_transactions`, e `CHECK` da aritmética `balance_after = balance_before ± amount`; triggers que bloqueiam `UPDATE`, `DELETE` e `TRUNCATE` no ledger; trigger que torna imutável o `payload` da outbox.
- `ON CONFLICT DO NOTHING` sem alvo também absorve conflitos nos índices parciais. Se isso acontecer (situação que o lock da carteira já previne), a releitura não encontra vencedor e a transação inteira é retentada com `ErrConcurrentModification`. Nada é gravado silenciosamente.

## Idempotência

- **Persistente:** a própria linha de `wager_transactions` é o registro de idempotência (chave + `payload_hash`). Ela sobrevive a reinícios e é compartilhada por todas as instâncias. Não há cache em memória.
- **Hash:** SHA-256 (hex) do **JSON canônico** com chaves ordenadas (`encoding/json` ordena chaves de `map`). Campos: `providerId`, `externalTransactionId`, `playerId`, `walletId`, `roundId`, `gameId`, `kind`, `amount` (normalizado para 2 casas), `currency` e, se presente, `referenceExternalTransactionId`. Ficam de fora a chave de idempotência, o correlationId e os metadados de transporte. As strings não são normalizadas (sem trim nem mudança de caixa). HTTP e SQS usam a mesma função (`wagering.PayloadHash`) e, portanto, produzem o mesmo hash.
- **Regras:**
  - mesma chave e mesmo hash → resultado persistido, com `idempotentReplay=true` e o **saldo observado no processamento original** (`result_balance_amount`), mesmo que a carteira tenha mudado depois;
  - mesma chave e hash diferente → 409 `IDEMPOTENCY_KEY_REUSED`;
  - mesmo `(providerId, externalTransactionId)` com outra chave → 409 `DUPLICATE_EXTERNAL_TRANSACTION`, sem reaplicar. Se a linha encontrada pelo external id tiver a **mesma** chave, valem as duas regras anteriores: em READ COMMITTED, cada consulta usa um snapshot novo, então uma requisição concorrente com a mesma chave pode commitar entre a busca por chave e a busca por external id.
- A chave pertence ao namespace do provedor `(provider_id, idempotency_key)`. No HTTP, o provedor é imposto pelo token. No SQS, ele é vinculado pela assinatura do gateway, feita com a chave daquele provedor (ver [SQS e inbox](#sqs-e-inbox)). Assim, um provedor não consegue fazer replay de transações de outro.
- **Concorrência de duplicatas:** quando N requisições iguais chegam juntas, a primeira insere e as demais esperam pelo índice único. Depois do commit, `ON CONFLICT DO NOTHING` não insere e a releitura devolve o replay. HTTP e SQS cruzados seguem o mesmo caminho.

## Referências pendentes

- Quando a referência não existe, ou existe mas está `PENDING_REFERENCE`, a transação é gravada como `PENDING_REFERENCE`, com `reference_attempts = 1`, `next_reference_attempt_at = now + base` e o evento `WagerTransactionPendingReference`. HTTP responde **202**. No SQS a mensagem é removida, porque o tratamento foi concluído de forma durável.
- O worker (`pendingref` + `app.ReferenceResolver`) lista as transações vencidas, trava **carteira → transação**, relê o estado (se outra instância já avançou, ignora) e reaplica `Settle`.
- **Backoff exponencial:** `min(base · 2^(n−1), max)`. O estado fica no PostgreSQL, então sobrevive a reinícios e qualquer instância continua o trabalho.
- **Expiração:** ao atingir `REFERENCE_MAX_ATTEMPTS` tentativas (padrão 10) **ou** `REFERENCE_TTL` desde a criação (padrão 1h), a transação vai para `REJECTED` com `REFERENCE_NOT_FOUND` (ou `REFERENCE_NOT_RESOLVED` se a referência existia mas continuou pendente) e o evento `WagerTransactionRejected`.
- Referência `REJECTED`/`FAILED` → rejeição imediata com `REFERENCE_NOT_PROCESSED`.

## SQS e inbox

- **Filas** (`deploy/localstack/init-sqs.sh`):
  - `wager-transactions.fifo` (VisibilityTimeout 30s, RedrivePolicy `maxReceiveCount=5` → DLQ);
  - `wager-transactions-dlq.fifo` (retenção de 14 dias);
  - `wallet-events` (fila **padrão** de eventos de saída).
- **Envelope:** `{"messageId","type":"WagerTransactionRequested","occurredAt","data":{idempotencyKey, providerId, externalTransactionId, playerId, walletId, roundId, gameId, kind, money{amount,currency}, referenceExternalTransactionId?, correlationId?},"signature"}`. A decodificação é estrita: campos desconhecidos e valor numérico são rejeitados. `signature` é obrigatória (ver abaixo).
- **Produtores** devem usar `MessageGroupId = walletId` (ordem por carteira, paralelismo entre carteiras) e `MessageDeduplicationId = messageId`. **A correção não depende da deduplicação FIFO**: a inbox `(consumer_name, message_id)` deduplica na aplicação, e os testes reenviam a mesma mensagem com `MessageDeduplicationId` diferentes.
- A inbox é gravada **na mesma transação SQL** que saldo, ledger, transação e outbox. Em uma reentrega, a inbox é encontrada: se o hash for igual, a mensagem é confirmada como replay; se for diferente, `ErrInboxHashMismatch` → DLQ.
- **Remoção só após commit:** `Handle` processa e commita; só depois `Apply` executa `DeleteMessage`. Se o processo morre entre os dois, a mensagem volta após o visibility timeout e a inbox impede a reaplicação.
- **Ações:**
  - commit (processada, rejeitada por negócio, pendente ou replay) → delete;
  - erro permanente (envelope inválido, entrada inválida, OPENING, assinatura ausente ou inválida, carteira inexistente, conflito de idempotência, hash divergente) → `SendMessage` na DLQ seguido de delete;
  - erro transitório ou desconhecido → `ChangeMessageVisibility` com backoff `min(base·2^(receiveCount−1), max)`. Esgotado o `maxReceiveCount`, a redrive policy do SQS move a mensagem para a DLQ.
- **Shutdown:** o contexto de recebimento é cancelado (o long-poll para). A mensagem em andamento termina com um contexto próprio limitado por `CONSUMER_PROCESS_TIMEOUT` (menor que o visibility timeout). As mensagens já recebidas e ainda não iniciadas voltam com `VisibilityTimeout=0`. Se o prazo do Fx expira, a mensagem simplesmente reaparece e é deduplicada.
- **Modelo de confiança:** os provedores não publicam na fila. Um **gateway interno confiável** autentica o provedor e só então publica a operação, assinada com HMAC-SHA-256 usando a chave daquele provedor. As chaves (uma por provedor) são gerenciadas pelo serviço e pelo gateway e nunca são entregues aos provedores. O consumidor recalcula a assinatura com a chave do `providerId` declarado e só então chama `WagerService.Process`. Uma mensagem só é aceita se foi assinada com a chave do provedor em nome do qual ela age; ter permissão de publicar na fila não basta.
- **Assinatura (contrato v1):** `signature` é o HMAC-SHA-256, em hex minúsculo, de uma forma canônica dos campos de negócio. A forma canônica é a linha `wagering.sqs.v1` seguida de uma linha `<nome>:<tamanho em bytes>:<valor>` por campo, nesta ordem: `providerId`, `externalTransactionId`, `idempotencyKey`, `playerId`, `walletId`, `roundId`, `gameId`, `kind`, `amount` (normalizado para 2 casas, por exemplo `25` → `25.00`), `currency` e, só quando presente, `referenceExternalTransactionId`. Cada linha termina em `\n`, e o prefixo de tamanho evita ambiguidade. As strings não são normalizadas. `messageId`, `occurredAt` e `correlationId` ficam de fora: são metadados de transporte. Uma mensagem capturada e reenviada com outro `messageId` cai na idempotência pela `idempotencyKey`, que é assinada. A assinatura **não substitui** o hash de idempotência (`wagering.PayloadHash`), que continua igual e não inclui a chave de idempotência. A implementação de referência é `sqsauth.Canonical`/`sqsauth.Sign` (usada por `sqsconsumer.Sign` e `cmd/sqssign`). Vetor de teste (`internal/sqsauth/sqsauth_test.go`): com a chave fictícia `LOCAL-ONLY-FAKE-KEY-provider-alpha-do-not-use` e um REFUND `alpha`/`bet-1`/`bet-1-key`/`player-1`/`01a11822-22dc-72cd-be06-541e1384c17e`/`r1`/`g1`/`25.00 BRL`/ref `bet-0`, a assinatura é `f8e325440ac598a0d32b7d2638851a7e690b37fc2c01ae435872adbdea48d57f`.
- **Falha fechada:** assinatura ausente, malformada ou divergente, provedor sem chave, assinatura feita com a chave de outro provedor, ou conteúdo alterado depois de assinado → a mensagem vai para a DLQ antes do caso de uso, sem linha na inbox, transação, ledger, saldo ou evento. A comparação usa `hmac.Equal` (tempo constante), e um provedor sem chave também passa pelo cálculo de um MAC. Com `CONSUMER_ENABLED=true`, o serviço não sobe sem chaves configuradas. Cada rejeição incrementa `sqs_messages_unauthenticated_total` e gera um log `WARN` com o motivo, sem a assinatura nem a chave.
- **Chaves:** `SQS_PROVIDER_SIGNING_KEYS` (JSON `{"providerId": "chave"}`) ou `SQS_PROVIDER_SIGNING_KEYS_FILE` (arquivo com o mesmo JSON, por exemplo um segredo montado). Configure só uma das duas. Cada chave precisa ter pelo menos 32 bytes; a chave são os bytes da string, que não é decodificada. O tipo das chaves não aparece em `fmt`, `slog` nem JSON. Localmente, `deploy/local/fake-sqs-signing-keys.json` tem apenas chaves fictícias. Valores reais devem vir de um cofre de segredos (ver `deploy/aws/README.md`). Não há rotação com duas chaves ativas para o mesmo provedor: trocar a chave exige atualizar gateway e serviço juntos.
- **Validações no consumidor:** decodificação estrita do envelope (campos desconhecidos, dados após o JSON, valor numérico, `messageId` ausente ou com mais de 200 caracteres, `type` diferente de `WagerTransactionRequested`, `occurredAt` fora de RFC3339 → DLQ). Depois, a validação de entrada de `app.BuildRequest`: campos obrigatórios (até 200 caracteres), tipo, valor e moeda, política de zeros, regras de referência e `walletId` UUID; falha → DLQ. Em seguida, a **assinatura**; falha → DLQ. Só então a mensagem segue pelo mesmo `WagerService.Process` do HTTP: idempotência e hash da inbox, existência da carteira, jogador da carteira (`WALLET_PLAYER_MISMATCH`), moeda e regras de referência e saldo do `Settle`. Não há lista de provedores conhecidos: o provedor é autenticado pela chave, não por uma lista de nomes permitidos.
- **Credenciais/políticas (IAM):** o serviço usa a cadeia de credenciais da AWS (estáticas no LocalStack). As chamadas que ele faz são: na fila de entrada, `GetQueueUrl`, `GetQueueAttributes` (readiness), `ReceiveMessage`, `DeleteMessage` e `ChangeMessageVisibility`; na DLQ e na fila de eventos, `GetQueueUrl` e `SendMessage`. Os modelos em `deploy/aws/` foram escritos para aplicar privilégio mínimo: a política de recurso da fila de entrada permite `SendMessage` à role do gateway e nega aos demais, e a política de identidade do serviço não inclui `SendMessage` na fila de entrada. **Esses modelos não foram aplicados nem testados:** o repositório não tem infraestrutura de produção, e o LocalStack não aplica nem valida IAM (as filas locais são criadas sem o atributo `Policy`).

## Transactional outbox

- Os eventos são gravados em `outbox_events` (o `id` é o `eventId`; `payload` é o envelope JSON imutável, protegido por trigger) na mesma transação dos efeitos. Como o publisher só enxerga linhas commitadas, **nada é publicado antes do commit**.
- **Claim:** `UPDATE … WHERE id IN (SELECT … FOR UPDATE SKIP LOCKED) RETURNING`, que grava `lease_owner`/`lease_expires_at`. Vários publishers (no mesmo processo ou em processos diferentes) não pegam a mesma linha ao mesmo tempo.
- **Confirmação:** `published_at = now()` somente se `lease_owner` ainda for o dono. Em caso de falha no envio, `next_attempt_at` recebe backoff exponencial e o lease é liberado.
- **Recuperação:**
  - entre o commit e a publicação, o evento fica pendente até algum publisher rodar;
  - entre a publicação e a confirmação (crash ou perda de lease), o lease expira e o evento é **republicado com o mesmo `eventId`**. A entrega é *at-least-once*, e os consumidores devem deduplicar por `eventId` (enviado no corpo e no atributo `eventId`).
- **Envelope:** `eventId`, `eventType`, `aggregateId`, `correlationId`, `causationId?`, `occurredAt` (UTC RFC3339), `version`, `data`. O tipo e a versão vêm do tipo concreto do payload (`WagerTransactionProcessed`, `WagerTransactionRejected`, `WalletBalanceChanged`, `WagerTransactionPendingReference`). Valores monetários são strings decimais.
- **Ordem:** a ordem de publicação segue `seq`, mas não é garantida entre retries nem entre publishers. A fila de eventos é padrão.

## Autenticação e autorização

- **IdP externo:** Keycloak (realm `wagering`, importado de `deploy/keycloak/wagering-realm.json`). Os clientes usam `client_credentials`. O serviço só **valida** tokens: assinatura RS256 via JWKS, `iss`, `aud=wagering-api` e `exp` (biblioteca `go-oidc`). Não emite tokens e não guarda senhas.
- **Roles** de realm:
  - `wagering-provider` + claim `provider_id` (mapper fixo por cliente): só `POST /wagering/transactions` e consultas das próprias transações;
  - `wallet-internal`: carteiras, ledger, reconciliação e qualquer transação.
- **Isolamento:**
  - o `providerId` do corpo precisa ser igual ao do token (403 antes de qualquer efeito);
  - `GET /wagering/transactions/{id}` de outro provedor → 404, sem revelar existência;
  - `GET /providers/{p}/…` com `p` diferente do token → 403.
- **Health e métricas:** `/health/live`, `/health/ready` e `/metrics` são públicos.

## Fx e ciclo de vida

- **Módulos:** `observability`, `postgres`, `sqs`, `auth`, `app`, `http` e `workers`, com `fx.Provide`, `fx.Invoke` e `fx.Annotate` (`Store` como `app.Store`).
- **OnStart** valida as dependências: ping do PostgreSQL (e migração opcional), resolução de todas as filas e download do JWKS. Qualquer falha aborta o start. Em seguida o listener HTTP é aberto de forma síncrona e os workers são iniciados.
- **OnStop** roda na ordem reversa. Primeiro os workers param de buscar trabalho e aguardam o item em andamento. Depois o HTTP: readiness passa a responder 503 e `Shutdown` drena as requisições. Por último o pool é fechado, depois de todos os componentes que o usam. O prazo total é `SHUTDOWN_TIMEOUT` (`fx.StopTimeout`).
- Os workers registram em log quando iniciam e quando param.

## Reconciliação

`POST /wallets/{id}/reconciliation` lê, em uma transação `REPEATABLE READ READ ONLY` (visão consistente), o saldo armazenado e `SUM(créditos) − SUM(débitos)` do ledger (abertura incluída; `::bigint` falha em vez de estourar). A diferença é `armazenado − calculado`. Não altera nada. Uma divergência gera log `ERROR` e incrementa `reconciliation_divergences_total`.

## Observabilidade

- **Logs:** JSON (`slog`), com `instanceId`, `correlationId`, `messageId`, `transactionId`, `walletId` e `providerId` quando disponíveis. Não registram tokens, corpos ou payloads financeiros completos.
- **Métricas Prometheus** (`/metrics`):
  - `wager_transactions_total{source,status}`;
  - `wager_idempotent_replays_total`;
  - `wager_idempotency_conflicts_total`;
  - `db_concurrency_retries_total`;
  - `sqs_messages_total{result}`, `sqs_message_retries_total`, `sqs_messages_dead_lettered_total` e `sqs_messages_unauthenticated_total`;
  - `outbox_events_published_total`, `outbox_publish_failures_total` e `outbox_oldest_pending_age_seconds` (atraso da outbox);
  - `pending_reference_retries_total`;
  - `reconciliation_runs_total` e `reconciliation_divergences_total`;
  - `http_request_duration_seconds` e `wager_processing_duration_seconds`.

## Itens incompletos e riscos

1. **Testes de integração executados apenas localmente.** Passaram em Windows 11 + Docker Desktop (WSL2), com e sem `-race`, mas não há CI. A primeira execução revelou e corrigiu: (a) uma corrida de idempotência entre instâncias (409 `DUPLICATE_EXTERNAL_TRANSACTION` indevido para a mesma chave; ver [Idempotência](#idempotência)); (b) a query string mal codificada no ledger, antes ignorada em silêncio e agora 400; (c) SQL do teste de constraints com parâmetro de tipo ambíguo; (d) os testes agora usam `127.0.0.1` em vez de `localhost`, porque no Windows `::1` pode ser capturado por outro processo que aceita a conexão sem responder. Eles estão em `test/integration` e cobrem:
   - migrations up/down;
   - constraints e imutabilidade do ledger;
   - atomicidade da abertura;
   - idempotência;
   - 50 apostas paralelas iguais;
   - três processos com apostas de 80,00 em uma carteira de 100,00;
   - carteiras em paralelo;
   - inbox e reentrega;
   - crash entre commit e delete;
   - DLQ (permanente e por esgotamento);
   - HTTP × SQS cruzados;
   - dois publishers;
   - crash entre publicação e confirmação;
   - retry da outbox;
   - referências pendentes, expiração e reinício;
   - autenticação real (credencial ausente, inválida e expirada; isolamento; endpoints internos);
   - assinatura do gateway no SQS (ausente, malformada, de outro provedor, de provedor sem chave, conteúdo alterado → DLQ sem efeito);
   - start/stop do Fx.

   A disputa entre três processos é sensível a timing. Depois da correção, passou em 30 execuções seguidas, o que reduz a chance de outra corrida, mas não prova que ela não exista.
2. **Imagem do LocalStack:** está fixada em `localstack/localstack:4.4`. Versões recentes do LocalStack podem exigir `LOCALSTACK_AUTH_TOKEN`; o MiniStack é uma alternativa compatível com a API SQS, mas não foi testado aqui.
3. **Entrada SQS: assinatura implementada, IAM só em modelo.** A assinatura HMAC do gateway é verificada pelo serviço e coberta por testes unitários e de integração (LocalStack). As políticas IAM do broker existem apenas como modelos não aplicados em `deploy/aws/`; o LocalStack não as aplica nem valida. O gateway não faz parte deste repositório: `cmd/sqssign` apenas assina, sem autenticar o provedor. O gateway guarda as chaves de todos os provedores; se ele for comprometido, a assinatura não protege. Não há rotação de chave sem janela de troca coordenada.
4. **Ordem dos eventos** não é garantida entre retries e publishers. Os consumidores devem tolerar reordenação e deduplicar por `eventId`.
5. **Reconciliação** soma o ledger inteiro em SQL. Para carteiras muito grandes, seria preciso usar checkpoints.
6. **`/metrics` é público.** Em produção deveria ficar numa porta ou rede interna.
7. **`INTERNAL_PERMANENT_ERROR` (FAILED)** só é produzido pelo worker de referências. Não há caminho de reprocessamento manual de FAILED.
8. **Partidas dobradas** não foram implementadas (eram opcionais).
9. **Stack completa e perfil multi-instância: verificação manual, sem automação.** Executado em 2026-10-07, no mesmo ambiente. Os itens de build a perfil `multi` abaixo foram feitos antes da assinatura SQS; o item **Assinatura SQS na stack** foi feito depois.
   - **Build:** `docker compose build` gera a imagem distroless `nonroot` (~55 MB) com `server` e `migrate`.
   - **Stack completa:** `docker compose up -d --wait app`. O `migrate` aplicou a versão 1 e saiu com código 0; o `app` subiu sem reinícios, com `/health/ready` = `ready` (PostgreSQL e SQS `up`).
   - **Exemplos do README contra o container:** 401 sem token, 403 para provedor em endpoint interno, criação de carteira, BET, replay, REFUND, ledger, reconciliação consistente e consulta por external id. O app valida tokens com `iss=http://localhost:8081/...` buscando o JWKS em `keycloak:8080`.
   - **SQS:** a aposta enviada com `awslocal` foi processada (inbox gravada, saldo atualizado); a mensagem inválida foi para a DLQ; a outbox publicou todos os eventos em `wallet-events` (`WagerTransactionProcessed` e `WalletBalanceChanged`).
   - **Perfil `multi`:** `docker compose --profile multi up -d --build` subiu `app-2` (:8090) e `app-3` (:8091) ao lado do `app`. O `migrate` rodou de novo sem aplicar nada (`applied: []`). Em 5 rodadas, as mesmas duas apostas de 80,00 numa carteira de 100,00 foram enviadas em paralelo às três instâncias, com as mesmas chaves. As três responderam de forma idêntica: uma aposta `PROCESSED` e a outra `REJECTED/INSUFFICIENT_FUNDS`, com o mesmo `transactionId` e `idempotentReplay` só nas repetições. Em cada carteira: saldo 20,00, versão 2, um débito, duas transações e ledger igual ao saldo. Nenhum erro nos logs.
   - **Assinatura SQS na stack:** `docker compose --profile multi build app app-2 app-3` e `docker compose --profile multi up -d --no-deps --wait app app-2 app-3` (PostgreSQL, Keycloak e LocalStack não foram recriados nem reiniciados). As três instâncias subiram com `SQS_PROVIDER_SIGNING_KEYS_FILE` montado (chaves fictícias), consumidor ativo e `/health/ready` = `ready`. Uma BET assinada com `cmd/sqssign` foi processada (inbox gravada, saldo 100,00 → 90,00, eventos publicados). Três mensagens inválidas, na mesma carteira, foram para a DLQ, uma em cada instância: sem assinatura (`missing signature`), WIN assinado com valor trocado depois (`signature does not match`) e WIN assinado com a chave de `beta` declarando `alpha` (`signature does not match`). Nenhuma criou transação nem linha na inbox, e saldo, ledger e eventos ficaram iguais. `sqs_messages_unauthenticated_total` = 1 em cada instância. Nenhuma chave apareceu nos logs.
   - **Limites:** nesse ensaio a instância :8080 sempre recebeu as requisições primeiro, então as outras quase sempre responderam com replay já commitado. A disputa simultânea de verdade está coberta por `TestThreeProcessesCompetingBets` (binário no host). Continuam não verificados: migração concorrente de várias instâncias (o advisory lock só foi exercitado em sequência) e o roteiro "Simular falhas e recuperação" do README, que exige parar ou matar containers. No ambiente de verificação (Windows), os exemplos com `curl` precisaram de `127.0.0.1` no lugar de `localhost` (ver README, "Testes de integração").
10. **Falhas reais de infraestrutura** foram apenas simuladas. A falha transitória do SQS usa um processador falso, e a queda real do PostgreSQL (`/health/ready` em 503, HTTP 503, retomada) não foi exercitada. O encerramento gracioso foi testado com `Fx.Stop` no processo, não por SIGTERM nem `docker compose stop`; o teste multiprocesso encerra com SIGKILL.
