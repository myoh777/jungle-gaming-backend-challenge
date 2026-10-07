# Políticas IAM para SQS (modelos)

> **Status:** modelos não aplicados nem testados. O repositório não tem infraestrutura de produção (Terraform, CloudFormation ou Kubernetes). O LocalStack não aplica nem valida IAM, então nada aqui foi verificado localmente. Valide em uma conta AWS de teste antes de usar, por exemplo com `aws accessanalyzer validate-policy`.

## Papel destas políticas

A entrada SQS tem duas camadas:

1. **Verificação da assinatura pelo serviço (testada).** O gateway interno confiável deve autenticar o provedor e assinar cada mensagem com HMAC-SHA-256, usando a chave daquele provedor. O consumidor recalcula a assinatura com a chave do `providerId` declarado e manda para a DLQ, sem nenhum efeito, qualquer mensagem sem assinatura válida. O gateway de produção não faz parte deste repositório; `cmd/sqssign` é uma ferramenta local de referência. Detalhes no `ARCHITECTURE.md` (seção "SQS e inbox").
2. **IAM do broker (estes modelos, não testados).** Privilégio mínimo: só a role do gateway publica na fila de entrada, e o serviço só tem as permissões que usa.

As duas camadas se complementam. A assinatura impede que alguém sem a chave de um provedor aja em nome dele, mesmo que consiga publicar na fila. O IAM reduz quem consegue publicar. Os provedores externos não recebem permissão de publicar nem as chaves de assinatura.

## Arquivos

| Arquivo | Tipo | Onde aplicar |
|---|---|---|
| `wager-transactions-queue-policy.json` | política de recurso (atributo `Policy` da fila) | `wager-transactions.fifo` |
| `wallet-service-iam-policy.json` | política de identidade | role do serviço (`<WALLET_SERVICE_ROLE>`) |

Substitua `<ACCOUNT_ID>`, `<REGION>`, `<GATEWAY_ROLE>` e `<WALLET_SERVICE_ROLE>`.

A política da fila:
- permite `SendMessage` só à role do gateway;
- permite ao serviço apenas consumir (`GetQueueUrl`, `GetQueueAttributes`, `ReceiveMessage`, `DeleteMessage`, `ChangeMessageVisibility`);
- **nega** `SendMessage` a qualquer outro principal, inclusive os que tenham `sqs:*` na própria política de identidade;
- nega acesso sem TLS.

A política do serviço reflete as chamadas que o código faz: consumo e readiness (`GetQueueAttributes`) na fila de entrada; `GetQueueUrl` e `SendMessage` na DLQ e na fila de eventos. O serviço não recebe `SendMessage` na fila de entrada.

## Chaves de assinatura

- O serviço lê as chaves de `SQS_PROVIDER_SIGNING_KEYS` (JSON `{"providerId": "chave"}`) ou de um arquivo indicado em `SQS_PROVIDER_SIGNING_KEYS_FILE`. Configure só uma das duas; cada chave precisa ter pelo menos 32 bytes.
- Em AWS, guarde o JSON em um cofre de segredos, como AWS Secrets Manager ou SSM Parameter Store `SecureString`. Injete-o no serviço e no gateway pelo mecanismo de segredos da plataforma (por exemplo, `secrets` em ECS ou um Secret montado em Kubernetes). Conceda `secretsmanager:GetSecretValue` (e `kms:Decrypt`, se houver chave KMS própria) apenas às roles do serviço e do gateway. Essas permissões não estão nos modelos acima.
- Gere chaves aleatórias, por exemplo com `openssl rand -base64 48`. A chave são os bytes da string, que não é decodificada.
- O arquivo `deploy/local/fake-sqs-signing-keys.json` só tem chaves fictícias para o ambiente local.

## Como aplicar (exemplo, não executado)

```bash
# política de recurso da fila de entrada (o valor de Policy é o JSON como string)
aws sqs set-queue-attributes \
  --queue-url https://sqs.<REGION>.amazonaws.com/<ACCOUNT_ID>/wager-transactions.fifo \
  --attributes "{\"Policy\": $(jq -c . wager-transactions-queue-policy.json | jq -Rs .)}"

# política de identidade do serviço
aws iam put-role-policy --role-name <WALLET_SERVICE_ROLE> \
  --policy-name wallet-service-sqs --policy-document file://wallet-service-iam-policy.json
```

## Limites

- Qualquer principal com `sqs:SetQueueAttributes` na fila pode trocar a política. Restrinja essa permissão a quem administra a infraestrutura.
- As políticas não cobrem a fila `wallet-events`. Consumidores de eventos que precisem confiar na origem devem restringir `SendMessage` nessa fila ao serviço, de forma análoga.
- A DLQ recebe mensagens do serviço (`SendMessage`) e do redrive do próprio SQS. Uma política de recurso restritiva na DLQ não foi incluída, porque sua interação com o redrive não foi verificada.
- O gateway tem as chaves de todos os provedores. Se ele for comprometido, a assinatura não protege; o IAM e o controle de acesso ao cofre de segredos continuam sendo necessários.
