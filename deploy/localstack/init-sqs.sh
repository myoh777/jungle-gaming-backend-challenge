#!/bin/bash
# Provisions the SQS queues used by the service (runs when LocalStack is ready).
#
#   wager-transactions-dlq.fifo  dead-letter queue for wager operations
#   wager-transactions.fifo      inbound wager operations; redrive to the DLQ after 5 receives
#   wallet-events                outbound domain events published from the outbox (standard queue)
set -euo pipefail

VISIBILITY_TIMEOUT=30
MAX_RECEIVE_COUNT=5

awslocal sqs create-queue --queue-name wager-transactions-dlq.fifo \
  --attributes FifoQueue=true,ContentBasedDeduplication=false,MessageRetentionPeriod=1209600

DLQ_ARN=$(awslocal sqs get-queue-attributes \
  --queue-url "$(awslocal sqs get-queue-url --queue-name wager-transactions-dlq.fifo --output text --query QueueUrl)" \
  --attribute-names QueueArn --output text --query Attributes.QueueArn)

cat > /tmp/wager-queue-attributes.json <<EOF
{
  "FifoQueue": "true",
  "ContentBasedDeduplication": "false",
  "VisibilityTimeout": "${VISIBILITY_TIMEOUT}",
  "RedrivePolicy": "{\"deadLetterTargetArn\":\"${DLQ_ARN}\",\"maxReceiveCount\":\"${MAX_RECEIVE_COUNT}\"}"
}
EOF
awslocal sqs create-queue --queue-name wager-transactions.fifo --attributes file:///tmp/wager-queue-attributes.json

awslocal sqs create-queue --queue-name wallet-events

echo "SQS queues provisioned"
