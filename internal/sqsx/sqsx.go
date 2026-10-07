// Package sqsx builds the SQS client and resolves the queues used by the service.
package sqsx

import (
	"context"
	"fmt"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"

	"wagering/internal/config"
)

// NewClient builds an SQS client. With AWS_ENDPOINT_URL set it targets
// LocalStack/MiniStack; static credentials are used when provided, otherwise
// the default AWS credential chain (IAM role, profile...) applies.
func NewClient(ctx context.Context, cfg config.Config) (*sqs.Client, error) {
	opts := []func(*awsconfig.LoadOptions) error{awsconfig.WithRegion(cfg.AWSRegion)}
	if cfg.AWSAccessKeyID != "" {
		opts = append(opts, awsconfig.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider(cfg.AWSAccessKeyID, cfg.AWSSecretKey, "")))
	}
	awsCfg, err := awsconfig.LoadDefaultConfig(ctx, opts...)
	if err != nil {
		return nil, fmt.Errorf("load aws config: %w", err)
	}
	return sqs.NewFromConfig(awsCfg, func(o *sqs.Options) {
		if cfg.AWSEndpoint != "" {
			o.BaseEndpoint = aws.String(cfg.AWSEndpoint)
		}
	}), nil
}

// Queues holds the resolved queue URLs.
type Queues struct {
	WagerURL  string
	DLQURL    string
	EventsURL string
}

// Resolve looks up every queue URL; it fails if any queue is missing, so a
// misprovisioned environment is detected at startup.
func Resolve(ctx context.Context, client *sqs.Client, cfg config.Config) (Queues, error) {
	var q Queues
	for name, dst := range map[string]*string{
		cfg.WagerQueueName: &q.WagerURL, cfg.WagerDLQName: &q.DLQURL, cfg.EventsQueueName: &q.EventsURL,
	} {
		out, err := client.GetQueueUrl(ctx, &sqs.GetQueueUrlInput{QueueName: aws.String(name)})
		if err != nil {
			return Queues{}, fmt.Errorf("resolve queue %s: %w", name, err)
		}
		*dst = aws.ToString(out.QueueUrl)
	}
	return q, nil
}

// Ping verifies SQS reachability for readiness checks.
func Ping(ctx context.Context, client *sqs.Client, queueURL string) error {
	_, err := client.GetQueueAttributes(ctx, &sqs.GetQueueAttributesInput{
		QueueUrl:       aws.String(queueURL),
		AttributeNames: []types.QueueAttributeName{types.QueueAttributeNameApproximateNumberOfMessages},
	})
	return err
}

// EventSender publishes outbox events to the events queue (a standard queue).
// The eventId travels as a message attribute and inside the body so consumers
// can deduplicate redeliveries and republications.
type EventSender struct {
	client   *sqs.Client
	queueURL func() string
}

func NewEventSender(client *sqs.Client, queueURL func() string) *EventSender {
	return &EventSender{client: client, queueURL: queueURL}
}

func (s *EventSender) Send(ctx context.Context, eventID, eventType string, payload []byte) error {
	_, err := s.client.SendMessage(ctx, &sqs.SendMessageInput{
		QueueUrl:    aws.String(s.queueURL()),
		MessageBody: aws.String(string(payload)),
		MessageAttributes: map[string]types.MessageAttributeValue{
			"eventId":   {DataType: aws.String("String"), StringValue: aws.String(eventID)},
			"eventType": {DataType: aws.String("String"), StringValue: aws.String(eventType)},
		},
	})
	return err
}
