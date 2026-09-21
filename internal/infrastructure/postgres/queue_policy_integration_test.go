//go:build integration

package postgres

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
)

func configureTestQueuePolicy(t *testing.T, ctx context.Context, client *sqs.Client, queueURL string) string {
	t.Helper()
	attributes, err := client.GetQueueAttributes(ctx, &sqs.GetQueueAttributesInput{QueueUrl: aws.String(queueURL), AttributeNames: []types.QueueAttributeName{types.QueueAttributeNameQueueArn}})
	if err != nil {
		t.Fatal(err)
	}
	arn := attributes.Attributes["QueueArn"]
	policy, err := json.Marshal(map[string]any{"Version": "2012-10-17", "Statement": []any{map[string]any{
		"Effect": "Allow", "Principal": map[string]string{"AWS": "arn:aws:iam::000000000000:root"},
		"Action": "sqs:SendMessage", "Resource": arn,
	}}})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := client.SetQueueAttributes(ctx, &sqs.SetQueueAttributesInput{QueueUrl: aws.String(queueURL), Attributes: map[string]string{"Policy": string(policy)}}); err != nil {
		t.Fatal(err)
	}
	return arn
}
