import boto3
import json

# Executado pelo LocalStack ao ficar pronto. CreateQueue também aceita
# uma fila já existente com os mesmos atributos.
sqs = boto3.client(
    "sqs",
    endpoint_url="http://localhost:4566",
    region_name="us-east-1",
    aws_access_key_id="test",
    aws_secret_access_key="test",
)
sqs.create_queue(QueueName="wager-events")

dlq = sqs.create_queue(
    QueueName="wager-transactions-dlq.fifo",
    Attributes={"FifoQueue": "true", "MessageRetentionPeriod": "1209600"},
)["QueueUrl"]
dlq_arn = sqs.get_queue_attributes(
    QueueUrl=dlq, AttributeNames=["QueueArn"]
)["Attributes"]["QueueArn"]
source = sqs.create_queue(
    QueueName="wager-transactions.fifo",
    Attributes={
        "FifoQueue": "true",
        "ContentBasedDeduplication": "false",
        "VisibilityTimeout": "30",
        "ReceiveMessageWaitTimeSeconds": "10",
        "RedrivePolicy": json.dumps({"deadLetterTargetArn": dlq_arn, "maxReceiveCount": 5}),
    },
)["QueueUrl"]
source_arn = sqs.get_queue_attributes(
    QueueUrl=source, AttributeNames=["QueueArn"]
)["Attributes"]["QueueArn"]
sqs.set_queue_attributes(
    QueueUrl=dlq,
    Attributes={"RedriveAllowPolicy": json.dumps({"redrivePermission": "byQueue", "sourceQueueArns": [source_arn]})},
)
