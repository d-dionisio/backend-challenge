import boto3
import json
from pathlib import Path

Path('/tmp/wager-queues-ready').unlink(missing_ok=True)

# Executado pelo LocalStack ao ficar pronto. CreateQueue também aceita
# uma fila já existente com os mesmos atributos.
sqs = boto3.client(
    "sqs",
    endpoint_url="http://localhost:4566",
    region_name="us-east-1",
    aws_access_key_id="test",
    aws_secret_access_key="test",
)
account = "000000000000"
producer_principal = f"arn:aws:iam::{account}:root"
consumer_principal = f"arn:aws:iam::{account}:root"

events = sqs.create_queue(QueueName="wager-events")["QueueUrl"]
events_arn = sqs.get_queue_attributes(
    QueueUrl=events, AttributeNames=["QueueArn"]
)["Attributes"]["QueueArn"]
sqs.set_queue_attributes(
    QueueUrl=events,
    Attributes={
        "Policy": json.dumps(
            {
                "Version": "2012-10-17",
                "Statement": [
                    {
                        "Effect": "Allow",
                        "Principal": {"AWS": consumer_principal},
                        "Action": "sqs:SendMessage",
                        "Resource": events_arn,
                    }
                ],
            }
        )
    },
)

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
sqs.set_queue_attributes(
    QueueUrl=source,
    Attributes={
        "Policy": json.dumps(
            {
                "Version": "2012-10-17",
                "Statement": [
                    {
                        "Effect": "Allow",
                        "Principal": {"AWS": producer_principal},
                        "Action": "sqs:SendMessage",
                        "Resource": source_arn,
                    }
                ],
            }
        )
    },
)

# Sinaliza que todas as filas e policies foram provisionadas.
Path('/tmp/wager-queues-ready').touch()
