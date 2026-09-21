import boto3

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
