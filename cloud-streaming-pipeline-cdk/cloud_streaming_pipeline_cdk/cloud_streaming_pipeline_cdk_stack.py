import aws_cdk as cdk
from aws_cdk import (
    # Duration,
    aws_dynamodb as ddb,
    aws_kinesis as kinesis,
    Stack
    # aws_sqs as sqs,
)

from constructs import Construct

class CloudStreamingPipelineCdkStack(Stack):

    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)
        
        # Create a new DynamoDB table
        self.dynamo_table = ddb.Table(
            self, "id123",
            table_name = "customers",
            partition_key=ddb.Attribute(
                name="customer_id",
                type=ddb.AttributeType.STRING
            ),
            billing_mode=ddb.BillingMode.PAY_PER_REQUEST
        )
        

        # Create a new Kinesis stream
        #The id123 is the id of the kinesis stream. Not sure on much more than that. 
        self.kinesis_stream = kinesis.Stream(
            self, "id123",
            shard_count=1,  # Start with 1 shard
            stream_name="Customers",
            retention_period=cdk.Duration.days(1)
        )
        # s3 bucket
        # athena metastore
