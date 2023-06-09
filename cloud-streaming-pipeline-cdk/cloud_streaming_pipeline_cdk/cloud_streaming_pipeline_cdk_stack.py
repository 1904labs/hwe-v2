import aws_cdk as cdk
from aws_cdk import (
    # Duration,
    aws_dynamodb as ddb,
    aws_kinesis as kinesis,
    Stack,
    # aws_sqs as sqs,
    aws_s3 as s3
)

from constructs import Construct

class CloudStreamingPipelineCdkStack(Stack):

    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)
        
        # Create a new DynamoDB table
        self.dynamo_table = ddb.Table(
            self, "dynamo-customers",
            table_name = "customers",
            partition_key=ddb.Attribute(
                name="customer_id",
                type=ddb.AttributeType.STRING
            ),
            billing_mode=ddb.BillingMode.PAY_PER_REQUEST
        )
        
        # s3 bucket for each handle in handles file
        with open('handles.txt') as file:
            handles = file.read().splitlines()
            for handle in handles:
                s3.Bucket(self, handle, bucket_name='hwe-' + handle, removal_policy=cdk.RemovalPolicy.DESTROY)
        # athena metastore
