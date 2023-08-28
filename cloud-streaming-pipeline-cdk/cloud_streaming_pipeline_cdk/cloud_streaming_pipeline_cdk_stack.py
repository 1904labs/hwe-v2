import aws_cdk as cdk
from aws_cdk import (
    # Duration,
    aws_dynamodb as ddb,
    aws_kinesis as kinesis,
    Stack,
    # aws_sqs as sqs,
    aws_s3 as s3,
    aws_glue as glue,
    aws_athena as athena,
    aws_s3_deployment as s3deploy
)

from constructs import Construct

class CloudStreamingPipelineCdkStack(Stack):

    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)
        '''
        # Create a new DynamoDB table
        # If this table already exists, deploy will fail.
        self.dynamo_table = ddb.Table(
            self, "dynamo-customers",
            table_name = "customers",
            partition_key=ddb.Attribute(
                name="customer_id",
                type=ddb.AttributeType.STRING
            ),
            billing_mode=ddb.BillingMode.PAY_PER_REQUEST
        )
        '''
        # Create an s3 bucket for the new semester
        # Create subfolders within that bucket for each handle
        with open('semester_config.txt') as semester_file:
            semester = semester_file.read().splitlines()[0]
            semester_bucket = s3.Bucket(self, semester + '1', bucket_name=semester, removal_policy=cdk.RemovalPolicy.DESTROY)
        
        with open('handles.txt') as handles_file:
            handles = handles_file.read().splitlines()
            hweBucketDeployment = s3deploy.BucketDeployment(self, 'handles', sources=[], destination_bucket=semester_bucket)
            for handle in handles:
                hweBucketDeployment.add_source(s3deploy.Source.data(handle + '/placeholder.txt', ''))

        
        # Create an S3 bucket for Athena query results
        # If this bucket already exists, deploy will fail.
        # s3.Bucket(self, 'hwe-athena-query-results', bucket_name='hwe-athena-query-results', removal_policy=cdk.RemovalPolicy.DESTROY)

        '''
        # Create an Athena WorkGroup
        workgroup = athena.CfnWorkGroup(
            self,
            "hwe-athena-workgroup",
            name="hwe",
            work_group_configuration=athena.CfnWorkGroup.WorkGroupConfigurationProperty(
                result_configuration=athena.CfnWorkGroup.ResultConfigurationProperty(
                    output_location="s3://hwe-athena-query-results/athena-query-results/"
                )
            )
        )
        
        
        # Create a Glue Data Catalog for Athena
        database = glue.CfnDatabase(
            self,
            "hwe-glue-data-catalog", catalog_id = '153601099083',
            database_input=glue.CfnDatabase.DatabaseInputProperty(
                name="hwe"
            ),
        )
        '''