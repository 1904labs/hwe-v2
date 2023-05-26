import aws_cdk as core
import aws_cdk.assertions as assertions

from cloud_streaming_pipeline_cdk.cloud_streaming_pipeline_cdk_stack import CloudStreamingPipelineCdkStack

# example tests. To run these tests, uncomment this file along with the example
# resource in cloud_streaming_pipeline_cdk/cloud_streaming_pipeline_cdk_stack.py
def test_sqs_queue_created():
    app = core.App()
    stack = CloudStreamingPipelineCdkStack(app, "cloud-streaming-pipeline-cdk")
    template = assertions.Template.from_stack(stack)

#     template.has_resource_properties("AWS::SQS::Queue", {
#         "VisibilityTimeout": 300
#     })
