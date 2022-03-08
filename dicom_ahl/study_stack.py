# Study processor stack
#
#   Implementation used example:
#   https://github.com/aws-samples/aws-cdk-examples/blob/master/python/api-sqs-lambda/api_sqs_lambda/api_sqs_lambda_stack.py
#
import json

from aws_cdk import (
    aws_lambda as aws_lambda,
    aws_lambda_python as aws_lambda_py,
    aws_iam as iam,
    aws_sqs as sqs,
    aws_s3 as s3,
    core,
    aws_apigateway as apigateway,
    aws_logs as logs
)
from aws_cdk.aws_lambda_event_sources import SqsEventSource
from aws_cdk.aws_sqs import QueueEncryption, DeadLetterQueue


class StudyProcessorStack(core.Stack):

    def __init__(self, scope: core.Construct, id: str, **kwargs) -> None:
        super().__init__(scope, id, **kwargs)

        with open("dicom_ahl/config.json", 'r') as stream:
            configs = json.load(stream)

        #
        # Create bucket and explicitly apply best security practices for blocking public access and encryption
        #

        ingest_bucket = s3.Bucket(self, configs["dicom"]["source_bucket_prefix"],
                    block_public_access=s3.BlockPublicAccess.BLOCK_ALL,
                    encryption=s3.BucketEncryption.S3_MANAGED,
                    enforce_ssl=True)

        template_bucket = s3.Bucket(self, configs["dicom"]["template_bucket_prefix"],
                    block_public_access=s3.BlockPublicAccess.BLOCK_ALL,
                    encryption=s3.BucketEncryption.S3_MANAGED,
                    enforce_ssl=True)

        ###
        # Create SQS queue
        ###

        dicom_bad_queue = sqs.Queue(self, "Bad SOP Instance Queue",
                              encryption=QueueEncryption.KMS_MANAGED,
                                retention_period=core.Duration.days(1))

        dead_ltr_queue = sqs.Queue(self, "Dead Letter Queue",
                              encryption=QueueEncryption.KMS_MANAGED,
                                retention_period=core.Duration.days(1))

        queue_timeout=core.Duration.seconds(300)
        dicom_queue = sqs.Queue(self, "DICOM Queue",visibility_timeout=queue_timeout,
                                encryption=QueueEncryption.KMS_MANAGED,
                                dead_letter_queue=DeadLetterQueue(max_receive_count=1,
                                                                  queue=dead_ltr_queue)
                                )

        rest_api_role = iam.Role(self, "RestAPIRole",
                                assumed_by=iam.ServicePrincipal("apigateway.amazonaws.com"),
                                managed_policies=[iam.ManagedPolicy.from_aws_managed_policy_name("AmazonSQSFullAccess")]
                                )

        sqs_role = iam.Role(self, "SqsRole", 
                            assumed_by=iam.ServicePrincipal("apigateway.amazonaws.com")
                            )

        sqs_role.add_to_policy(iam.PolicyStatement(effect=iam.Effect.ALLOW, 
                               resources=[dicom_queue.queue_arn],
                               actions=["sqs:*"] # SendMessage","sqs:GetQueueAttributes","sqs:GetQueueUrl"]
                               ))

        ###
        # Create API Gateway and Integrate with SQS queue
        ###
        prd_log_group = logs.LogGroup(self, "dicom-ahl-api-logs")
        api_base = apigateway.RestApi(self, "dicom-ahl-api",
                                      deploy_options={"access_log_destination": apigateway.LogGroupLogDestination(prd_log_group),
                                                      "access_log_format": apigateway.AccessLogFormat.json_with_standard_fields(
                                                                           caller=True,
                                                                           http_method=True,
                                                                           ip=True,
                                                                           protocol=True,
                                                                           request_time=True,
                                                                           resource_path=True,
                                                                           response_length=True,
                                                                           status=True,
                                                                           user=True)})

        api_base.root.add_method("ANY")

        api_resource_path = "study"
        studies = api_base.root.add_resource(api_resource_path)

        #Create API Integration Response object
        integration_response = apigateway.IntegrationResponse(status_code="200",
                                                              response_templates={"application/json": ""},
                                                              )

        #Create API Integration Options object - https://docs.aws.amazon.com/cdk/api/latest/python/aws_cdk.aws_apigateway/IntegrationOptions.html
        api_integration_options = apigateway.IntegrationOptions(
                                                        credentials_role=rest_api_role,
                                                        integration_responses=[integration_response],
                                                        request_templates={"application/json": "Action=SendMessage&MessageBody=$input.body"},
                                                        passthrough_behavior=apigateway.PassthroughBehavior.NEVER,
                                                        request_parameters={"integration.request.header.Content-Type": "'application/x-www-form-urlencoded'"},
                                                        )

        api_resource_sqs_integration = apigateway.AwsIntegration(service="sqs",
                                                            integration_http_method="POST",
                                                            path="{}/{}".format(core.Aws.ACCOUNT_ID, dicom_queue.queue_name),
                                                            options=api_integration_options
                                                            )

        #Create a Method Response Object - https://docs.aws.amazon.com/cdk/api/latest/python/aws_cdk.aws_apigateway/MethodResponse.html
        method_response = apigateway.MethodResponse(status_code="200")

        studies.add_method(
            "POST",
            api_resource_sqs_integration,
            method_responses=[method_response]
        )

        ###
        # Create Lambda that subscribes to above SQS queue
        ###
        sqs_fhir_subscriber = aws_lambda_py.PythonFunction(self,
                                                "Echo",
                                                entry="lambdas",
                                                index='aggregator.py',
                                                handler='sqs_lambda_handler',
                                                runtime=aws_lambda.Runtime.PYTHON_3_8,
                                                memory_size=1769,                       # one vCPU-second of credits per second
                                                timeout=queue_timeout,
                                                dead_letter_queue_enabled=False,
                                                environment={
                                                    'LOG_LEVEL': configs['log_level'],
                                                    'SOP_BYTES_READ': str(configs['dicom']['sop_instance_bytes_to_read']),
                                                    'TEMPLATE_BUCKET': template_bucket.bucket_name,
                                                    'TEMPLATE_KEY': configs['fhir']['template_key'],
                                                    'TEMPLATE_MAP_KEY': configs['fhir']['template_map_key'],
                                                    'HEALTHLAKE_HOST': configs['healthlake']['healthlake_host'],
                                                    'HEALTHLAKE_ENDPOINT': configs['healthlake']['healthlake_endpoint'],
                                                    'REGION': configs['region'],
                                                    'BAD_QUEUE': dicom_bad_queue.queue_url
                                                })

        ingest_bucket.grant_read(sqs_fhir_subscriber)

        # template_bucket = s3.Bucket.from_bucket_name(self, "TemplateBucket", configs['fhir']['template_bucket_name'])
        template_bucket.grant_read(sqs_fhir_subscriber)

        dicom_queue.grant_consume_messages(sqs_fhir_subscriber)
        dicom_bad_queue.grant_send_messages(sqs_fhir_subscriber)

        sqs_fhir_subscriber.add_event_source(SqsEventSource(dicom_queue,
                                                            batch_size=10))

        # Add permission for s3_handler to write to HealthLake.  We have to create the policy 
        # statements manually until Boto3 supports AHL.
        ahl_statement1 = iam.PolicyStatement(
                                effect=iam.Effect.ALLOW,
                                actions=["healthlake:*",
                                         "iam:ListRoles"],
                                resources=['*'])

        ahl_statement2 = iam.PolicyStatement(
                                effect=iam.Effect.ALLOW,
                                actions=["iam:PassRole"],
                                resources=['*'],
                                conditions={"StringEquals": {
                                    "iam:PassedToService": "healthlake.amazonaws.com"}})

        sqs_fhir_subscriber.add_to_role_policy(ahl_statement1)
        sqs_fhir_subscriber.add_to_role_policy(ahl_statement2)    

        # Generate CF Outputs 
        core.CfnOutput(self, "DicomIngestionS3Bucket", value=ingest_bucket.bucket_name)
        core.CfnOutput(self, "TemplateS3Bucket", value=template_bucket.bucket_name)
        core.CfnOutput(self, "DicomApiUrl",
            value=f"https://{api_base.rest_api_id}.execute-api.{self.region}.amazonaws.com/prod/{api_resource_path}"
            )


