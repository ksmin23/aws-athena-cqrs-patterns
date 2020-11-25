#!/usr/bin/env python3
# -*- encoding: utf-8 -*-
# vim: tabstop=2 shiftwidth=2 softtabstop=2 expandtab

from aws_cdk import (
  core,
  aws_ec2,
  aws_iam,
  aws_s3 as s3,
  aws_lambda as _lambda,
  aws_logs,
  aws_events,
  aws_events_targets
)

class AwsAthenaCqrsPatternsStack(core.Stack):

  def __init__(self, scope: core.Construct, id: str, **kwargs) -> None:
    super().__init__(scope, id, **kwargs)

    # The code that defines your stack goes here
    vpc_name = self.node.try_get_context("vpc_name")
    vpc = aws_ec2.Vpc.from_lookup(self, "VPC",
      is_default=True, #XXX: Whether to match the default VPC
      vpc_name=vpc_name)

    # s3_bucket_name = self.node.try_get_context('s3_bucket_name')
    # s3_bucket = s3.Bucket.from_bucket_name(self, id, s3_bucket_name)
    s3_bucket_name_suffix = self.node.try_get_context('s3_bucket_name_suffix')
    s3_bucket = s3.Bucket(self, 'AthenaQueryResultsBucket',
      bucket_name='aws-athena-cqrs-workspace-{region}-{suffix}'.format(region=core.Aws.REGION,
        suffix=s3_bucket_name_suffix))

    s3_bucket.add_lifecycle_rule(prefix='query-results/', id='query-results',
      abort_incomplete_multipart_upload_after=core.Duration.days(3),
      expiration=core.Duration.days(7))

    athena_work_group = self.node.try_get_context("athena_work_group_name")

    # QueryResultsHandler
    query_results_lambda_fn = _lambda.Function(self, "QueryResultsHandler",
      runtime=_lambda.Runtime.PYTHON_3_7,
      function_name="QueryResultsHandler",
      handler="query_results_handler.lambda_handler",
      description="athena query results handler",
      code=_lambda.Code.asset("./src/main/python/QueryResultsHandler"),
      environment={
        #TODO: MUST set appropriate environment variables for your workloads.
        'AWS_REGION_NAME': core.Aws.REGION,
        'DOWNLOAD_URL_TTL': '3600'
      },
      timeout=core.Duration.minutes(5)
    )

    managed_policy = aws_iam.ManagedPolicy.from_managed_policy_arn(self,
      'AthenaFullAccessPolicy',
      'arn:aws:iam::aws:policy/AmazonAthenaFullAccess')
    query_results_lambda_fn.role.add_managed_policy(managed_policy)

    query_results_lambda_fn.add_to_role_policy(aws_iam.PolicyStatement(
      effect=aws_iam.Effect.ALLOW,
      resources=[s3_bucket.bucket_arn, "{}/*".format(s3_bucket.bucket_arn)],
      actions=["s3:Get*",
        "s3:List*",
        "s3:PutObjectAcl",
        "s3:PutObjectVersionAcl"
      ]))

    log_group = aws_logs.LogGroup(self, "QueryResultsHandlerLogGroup",
      log_group_name="/aws/lambda/QueryResultsHandler",
      retention=aws_logs.RetentionDays.THREE_DAYS)
    log_group.grant_write(query_results_lambda_fn)

    #XXX: Athena Query State Change Event Pattern
    # {
    #   "source": [
    #     "aws.athena"
    #   ],
    #   "detail-type": [
    #     "Athena Query State Change"
    #   ],
    #   "detail": {
    #     "previousState": [
    #       "RUNNING"
    #     ],
    #     "workgroupName": [
    #       "primary"
    #     ]
    #   }
    #  }
    aws_event_pattern = aws_events.EventPattern(
      account=[core.Aws.ACCOUNT_ID],
      region=[core.Aws.REGION],
      source=['aws.athena'],
      detail_type=['Athena Query State Change'],
      detail={
        "previousState": ["RUNNING"],
        "workgroupName": [athena_work_group]
      }
    )

    lambda_fn_target = aws_events_targets.LambdaFunction(query_results_lambda_fn)
    event_rule = aws_events.Rule(self, "AthenaQueryExecutionRule",
      enabled=False,
      event_pattern=aws_event_pattern,
      description='Athena Query State Change Event',
      rule_name='AthenaQueryExecutionRule',
      targets=[lambda_fn_target]
    )
