#!/usr/bin/env python3

from aws_cdk import core

from aws_athena_cqrs_patterns.aws_athena_cqrs_patterns_stack import AwsAthenaCqrsPatternsStack

_env = core.Environment(
  account=os.environ["CDK_DEFAULT_ACCOUNT"],
  region=os.environ["CDK_DEFAULT_REGION"])

app = core.App()
AwsAthenaCqrsPatternsStack(app, "aws-athena-cqrs-patterns", env=_env)

app.synth()
