#!/usr/bin/env python3
# -*- encoding: utf-8 -*-
# vim: tabstop=2 shiftwidth=2 softtabstop=2 expandtab

from aws_cdk import (
  core
  aws_s3 as s3
)

class AwsAthenaCqrsPatternsStack(core.Stack):

  def __init__(self, scope: core.Construct, id: str, **kwargs) -> None:
    super().__init__(scope, id, **kwargs)

    # The code that defines your stack goes here
    # The code that defines your stack goes here
    vpc_name = self.node.try_get_context("vpc_name")
    vpc = aws_ec2.Vpc.from_lookup(self, "VPC",
      # is_default=True, #XXX: Whether to match the default VPC
      vpc_name=vpc_name)

    # s3_bucket_name = self.node.try_get_context('s3_bucket_name')
    # s3_bucket = s3.Bucket.from_bucket_name(self, id, s3_bucket_name)
    s3_bucket_name_suffix = self.node.try_get_context('s3_bucket_name_suffix')
    s3_bucket = s3.Bucket(self, 'TransRecentAnncmtBucket',
      bucket_name='aws-rss-feed-{region}-{suffix}'.format(region=core.Aws.REGION,
        suffix=s3_bucket_name_suffix))

    s3_bucket.add_lifecycle_rule(prefix='query-results/', id='query-results',
      abort_incomplete_multipart_upload_after=core.Duration.days(3),
      expiration=core.Duration.days(7))
