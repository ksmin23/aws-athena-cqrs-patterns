#!/usr/bin/env python3
# -*- encoding: utf-8 -*-
#vim: tabstop=2 shiftwidth=2 softtabstop=2 expandtab

import json
import os
import logging
import pprint
from urllib.parse import urlparse

import boto3
import botocore


LOGGER = logging.getLogger()
if len(LOGGER.handlers) > 0:
  # The Lambda environment pre-configures a handler logging to stderr.
  # If a handler is already configured, `.basicConfig` does not execute.
  # Thus we set the level directly.
  LOGGER.setLevel(logging.INFO)
else:
  logging.basicConfig(level=logging.INFO)

AWS_REGION_NAME = os.getenv('AWS_REGION_NAME', 'us-east-1')
DOWNLOAD_URL_TTL = int(os.getenv('DOWNLOAD_URL_TTL', '3600'))

def get_athena_query_result_location(query_execution_id):
  athena_client = boto3.client('athena', region_name=AWS_REGION_NAME)
  response = athena_client.get_query_execution(
    QueryExecutionId=query_execution_id
  )
  output_location = response['QueryExecution']['ResultConfiguration']['OutputLocation']
  return output_location


def create_presigned_url(bucket_name, object_name, expiration=3600):
  s3_client = boto3.client('s3', region_name=AWS_REGION_NAME)
  try:
    presigned_url = s3_client.generate_presigned_url('get_object',
                                                 Params={'Bucket': bucket_name,
                                                         'Key': object_name},
                                                 ExpiresIn=expiration)
  except botocore.exceptions.ClientError as ex:
    LOGGER.error(ex)
    return None

  return presigned_url


def lambda_handler(event, context):
  LOGGER.debug(event)
  current_query_state = event['detail']['currentState']
  if current_query_state != 'SUCCEEDED':
    #TODO: send alert by sns
    LOGGER.info('athena query state: %s' % current_query_state)
    return

  query_execution_id = event['detail']['queryExecutionId']
  output_location = get_athena_query_result_location(query_execution_id)
  LOGGER.info(output_location)

  url_parse_result = urlparse(output_location, scheme='s3')
  bucket_name, object_name = url_parse_result.netloc, url_parse_result.path.lstrip('/')
  presigned_url = create_presigned_url(bucket_name, object_name, expiration=DOWNLOAD_URL_TTL)
  LOGGER.info('presigned_url: %s' % presigned_url)

  #TODO: send email
  # read requester's email from DynamoDB
  # send email to requester


if __name__ == '__main__':
  import argparse

  parser = argparse.ArgumentParser()
  parser.add_argument('--region-name', default='us-east-1',
    help='aws region name: default=us-east-1')
  parser.add_argument('--query-execution-id', required=True,
    help='aws athena query execution id. ex: ce8826f3-6949-4405-81e5-392745da2c95')
  parser.add_argument('--work-group-name', default='primary',
    help='aws athena work group name: default=primary')

  options = parser.parse_args()
  AWS_REGION_NAME = options.region_name

  event_template = {
    "account": "111122223333",
    "detail": {
      "currentState": "SUCCEEDED",
      "previousState": "RUNNING",
      "queryExecutionId": options.query_execution_id,
      "sequenceNumber": "3",
      "statementType": "DML",
      "statementType": "DML",
      "versionId": "0",
      "workgroupName": options.work_group_name
    },
    "detail-type": "Athena Query State Change",
    "id": "d9b0f8f8-1f67-6772-a390-01556bb3c09d",
    "region": options.region_name,
    "resources": [],
    "source": "aws.athena",
    "time": "2020-11-24T05:52:12Z",
    "version": "0"
  }

  for query_state in ['SUCCEEDED', 'FAILED', 'CANCELLED']:
    event = dict(event_template)
    event['detail']['currentState'] = query_state
    lambda_handler(event, {})

