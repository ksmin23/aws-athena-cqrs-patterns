#!/usr/bin/env python3
# -*- encoding: utf-8 -*-
#vim: tabstop=2 shiftwidth=2 softtabstop=2 expandtab

import json
import os
import logging
import pprint

import boto3

LOGGER = logging.getLogger()
if len(LOGGER.handlers) > 0:
  # The Lambda environment pre-configures a handler logging to stderr.
  # If a handler is already configured, `.basicConfig` does not execute.
  # Thus we set the level directly.
  LOGGER.setLevel(logging.INFO)
else:
  logging.basicConfig(level=logging.INFO)

AWS_REGION_NAME=os.getenv('AWS_REGION_NAME', 'us-east-1')


def lambda_handler(event, context):
  LOGGER.debug(event)
  current_query_state = event['detail']['currentState']
  if current_query_state != 'SUCCEEDED':
    #TODO: send alert by sns
    LOGGER.info('athena query state: %s' % current_query_state)
    return

  athena_client = boto3.client('athena', region_name=AWS_REGION_NAME)
  response = athena_client.get_query_execution(
    QueryExecutionId=event['detail']['queryExecutionId']
  )

  pprint.pprint(response) #debug

  output_location = response['QueryExecution']['ResultConfiguration']['OutputLocation']
  LOGGER.info(output_location)
  #TODO: create presigned url
  #TODO: send email


if __name__ == '__main__':
  import argparse

  parser = argparse.ArgumentParser()
  parser.add_argument('--region-name', default='us-east-1',
    help='aws region name')
  parser.add_argument('--query-execution-id', required=True,
    help='aws athena query execution id. ex: ce8826f3-6949-4405-81e5-392745da2c95')
  parser.add_argument('--work-group-name', default='primary',
    help='aws athena work group name')

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

