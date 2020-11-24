#!/usr/bin/env python3
# -*- encoding: utf-8 -*-
# vim: tabstop=2 shiftwidth=2 softtabstop=2 expandtab

import json

def lambda_handler(event, context):
  print(event)


if __name__ == '__main__':
  event = None
  lambda_handler(event, {})
