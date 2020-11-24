import json

def lambda_handler(event, context):
  print(event)

if __name__ == '__main__':
  event = ''
  lambda_handler(event, {})
