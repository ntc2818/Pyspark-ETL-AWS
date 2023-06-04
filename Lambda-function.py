import json 
import boto3
import uuid
sfclient=boto3.client('stepfunctions')
def lambda_handler(event, context):
   response = sfclient.start_execution(stateMachineArn = '  STEPFUNCTION_ARN')
   return "Invoked Step Function Successfully"
