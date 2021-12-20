import boto3
import json
import logging
import uuid
from botocore.exceptions import ClientError

sqs = boto3.client("sqs", region_name="us-east-1")
logger = logging.getLogger()

def lambda_handler(event,context):
    try:
        fileObj = open("data.txt","r")
        groupId = uuid.uuid4()
        for data in fileObj:
            print("File data >>>"+data)
            fifo_queue_response = sqs.send_message(
                QueueUrl="<< QUEUE URL >>",
                MessageBody=json.dumps(data),
                MessageGroupId = str(groupId),
                MessageDeduplicationId = str(uuid.uuid4())
            )
    except ClientError:
        logger.exception("Unable to push message to FIFO queue.")
        #raise
        return{
            'statusCode':400,
            'body':'Error occured while pushing a message to FIFO queue.'
        }
    else:
        return{
            'statusCode':200,
            'body':fifo_queue_response,
            'messageId':fifo_queue_response['MessageId']
        }
