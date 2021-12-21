import boto3
import json
import logging
import uuid
from botocore.exceptions import ClientError

sqs = boto3.client("sqs", region_name="us-east-1")
logger = logging.getLogger()

# Function to open a file in read mode to read one by one
def push_data_to_sqs_queue():
    try:
        fileObj = open("data.txt","r")
        
        for data in fileObj:
            print("File data >>>"+data)
            queue_response = sqs.send_message(
                QueueUrl="<< QUEUE URL >>",
                MessageBody=json.dumps(data)
            )
            print(queue_response)
    except Exception as e:
        print ("Exception occured.",e)

# Function to check all the queues under AWS account
def get_sqs_queues():
    sqs = boto3.client('sqs')
    response = sqs.list_queues()
    print(response['QueueUrls'])

# Function to create a new standard sqs queue
def create_new_sqs():
    sqs = boto3.client('sqs',region_name="us-east-1")

    # Create a new queue
    create_queue_response = sqs.create_queue(
        QueueName="boto3-queue",
        Attributes={
            "DelaySeconds":"0",
            "VisibilityTimeout":"60"
        }
    )
    print(create_queue_response)

# Function to create a new FIFO sqs queue
def create_new_fifo_sqs():
    try:
        fifo_response = sqs.create_queue(
            QueueName="boto3-queue.fifo",
            Attributes={
                "DelaySeconds":"0",
                "VisibilityTimeout":"60",
                "FifoQueue":"true"
            }
        )
    except ClientError:
        logger.exception("Unable to create FIFO queue.")
        raise
    else:
        print(fifo_response)

def push_data_to_fifo_sqs_queue():
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
        raise
    else:
        print(fifo_queue_response['MessageId'])

def push_json_data_to_fifo_sqs_queue():
    try:
        dictObj = {}
        allRecords = []
        fields =['Title', 'Id', 'ProcessYear', 'SequenceId', 'Code', 'Name', 'InfoType', 'CustId', 'RecordDate', 'RecordReference']
        with open("data.txt") as fh:
            recordCount = 1
            for line in fh:
                # reads each line and trims of extra the spaces and gives only the valid words
                description = list( line.strip().split(None, 10))
                # print(description)
                recordSequence = "Records"
                fieldCount = 0
                innerDictObj = {}
                while fieldCount<len(fields):
                    innerDictObj[fields[fieldCount]]= description[fieldCount]
                    fieldCount = fieldCount + 1
                # appending the record of each record to main directory
                allRecords.append(innerDictObj)
                recordCount = recordCount + 1
            dictObj[recordSequence] = allRecords
        # creating json file
        out_file = open("test1.json", "w")
        jsonData = json.dump(dictObj, out_file, indent = 4)
        print(jsonData)
        out_file.close()

        # Reading the newly created json file and eval the values and push to fifo queue one by one
        readFile = open('test1.json')
        recordDir = json.load(readFile)
        groupId = uuid.uuid4()
        for rec in recordDir['Records']:
            print(rec)
            fifo_queue_response = sqs.send_message(
                QueueUrl="https://sqs.us-east-1.amazonaws.com/421788775798/boto3-queue.fifo",
                MessageBody=json.dumps(rec),
                MessageGroupId = str(groupId),
                MessageDeduplicationId = str(uuid.uuid4())
            )
        readFile.close()
            
    except ClientError:
        logger.exception("Unable to push message to FIFO queue.")
        raise
    else:
        print("Json file created successfully.")
        #print(fifo_queue_response['MessageId'])


# Calling function to send data to sqs queue
# get_sqs_queues()

# create_new_sqs()
# push_data_to_sqs_queue()

# create_new_fifo_sqs()
# push_data_to_fifo_sqs_queue()

push_json_data_to_fifo_sqs_queue()