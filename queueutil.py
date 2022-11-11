import boto3


def get_queue_url():
    # Credentials for Abhay user
    #sqs_client = boto3.client("sqs", aws_access_key_id='AKIAT25FSAUCA4HBOPX5',
    #                      aws_secret_access_key='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', region_name='us-east-1')
    sqs_client = boto3.client("sqs")
    response = sqs_client.get_queue_url(
        QueueName="expQueue",
    )
    print(response)
    return response["QueueUrl"]

def sendMsg(queueurl, qmsg):
    #sqs_client = boto3.client("sqs", aws_access_key_id='AKIAT25FSAUCA4HBOPX5',
    #                      aws_secret_access_key='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', region_name='us-east-1')
    sqs_client = boto3.client("sqs")
    #message = {"From Lambda": "Queued msg #29"}
    response2 = sqs_client.send_message(
        QueueUrl=queueurl,
        MessageBody=(qmsg)
    )
    print(response2)

def recvMsg(queueurl):
    #sqs_client = boto3.client("sqs", aws_access_key_id='AKIAT25FSAUCA4HBOPX5',
    #                      aws_secret_access_key='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', region_name='us-east-1')
    sqs_client = boto3.client("sqs")
    response = sqs_client.receive_message(
        QueueUrl=queueurl,
        AttributeNames=[
            'SentTimestamp'
        ],
        MaxNumberOfMessages=10,
        MessageAttributeNames=[
            'All'
        ],
        VisibilityTimeout=100,
        WaitTimeSeconds=0
    )
    print(response)
    message = response['Messages'][1]
    print(message['Body'])
    receipt_handle = message['ReceiptHandle']
    print(receipt_handle)
    sqs_client.delete_message(
        QueueUrl=queueurl,
        ReceiptHandle=receipt_handle
    )