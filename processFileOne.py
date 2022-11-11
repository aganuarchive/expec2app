
import boto3 
from botocore.exceptions import ClientError
from boto3.dynamodb.conditions import Key
import processfile2

print('hello from ec2')

s3 = boto3.client('s3')

try:
    #dynamodb = boto3.resource('dynamodb', aws_access_key_id='AKIAT25FSAUCA4HBOPX5',
    #                      aws_secret_access_key='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', region_name='us-east-1')    
    filefound = 0
    dynamodb = boto3.resource('dynamodb')    
    l_table = dynamodb.Table("SHOP-FILE-TRNS") 
    l_filename = ''
    try:
        response = l_table.query(IndexName='FILESTATUS-index',KeyConditionExpression=Key('FILESTATUS').eq(0))
        print(response)
    except ClientError as e:
        print(e.response['Error']['Message'])
    else:
        if (response['Count'] > 0 ):
            l_filename = response['Items'][0]['SHOPFILENAME']
            if (l_filename.find('Shopping-') >= 0):
                filefound = 1
            print( response['Items'][0]['SHOPFILENAME'])

    if (filefound > 0):
        #client = boto3.client('s3', aws_access_key_id='AKIAT25FSAUCA4HBOPX5',
        #                      aws_secret_access_key='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', region_name='us-east-1')
        client = boto3.client('s3')
        s3response = client.get_object(Bucket = 'expjune2022store', Key = l_filename)

        Uresponse = l_table.update_item(
            Key={
                'SHOPFILENAME': l_filename
            },
            UpdateExpression="set FILESTATUS = :r",
            ExpressionAttributeValues={
                ':r': 1,
            },        
            ReturnValues="UPDATED_NEW"
        )
        print(Uresponse) 
        processfile2.processFileContent( s3response, response['Items'][0]['SHOPFILENAME'])
    else:
        print('File not available')
except Exception as e:
        print(e)
