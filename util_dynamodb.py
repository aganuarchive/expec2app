import boto3
from boto3.dynamodb.conditions import Key

def getDynamoDB()->boto3:
    #dynamo = boto3.resource('dynamodb', aws_access_key_id='AKIAT25FSAUCA4HBOPX5',
    #                      aws_secret_access_key='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')
    dynamo = boto3.resource('dynamodb')

    return dynamo

def getFieldName(dynamo, fieldtag)->str:
    response = dynamo.get_item(
        TableName="TRN-HEADER-FIELDS",
        Key={
            'FIELDTAG': fieldtag
        }
    )
    return response['Item']['FIELDNAME']['S']

def getTrnHeaderFieldName(dynamo, fieldtag)->str:
    l_table = dynamo.Table("TRN-HEADER-FIELDS") 
    response = l_table.query(KeyConditionExpression=Key('FIELDTAG').eq(fieldtag))
    print(response)
    return response['Items'][0]['FIELDNAME']

def getTrnLinesFieldName(dynamo, fieldtag)->str:
    l_table = dynamo.Table("TRN-LINES-FIELDS") 
    response = l_table.query(KeyConditionExpression=Key('FIELDTAG').eq(fieldtag))
    print(response)
    return response['Items'][0]['FIELDNAME']

