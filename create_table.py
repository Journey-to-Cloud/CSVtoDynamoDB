"""
This module is responsible for creating the DynamoDB table
"""

import boto3


def create_dynamoDB_table(table_name, partition_key_col_name, sort_key_col_name):
    """
    This function checks if a table already exists with the same name. If yes, it exits the program. If not, it makes a new table.

    Args:
        table_name: name of the table in DynamoDB
        partition_key_col_name: This is the name of the primary key (hash key)
        sort_key_col_name: This is the name of the sort key (range key)
    """
    dynamodb_client = boto3.client('dynamodb')
    response = dynamodb_client.list_tables()
    if table_name in response['TableNames']:
        return 1
    else:
        dynamodb_client.create_table(
        TableName=table_name,
        KeySchema=[
            {
                'AttributeName': partition_key_col_name,
                'KeyType': 'HASH'  #Partition key
            },
            {
                'AttributeName': sort_key_col_name,
                'KeyType': 'RANGE'  #Sort key
            },
        ],
        AttributeDefinitions=[{
            'AttributeName': partition_key_col_name,
            'AttributeType': 'S',
        },
        {
            'AttributeName': sort_key_col_name,
            'AttributeType': 'S',
        }],
        ProvisionedThroughput={
            'ReadCapacityUnits': 100,
            'WriteCapacityUnits': 300,
            }
        )
        waiter = dynamodb_client.get_waiter('table_exists')
        waiter.wait(TableName=table_name)
        return 0
