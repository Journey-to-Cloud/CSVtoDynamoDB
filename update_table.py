"""
This module updates the provisioned read/write units after the data has been inserted. We provisioned 100 read and 300 write units
initially to speed up the export process. As we get billed based on the provisioned units, it is better to bring the capacity down after 
writing data.
"""

import boto3


def reduce_capacity(table_name):
    """
    This function reduces the provisioned capacity units of the table

    Args:
        table_name: The name of the table
    """
    client = boto3.client('dynamodb')
    response = client.update_table(
        ProvisionedThroughput={
            'ReadCapacityUnits': 5,
            'WriteCapacityUnits': 5,
        },
        TableName=table_name,
    )
    return response
