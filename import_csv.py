"""
This module addresses the functionality of looking up items from DynamoDB and converting them to CSV.
"""

import boto3
import csv
from boto3.dynamodb.conditions import Key
import input_output as io


def scan_table(table_name,table,filter_key, filter_value,filter_value2, column_names):
    """
    Perform a scan operation on table.
    Can specify filter_key (col name) and its value to be filtered.

    Args:
        table_name: name of the table in DynamoDB
        table: This is the boto3 DynamoDB resource which refers to the table
        filter_key: This param takes the name of the key with which you are going to perform the scan
        filter_value: This is the low range of the filter
        filter_value2: This is the high range of the filter
        column_names: This is the list of the headers in the csv
    """

    if filter_key and filter_value:
        filtering_exp = Key(filter_key).between(filter_value,filter_value2)
        response = table.scan(FilterExpression=filtering_exp)
    else:
        response = table.scan()

    write_to_csv(column_names,response['Items'])


def query_table(table_name,table,filter_key, filter_value, column_names):
    """
    Perform a query operation on table.
    Can specify filter_key (col name) and its value to be filtered.

    Args:
        table_name: name of the table in DynamoDB
        table: This is the boto3 DynamoDB resource which refers to the table
        filter_key: This param takes the name of the primary key with which you are going to perform the scan
        filter_value: This is the primary key value
        column_names: This is the list of the headers in the csv
    """
    filtering_exp = Key(filter_key).eq(filter_value)
    response = table.query(KeyConditionExpression=filtering_exp)
    write_to_csv(column_names,response['Items'])

def write_to_csv(column_names, response):
    """
    Converts the query/scan outputs to CSV

    Args:
        column_names: This is the list of the headers in the csv
        response: This is the response from the query/scan operations
    """
    csv_file = "output.csv"
    try:
        with open(csv_file, 'w') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=column_names)
            writer.writeheader()
            for data in response:
                writer.writerow(data)
    except IOError:
        io.console_output("I/O error") 
    return csv_file
