"""
This module addresses the functionality of looking up items from DynamoDB and converting them to CSV.
"""

import boto3
import csv
from boto3.dynamodb.conditions import Key
import input_output as io


def scan_table(table_name,table,filter_key, filter_value,filter_value2):
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
    try:
        column_names = []
        if filter_key and filter_value:
            filtering_exp = Key(filter_key).between(filter_value,filter_value2)
            response = table.scan(FilterExpression=filtering_exp)
        else:
            response = table.scan()
        temp_dict = response['Items'][0]
        for key,value in temp_dict.items():
            column_names.append(key)
        column_names.sort()
        io.write_to_csv(column_names,response['Items'],"data_from_db.csv")
    except Exception as e:
        io.console_output("The program had to terminate because of the following error in scan_table: "+str(e))
        exit(1)


def query_table(table_name,table,filter_key, filter_value):
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
    try:
        column_names = []
        filtering_exp = Key(filter_key).eq(filter_value)
        response = table.query(KeyConditionExpression=filtering_exp)
        temp_dict = response['Items'][0]
        for key,value in temp_dict.items():
            column_names.append(key)
        column_names.sort()
        io.write_to_csv(column_names,response['Items'],"data_from_db.csv")
    except Exception as e:
        io.console_output("The program had to terminate because of the following error in query_table: "+str(e))
        exit(1)
