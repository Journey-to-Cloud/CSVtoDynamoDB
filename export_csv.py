"""
This module addresses the functionality of reading the values from CSV file and exporting them to DynamoDB table. 
It also validates the items after the insert to make sure the records are updated. If yes, all good. If a record is missed
during the insert, it updates the error description of that particular row.
"""

import boto3
import csv
import json
from pprint import pprint
from datetime import datetime
import input_output as io
import threading
import math

def read_csv(csv_file_name):
    """
    This function reads the CSV file

    Args:
        csv_file_name: The name of the CSV file
    """
    with open(csv_file_name, newline='') as csv_file:
        reader = csv.reader(csv_file)
        for row in reader:
            column_names = row
            break
        
        # Adding the additional columns to reflect the status of each insert
        column_names.append('Success/Failure')
        column_names.append('Error Code')
        column_names.append('Error Description')
        item_collection = []
        for row in reader:
            item = {}
            for column in range (0, len(column_names)):
                if column_names[column] == 'Time':
                    item[column_names[column]] = datetime.strptime(row[column], "%d/%m/%y %H:%M").isoformat()
                elif column_names[column] == 'Success/Failure':
                    item[column_names[column]] = "Success"
                elif column_names[column] == 'Error Code':
                    item[column_names[column]] = "0"
                elif column_names[column] == 'Error Description':
                    item[column_names[column]] = "No Error"
                else:
                    item[column_names[column]] = row[column]
            item_collection.append(item)
        csv_file.close()
        return_values = [column_names,item_collection]
        return return_values

def prep_write(table, item_collection):
    """
    This functions splits the given collection of csv rows into 3 sets and spins up 3 threads to write to
    DynamoDB parallely.

    Args:
        table: This is the boto3 DynamoDB resource which refers to the table
        item_collecti
    """
    items_length = len(item_collection)
    item_set_1 = math.ceil(items_length/3)
    item_set_2 = item_set_1
    item_set_3 = items_length - (item_set_1 + item_set_2)
    item_collection_1 = item_collection[:item_set_1]
    item_collection_2 = item_collection[item_set_1:item_set_3]
    item_collection_3 = item_collection[item_set_3:]
    thread_1 = threading.Thread(target=batch_write, args=(table,item_collection_1))
    thread_2 = threading.Thread(target=batch_write, args=(table,item_collection_2))
    thread_3 = threading.Thread(target=batch_write, args=(table,item_collection_3))
    thread_1.start()
    thread_2.start()
    thread_3.start()
    thread_1.join()
    thread_2.join()
    thread_3.join()

def batch_write(table, item_collection):
    """
    Performs a batch write operation on DynamoDB. Please note that the maximum number of Items that can be pushed through is 25

    Args:
        table: This is the boto3 DynamoDB resource which refers to the table
        item_collection: This is the dictionary of all the rows read from the CSV file
    """
    io.console_output('Beginning csv to dynamoDB import...')
    count = 0
    try:
        with table.batch_writer() as batch:
            for item in item_collection:
                batch.put_item(Item=item)
                count += 1
    except Exception as e:
        io.console_output("Error: "+ str(e))
    io.console_output('Inserted ' + str(count) + ' items...')
    io.console_output('Finished importing data into dynamoDB...')
    return str(e)

def validate(table, table_name, item_collection, partition_key_col_name, sort_key_col_name):
    """
    Validates the Items inserted into the DynamoDB. It iterates over each CSV row and queries the DynamoDB to get the matching Item.
    If it does not get any response, it re-inserts the record and updates the error code and description accordingly.

    Args:
        table: This is the boto3 DynamoDB resource which refers to the table
        table_name: name of the table in DynamoDB
        item_collection: This is the dictionary of all the rows read from the CSV file
        partition_key_col_name: This is the name of the primary key (hash key)
        sort_key_col_name: This is the name of the sort key (range key)
        
    """
    io.console_output('Beginning data validation...')
    for row in item_collection:
        key = {partition_key_col_name: row[partition_key_col_name], sort_key_col_name: row[sort_key_col_name]}
        try:
            response = table.get_item(Key=key)
            assert('Item' in response)
        except AssertionError:
            io.console_output('Failed to validate data. Key ' + json.dumps(key) + ' does not exist. Re-inserting the item')
            row['Success/Failure'] = "Failure"
            row['Error Code'] = "1"
            row['Error Description'] = "Data re-inserted after validation"
            table.put_item(Item=row)
            
    io.console_output('Finished data validation...')