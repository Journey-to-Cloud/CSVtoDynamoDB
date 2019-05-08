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
from threading import Thread
import math
from copy import copy

class ThreadWithReturnValue(Thread):
    def __init__(self, group=None, target=None, name=None,
                 args=(), kwargs={}, Verbose=None):
        Thread.__init__(self, group, target, name, args, kwargs)
        self._return = None
    def run(self):
        if self._target is not None:
            self._return = self._target(*self._args,
                                                **self._kwargs)
    def join(self, *args):
        Thread.join(self, *args)
        return self._return

def read_csv(csv_file_name):
    """
    This function reads the CSV file

    Args:
        csv_file_name: The name of the CSV file
    """
    try:
        with open(csv_file_name, newline='') as csv_file:
            reader = csv.reader(csv_file)
            for row in reader:
                temp_column_names = row
                break
            column_names = []
            for each in temp_column_names:
                if not each:
                    continue
                else:
                    column_names.append(each)
            
            # Adding the additional columns to reflect the status of each insert
            output_column_names = []
            output_column_names = list(column_names)
            output_column_names.append('Success/Failure')
            output_column_names.append('Error Code')
            output_column_names.append('Error Description')
            item_collection = []
            
            for row in reader:
                
                item = {}
                for column in range (0, len(column_names)):
                    if column_names[column] == 'Time' or column_names[column] == 'time':
                        try:
                            date_time_value = datetime.strptime(row[column], "%d-%m-%y %H:%M").strftime("%d/%m/%y %H:%M")
                            item[column_names[column]] = datetime.strptime(date_time_value, "%d/%m/%y %H:%M").isoformat()
                        except ValueError:
                            item[column_names[column]] = datetime.strptime(row[column], "%d/%m/%y %H:%M").isoformat()
                        except Exception as e:
                            io.console_output("The program had to terminate because of the following error in read_csv: "+str(e))
                            exit(1)
                    else:
                        item[column_names[column]] = row[column]
                item_collection.append(item)
            csv_file.close()
            
            return_values = [column_names,item_collection,output_column_names]
            return return_values
    except Exception as e:
        io.console_output("The program had to terminate because of the following error in read_csv: "+str(e))

def prep_write(table, item_collection, partition_key_col_name,sort_key_col_name):
    """
    This functions splits the given collection of csv rows into 3 sets and spins up 3 threads to write to
    DynamoDB parallely.

    Args:
        table: This is the boto3 DynamoDB resource which refers to the table
        item_collecti
    """
    try:
        items_length = len(item_collection)
        item_set_1 = math.ceil(items_length/3)
        item_set_2 = item_set_1
        item_set_3 = items_length - (item_set_1 + item_set_2)
        item_collection_1 = item_collection[:item_set_1]
        item_collection_2 = item_collection[item_set_1:item_set_3]
        item_collection_3 = item_collection[item_set_3:]
        thread_1 = ThreadWithReturnValue(target=batch_write, args=(table,item_collection_1, partition_key_col_name,sort_key_col_name,"thread-1"))
        thread_2 = ThreadWithReturnValue(target=batch_write, args=(table,item_collection_2, partition_key_col_name,sort_key_col_name,"thread-2"))
        thread_3 = ThreadWithReturnValue(target=batch_write, args=(table,item_collection_3, partition_key_col_name,sort_key_col_name,"thread-3"))
        io.console_output('Beginning csv to dynamoDB import\n')
        thread_1.start()
        thread_2.start()
        thread_3.start()
        result_thread_1 = thread_1.join()
        result_thread_2 = thread_2.join()
        result_thread_3 = thread_3.join()
        return(result_thread_1+","+result_thread_2+","+result_thread_3)
    except Exception as e:
        io.console_output("The program had to terminate because of the following error in prep_write: "+str(e))
        exit(1)

def batch_write(table, item_collection, partition_key_col_name,sort_key_col_name,thread_name):
    """
    Performs a batch write operation on DynamoDB. Please note that the maximum number of Items that can be pushed through is 25

    Args:
        table: This is the boto3 DynamoDB resource which refers to the table
        item_collection: This is the dictionary of all the rows read from the CSV file
    """
    count = 0
    try:
        with table.batch_writer(overwrite_by_pkeys=[partition_key_col_name, sort_key_col_name]) as batch:
            for item in item_collection:
                batch.put_item(Item=item)
                count += 1
    except Exception as e:
        io.console_output("Error: "+ str(e))
        exception = str(e)
        return exception
    io.console_output('Inserted ' + str(count) + ' items using: '+thread_name)
    return ("No Error in : "+thread_name)

def validate(table, table_name, item_collection, partition_key_col_name, sort_key_col_name, result):
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
    out_rows = []
    for row in item_collection:
        key = {partition_key_col_name: row[partition_key_col_name]}
        out_row = copy(row)
        try:
            response = table.get_item(Key=key,ConsistentRead=True)
            assert('Item' in response)
            out_row['Success/Failure'] = result
            out_row['Error Code'] = "0"
            out_row['Error Description'] = ""
        except Exception as e:
            io.console_output('Failed to validate data. Key ' + json.dumps(key) + ' does not exist. Re-inserting the item')
            out_row['Success/Failure'] = "Failure"
            out_row['Error Code'] = "1"
            out_row['Error Description'] = result + "," + str(e)
            table.put_item(Item=row)
        out_rows.append(out_row)
    io.console_output('Finished data validation...')
    return out_rows
            
    