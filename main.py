"""
This module has the main function that calls all the functions defined in other files. This is the entry point
to the program.
"""
import boto3
from datetime import datetime
import export_csv
import create_table
import import_csv
import update_table
import input_output as io

global_variables = {
    'file_name':'',
    'column_names': []
}

def write(dynamodb_resource):
    """
    This function is responsible for calling the read from csv and write to dynamoDB functions defined in export_csv file.

    Args:
        table: This is the boto3 DynamoDB resource which refers to the table
        table_name: name of the table in DynamoDB
        item_collection: This is the dictionary of all the rows read from the CSV file
        partition_key_col_name: This is the name of the primary key (hash key)
        sort_key_col_name: This is the name of the sort key (range key)
    """
    table_name = io.user_input("Please enter the table name: ")
    table = dynamodb_resource.Table(table_name)
    csv_file_name = io.user_input("Please enter the name/path of the csv file: ")
    global_variables['file_name'] = csv_file_name
    return_values = export_csv.read_csv(csv_file_name)
    column_names = return_values[0]
    global_variables['column_names'] = column_names
    item_collection = return_values[1]
    output_column_names = return_values[2]
    io.console_output(column_names)
    io.console_output('From the above column names, please select: \n 1) Partition Key (A unique value that helps in identifying a record) \n 2) Sort Key (A value to help sort the records)')
    partition_key_col_name = io.user_input("Partition Key: ")
    sort_key_col_name = io.user_input ("Sort Key: ")
    io.console_output("Creating table: " + table_name)
    create_response = create_table.create_dynamoDB_table(table_name,partition_key_col_name,sort_key_col_name)
    if not create_response:
        result = export_csv.prep_write(table, item_collection, partition_key_col_name, sort_key_col_name)
        output = export_csv.validate(table, table_name, item_collection, partition_key_col_name, sort_key_col_name, result)
        update_table.reduce_capacity(table_name)
        io.write_to_csv(output_column_names,output,"write_status.csv")
    elif create_response:
        io.console_output("The table name already exists. Do you want to:\n1) Continue writing to the table\n2) Quit Writing")
        user_choice = io.user_input("Your Selection (1/2): ")
        if user_choice == "1":
            result = export_csv.prep_write(table, item_collection, partition_key_col_name,sort_key_col_name)
            output = export_csv.validate(table, table_name, item_collection, partition_key_col_name, sort_key_col_name, result)
            update_table.reduce_capacity(table_name)
            io.write_to_csv(output_column_names,output,"write_status.csv")
        elif user_choice == "2":
            io.console_output("Back to menu")

def read(dynamodb_resource):
    """
    This function is responsible for querying the DynamoDB

    Args:
        table: This is the boto3 DynamoDB resource which refers to the table
        table_name: name of the table in DynamoDB
        column_names: This is the list of the headers from the csv file
        partition_key_col_name: This is the name of the primary key (hash key)
        sort_key_col_name: This is the name of the sort key (range key)
    """
    client = boto3.client('dynamodb')
    table_name = io.user_input("Please enter the table name: ")
    table = dynamodb_resource.Table(table_name)
    response = client.describe_table(
    TableName=table_name
    )
    key_schemas = response['Table']['KeySchema']
    for each_key in key_schemas:
        if each_key['KeyType'] == 'HASH':
            partition_key_col_name = each_key['AttributeName']
        elif each_key['KeyType'] == 'RANGE':
            sort_key_col_name = each_key['AttributeName']
    column_names = global_variables['column_names']
    io.console_output("Please select one of the option: \n1) Search based on unique id\n2) Search based on a time range (eg: 16/4/19 2:22)\n3) quit program")
    user_choice = io.user_input("Your Selection (1/2/3): ")
    if user_choice == "1":
        unique_id = io.user_input("Please enter the unique id: ")
        import_csv.query_table(table_name,table,partition_key_col_name, unique_id, column_names)
    elif user_choice == "2":
        low_value = io.user_input("Please enter the low range value: ")
        high_value = io.user_input("Please enter the high range value: ")
        try:
            low_time_value = datetime.strptime(low_value, "%d/%m/%y %H:%M").isoformat()
            high_time_value = datetime.strptime(high_value, "%d/%m/%y %H:%M").isoformat()
            import_csv.scan_table(table_name,table,sort_key_col_name,low_time_value,high_time_value,column_names)
        except Exception:
            import_csv.scan_table(table_name,table,sort_key_col_name,low_value,high_value,column_names)

def main():
    """
    This is the entry point into the program. It initialises the required variables and passes them to the functions as required. Depending
    on the user input, the appropriate functions are invoked.
    """
    session = boto3.Session()
    region = session.region_name
    dynamodb_resource = boto3.resource('dynamodb', region_name=region)
    
    while(1):
        io.console_output("Please select one of the options:\n1) Write to DyanamoDB Table\n2) Read from DynamoDB Table\n3) quit")
        user_choice = io.user_input("Selection an option (1/2/3): ")
        if user_choice == "1":
            write(dynamodb_resource)
        elif user_choice == "2":
            io.console_output(read(dynamodb_resource))
        elif user_choice == "3":
            exit(0)
        else:
            io.console_output("Invalid Choice")

if __name__ == "__main__":
    main()