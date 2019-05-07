"""
This module handles the input output functionality of the program. It is considered best practise to let one function
handle the interaction with the user/console.
"""
import csv

def user_input(message):
    """
    This function takes a user input

    Args:
        message: This is the question that is being asked to the user
    """
    response = input(message)
    while(1):
        if response != "":
            return response

def console_output(message):
    """
    This function displays a message on the console

    Args:
        message: This is the question that is being asked to the user
    """
    print(message)

def write_to_csv(column_names, response, filename):
    """
    Converts the query/scan outputs to CSV

    Args:
        column_names: This is the list of the headers in the csv
        response: This is the response from the query/scan operations
    """
    csv_file = filename
    try:
        with open(csv_file, 'w') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=column_names)
            writer.writeheader()
            for data in response:
                writer.writerow(data)
            return csv_file
    except Exception as e:
        console_output("The program had to terminate because of the following error in write_to_csv: "+str(e))
        exit(1)
    