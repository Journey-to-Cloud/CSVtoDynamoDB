"""
This module handles the input output functionality of the program. It is considered best practise to let one function
handle the interaction with the user/console.
"""

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