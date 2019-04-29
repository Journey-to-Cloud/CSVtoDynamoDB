# CSVtoDynamoDB and DynamoDBtoCSV
This project reads a CSV file and transfers the records to DynamoDB and also provides a functionality to query/scan the items in DynamoDB and create an output file in CSV format.

The data transfer from CSV to DynamoDB includes multithreaded functionality where each threads calls a batch_write function of DynamoDB api.
