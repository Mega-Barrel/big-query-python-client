""" Main file to insert data to BQ"""
from big_query import BigQueryOperations

if __name__ == '__main__':
    TABLE_ID = 'test_table'
    TABLE_SCHEMA = 'table_schema/zwya_transactions.json'
    TABLE_FILE_PATH = 'table_schema/zwya_transactions.xlsx'

    # Create BigQueryOperations instance
    bq = BigQueryOperations()
    # Create table
    bq.create_table(
        table_id=TABLE_ID,
        schema=TABLE_SCHEMA
    )
    # Read data from specified file.
    if '.csv' in TABLE_FILE_PATH:
        print('reading CSV file')
    elif '.xlsx' in TABLE_FILE_PATH:
        print('reading XLSX file')
    else:
        print('We are working with integrating with different file format. : D')

    # Insert data to BQ {TABLE_ID} table
    