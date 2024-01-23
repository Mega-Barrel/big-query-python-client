""" Main file to insert data to BQ"""
import os

from dotenv import load_dotenv
from big_query import BigQueryOperations

if __name__ == '__main__':
    load_dotenv()

    PROJECT_ID = os.environ.get('project_id')
    DATASET_ID = os.environ.get('dataset_id')
    CREDS_FILE = '/bq_service_account.json'
    bq = BigQueryOperations(
        project_id=PROJECT_ID,
        dataset_id=DATASET_ID,
        credentials_path=CREDS_FILE
    )

    TABLE_ID = 'test_table'
    TABLE_SCHEMA = '/table_schema/zwya_transactions.json'

    is_exists = bq.table_exists(table_id=TABLE_ID)
    if is_exists:
        print(f'Table {TABLE_ID} already exists in BQ project.')
    else:
        bq.create_table(table_id=TABLE_ID, schema=TABLE_SCHEMA)
