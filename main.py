""" Main file to insert data to BQ"""
import pandas as pd
from big_query import BigQueryOperations

if __name__ == '__main__':
    TABLE_ID = 'games2win_assignment_funnel_analysis'
    # TABLE_SCHEMA = 'table_schema/zwya_transactions.json'
    TABLE_FILE_PATH = 'table_schema/games2win_assignment_funnel_analysis.csv'

    # Create BigQueryOperations instance
    bq = BigQueryOperations()

    # Read pandas dataframe
    data_frame = pd.read_csv(
        TABLE_FILE_PATH
    )

    # Read data from file (with custom schema).
    # bq.create_table(
    #     df=data_frame,       # Pandas DataFrame
    #     table_name=TABLE_ID, # Table name
    #     schema=TABLE_SCHEMA  # Table schema
    # )

    # Read data from file.
    bq.create_table(
        df=data_frame,       # Pandas DataFrame
        table_name=TABLE_ID  # Table name
    )
