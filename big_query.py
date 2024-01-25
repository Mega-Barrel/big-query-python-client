""" Big Query class for handling table CURD operations"""

# Internal packages
import os
import json

# Installed packages
import pandas as pd
from dotenv import load_dotenv
from google.cloud import bigquery   #pylint: disable=E0401
from google.oauth2 import service_account   #pylint: disable=E0401

# Exceptions import
from google.cloud.exceptions import NotFound    #pylint: disable=E0401
from google.api_core.exceptions import BadRequest   #pylint: disable=E0401

class BigQueryOperations:
    """Encapsulates operations for interacting with BigQuery tables."""

    def __init__(self) -> None:
        """Initializes the BigQuery client and dataset ID.

        Args:
            project_id (str): The Google Cloud project ID.
            dataset_id (str): The BigQuery dataset ID.
            credentials_path (str): The path to the service account JSON file.
        """
        # load .env file
        load_dotenv()
        # Load environment variables
        self._project_id = os.environ.get('project_id')
        self._dataset_id = os.environ.get('dataset_id')
        self.credentials = service_account.Credentials.from_service_account_file(
            'bq_service_account.json'
        )
        # Creating bigquery client
        self._client = bigquery.Client(
            credentials=self.credentials,
            project=self._project_id
        )
        # Create empty list
        self._schema = []

    def table_exists(self, table_name: str):
        """Checks if a table exists in the dataset.

        Args:
            table_name (str): The name of the table to check.

        Returns:
            bool: True if the table exists, False otherwise.
        """
        # Set table_id to the ID of the table to determine existence.
        # table_id = "your-project.your_dataset.your_table"
        table_id = f'{self._project_id}.{self._dataset_id}.{table_name}'
        try:
            self._client.get_table(table_id)    # Make an API request.
            print(f'Table {table_id} already exists.')
            return False
        except NotFound:
            print(f'Table `{table_name}` does not exists.')
            return True

    def schema_helper(self, schema: dict) -> None:
        """Parses a JSON string representing a schema and returns a list of BigQuery SchemaField objects.
        Supports additional attributes like "mode" and "description" for columns.

        Args:
            schema (dict): A JSON string representing the schema.

        Returns:
            list: A list of BigQuery SchemaField objects.
        """
        with open(schema, encoding='utf-8') as json_data:
            data = json.load(json_data)
            for name, field_info in data.items():
                field_type = field_info.get("type")
                description = field_info.get("description")
                # Append data to schema list
                self._schema.append(
                    bigquery.SchemaField(
                        name=name,
                        field_type=field_type,
                        description=description
                    )
                )

    def create_table(self, df: pd.DataFrame, table_name: str, schema=None):
        """Creates a new table in the specified dataset.

        Args:
            df (DataFrame): Pandas DataFrame.
            table_name (str): The name of the table to create.
            schema [list] (Optional): A list of BigQuery schema fields for the table.
        """
        # define table_id
        table_id = f'{self._project_id}.{self._dataset_id}.{table_name}'
        # Check if table exists, if not create table with defined schema
        if self.table_exists(table_name):
            if schema is None:
                print(f"Creating table: {table_name}, with schema defined in pandas dataframe.")
                # Test
                job = self._client.load_table_from_dataframe(
                    df, table_id
                )
                try:
                    job.result()
                    print(f'Table: {table_name} created and data inserted successfully.')
                except BadRequest as error:
                    for error in job.errors:
                        print(f'ERROR: {error["message"]}')
            else:
                print('Calling Schema_helper function.')
                self.schema_helper(schema)
                print(f'Creating table: {table_name}, with custom schema specified.')
                # Table with custom schema
                job_config = bigquery.LoadJobConfig(schema=self._schema)
                job = self._client.load_table_from_dataframe(
                    df, table_id, job_config=job_config
                )
                try:
                    job.result()
                    print(f'Table: {table_name} created and data inserted successfully.')
                except BadRequest as error:
                    for error in job.errors:
                        print(f'ERROR: {error["message"]}')

    def delete_table(self, table_name: str) -> None:
        """Deletes a table from the dataset.

        Args:
            table_name (str): The name of the table to delete.
        """
        # define table_id
        table_id = f'{self._project_id}.{self._dataset_id}.{table_name}'
        self._client.delete_table(table_id, not_found_ok=True)  # Make an API request.
        print(f"Deleted table '{table_name}'.")
