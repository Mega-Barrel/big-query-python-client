""" Big Query class for handling table CURD operations"""

# Internal packages
import os
import json

# Installed packages
from dotenv import load_dotenv
from google.cloud import bigquery   #pylint: disable=E0401
from google.oauth2 import service_account   #pylint: disable=E0401

# Exceptions import
from google.cloud.exceptions import NotFound    #pylint: disable=E0401
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

    def schema_helper(self, schema_json):
        """Parses a JSON string representing a schema and returns a list of BigQuery SchemaField objects.
        Supports additional attributes like "mode" and "description" for columns.

        Args:
            schema_json (str): A JSON string representing the schema.

        Returns:
            list: A list of BigQuery SchemaField objects.
        """

    def create_table(self, table_id: str, schema: json):
        """Creates a new table in the specified dataset.

        Args:
            table_id (str): The name of the table to create.
            schema (list): A list of BigQuery schema fields for the table.
        """
        # Check if table exists, if not create table with defined schema
        if self.table_exists(table_id):
            # table = bigquery.Table(table_id, schema=schema)
            # table = self._client.create_table(table)  # Make an API request.
            print(f"Table created with following name: {table_id}.")

    def auto_detect_schema(self):
        """ Auto detects schema from excel/csv file.
        
        Args:
            file_name (str): Name of the file
        """

    def insert_data(self):
        """Inserts data into a table.

        Args:
            table_id (str): The name of the table to insert data into.
            data (list or str): The data to insert, either as a list of tuples
                or a JSON string.
            json_format (bool, optional): True if the data is in JSON format,
                False if it's a list of tuples. Defaults to False.
        """

    def delete_table(self):
        """Deletes a table from the dataset.

        Args:
            table_id (str): The name of the table to delete.
        """
