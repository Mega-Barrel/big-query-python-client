""" Big Query class for handling table CURD operations"""

import json
from google.cloud import bigquery #pylint: disable=E0401
from google.oauth2 import service_account #pylint: disable=E0401

class BigQueryOperations:
    """Encapsulates operations for interacting with BigQuery tables."""

    def __init__(self, project_id: str, dataset_id: str, credentials_path: str) -> None:
        """Initializes the BigQuery client and dataset ID.

        Args:
            project_id (str): The Google Cloud project ID.
            dataset_id (str): The BigQuery dataset ID.
            credentials_path (str): The path to the service account JSON file.
        """
        credentials = service_account.Credentials.from_service_account_file(credentials_path)
        self.client = bigquery.Client(credentials=credentials, project=project_id)
        self.dataset_id = dataset_id

    def auto_detect_schema(self):
        """ Auto detects schema from excel/csv file.
        
        Args:
            file_name (str): Name of the file
        """

    def create_table(self, table_id: str, schema: bigquery.SchemaField):
        """Creates a new table in the specified dataset.

        Args:
            table_id (str): The name of the table to create.
            schema (list): A list of BigQuery schema fields for the table.
        """
        # Construct table reference
        table_ref = self.client.dataset(self.dataset_id).table(table_id)
        table = bigquery.Table(table_ref, schema=schema)  # Create table object
        table = self.client.create_table(table)  # Create the table in BigQuery
        print(f"Table '{table_id}' created.")

    def schema_helper(self, schema_json):
        """Parses a JSON string representing a schema and returns a list of BigQuery SchemaField objects.
        Supports additional attributes like "mode" and "description" for columns.

        Args:
            schema_json (str): A JSON string representing the schema.

        Returns:
            list: A list of BigQuery SchemaField objects.
        """

        schema_dict = json.loads(schema_json)  # Load JSON into a dictionary
        schema = []
        for name, field_info in schema_dict.items():
            field_type = field_info.get("type")
            mode = field_info.get("mode")
            description = field_info.get("description")

            # Append data to schema list
            schema.append(
                bigquery.SchemaField(
                    name,
                    field_type,
                    mode=mode,
                    description=description
                )
            )
        return schema

    def table_exists(self, table_id: str):
        """Checks if a table exists in the dataset.

        Args:
            table_id (str): The name of the table to check.

        Returns:
            bool: True if the table exists, False otherwise.
        """
        # Construct table reference
        table_ref = self.client.dataset(self.dataset_id).table(table_id)
        return self.client.get_table(table_ref) is not None  # Check if table exists

    def insert_data(self, table_id: str, data: any, json_format=False):
        """Inserts data into a table.

        Args:
            table_id (str): The name of the table to insert data into.
            data (list or str): The data to insert, either as a list of tuples
                or a JSON string.
            json_format (bool, optional): True if the data is in JSON format,
                False if it's a list of tuples. Defaults to False.
        """
        # Construct table reference
        table_ref = self.client.dataset(self.dataset_id).table(table_id)
        if json_format:
            data = json.loads(data)  # Convert JSON string to Python data

        errors = self.client.insert_rows(table_ref, data)  # Insert data into table
        if not errors:
            print(f"Data inserted into table '{table_id}'.")
        else:
            print("Errors occurred during insertion:")
            print(errors)

    def delete_table(self, table_id: str):
        """Deletes a table from the dataset.

        Args:
            table_id (str): The name of the table to delete.
        """
        # Construct table reference
        table_ref = self.client.dataset(self.dataset_id).table(table_id)
        self.client.delete_table(table_ref)  # Delete the table
        print(f"Table {table_id}' deleted.")
