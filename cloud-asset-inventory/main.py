import base64
import json
import functions_framework

from google.cloud import bigquery
from google.auth import default

# Recursive function to define schema
def append_data_to_schema(schema, data):
    # Define schema dynamically based on top-level data keys and nested data structure
    for key, value in data.items():
        if isinstance(value, list):
            # Handle list (adjust data type for elements as needed)
            schema.append(bigquery.SchemaField(key, 'STRING', mode='REPEATED'))
        elif isinstance(value, dict):
            # Handle nested data (adjust data types for nested fields)
            nested_schema = []
            append_data_to_schema(nested_schema, value)
            schema.append(bigquery.SchemaField(key, 'RECORD', fields=nested_schema))
        else:
            # Handle other data types (adjust as needed)
            schema_type = 'STRING'
            schema.append(bigquery.SchemaField(key, schema_type))
    return schema

# Triggered from a message on a Cloud Pub/Sub topic.
@functions_framework.cloud_event
def export_cai_to_bigquery(cloud_event):
    _, project_id = default()
    # Connect to BigQuery client
    client = bigquery.Client(project=project_id)

    # Decode message
    data_string = base64.b64decode(cloud_event.data["message"]["data"])
    data_string = data_string.decode('utf-8').strip("'")

    message_data = json.loads(data_string)
    message_body = message_data["asset"]
    print(f"message_body: {message_body}")

    # Extract fields for BigQuery schema
    schema = []
    schema = append_data_to_schema(schema, message_body)
    print(f"schema generated: {schema}")
    rows = tuple(message_body.values())
    
    # Retrieve API and Asset name from Asset type
    asset_api_name = message_data["asset"]["assetType"].split(".googleapis.com/",1)[0]
    asset_name = message_data["asset"]["assetType"].split(".googleapis.com/",1)[1]
    print(f"Processing data asset type: {asset_api_name}")

    # Seperate APIs into different datasets
    dataset_name = f"{asset_api_name}_cai_dataset"
    dataset_id = f"{project_id}.{dataset_name}"

    # Seperate assets into different tables
    table_name = f"{asset_name}_cai_table"
    table_id = f"{dataset_id}.{table_name}"

    # Create/Set Dataset and table to export data to BigQuery
    dataset = client.create_dataset(dataset_id, exists_ok=True)
    table = bigquery.Table(table_id, schema=schema)
    client.create_table(table, exists_ok=True)

    # Insert CAI data into BigQuery table
    errors = client.insert_rows(table, rows)
    
    if errors:
        print(f"Errors while inserting CAI data: {errors}")
    else:
        print("CAI data succesfully exported to big query")

    return {}