import base64
import json
import functions_framework

from datetime import datetime
from google.cloud import bigquery
from google.auth import default

def modify_json_for_bq(nested_json):
    if isinstance(nested_json, dict):
        if nested_json == {}:
            return
        # Handle dictionaries:
        modified_dict = {}
        for key, value in nested_json.items():
            # Remove unsupported characters
            key = key.replace('.', '-')
            key = key.replace('/', '_')
            new_value = modify_json_for_bq(value)
            # Omit empty json, BQ does not support this
            if new_value is not None:
                modified_dict[key] = new_value
            else:
                print(f"Warning: Empty json [{key}] found, removing to comply with BigQuery")
        return modified_dict
    elif isinstance(nested_json, list):
        # Handle lists:
        return [modify_json_for_bq(item) for item in nested_json]
    else:
        # Handle other data types (no modification needed):
        return nested_json
    
def get_key_type(value):
    if isinstance(value, bool):
        return 'BOOL'
    elif isinstance(value, int):
        return 'INTEGER'
    elif isinstance(value, float):
        return 'FLOAT64'
    elif isinstance(value, list):
        return 'LIST'
    elif isinstance(value, dict):
        if "type" in value and "coordinates" in value:
            return 'GEOGRAPHY'
        else:
            return 'RECORD'
    else:
        try:
            datetime.fromisoformat(value)
            return 'TIMESTAMP'
        except:
            return 'STRING'

# Recursive function to define schema
def generate_schema_from_json(data):
    schema = []
    # Define schema dynamically based on top-level data keys and nested data structure
    for key, value in data.items():
        key_type = get_key_type(value)
        key_mode = 'NULLABLE'
        nested_schema = []
        if key_type == 'LIST':
            # Set mode as Repeated to account for list
            key_mode='REPEATED'
            if len(value[0]) == 0:
                key_type = 'STRING'
            else:
                key_type = get_key_type(value[0])
            # Handle list
            if key_type == 'RECORD':
                nested_schema = generate_schema_from_json(value[0])
        elif key_type == 'RECORD':
            # Handle nested data (adjust data types for nested fields)
            nested_schema = generate_schema_from_json(value)
        if len(nested_schema) == 0:
            schema.append(bigquery.SchemaField(key, key_type, mode=key_mode))
        else:
            schema.append(bigquery.SchemaField(key, key_type, fields=nested_schema, mode=key_mode))
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

    # Retrieve API and Asset name from Asset type
    asset_api_name = message_body["assetType"].split(".googleapis.com/",1)[0]
    asset_name = message_body["assetType"].split(".googleapis.com/",1)[1]

    print(f"Log: [{asset_api_name}.{asset_name}] Processing data asset type: {asset_api_name}")
    print(f"Log: [{asset_api_name}.{asset_name}] message_body: {message_body}")
    message_body = modify_json_for_bq(message_body)
    print(f"Log: [{asset_api_name}.{asset_name}] modified_message_body: {message_body}")
    # Extract fields for BigQuery schema
    schema = generate_schema_from_json(message_body)
    print(f"Log: [{asset_api_name}.{asset_name}] schema generated: {schema}")
    print(f"Log: [{asset_api_name}.{asset_name}] updated_body: {message_body}")
    rows = tuple(message_body.values())

    # Seperate APIs into different datasets
    dataset_name = f"cai_dataset_{asset_api_name}" # Replace accordingly
    dataset_id = f"{project_id}.{dataset_name}"

    # Seperate assets into different tables
    table_name = f"cai_{asset_api_name}_{asset_name}" # Replace accordingly
    table_id = f"{dataset_id}.{table_name}"

    # Create/Set Dataset and table to export data to BigQuery
    dataset = client.create_dataset(dataset_id, exists_ok=True)
    table = bigquery.Table(table_id, schema=schema)
    client.create_table(table, exists_ok=True)

    # Insert CAI data into BigQuery table
    errors = client.insert_rows(table, [rows])
    
    if errors:
        print(f"ERROR: while inserting {asset_api_name}.{asset_name} CAI data: {errors}")
    else:
        print(f"SUCCESS: {asset_api_name}.{asset_name} CAI data succesfully exported to big query")

    return {}