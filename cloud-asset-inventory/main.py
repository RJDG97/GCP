import base64
import json
import functions_framework

# Triggered from a message on a Cloud Pub/Sub topic.
@functions_framework.cloud_event
def export_cai_to_bigquery(cloud_event):
    from google.cloud import bigquery
    from google.auth import default

    _, project_id = default()
    # Connect to BigQuery client (replace with your project ID)
    client = bigquery.Client(project=project_id)

    # Decode message
    data_string = base64.b64decode(cloud_event.data["message"]["data"])
    data_string = data_string.decode('utf-8').strip("'")
    # Format string to json, TODO: find out what to do with non-json messages
    try: 
        message_data = json.loads(data_string)
    except ValueError as e:
        return{}

    message_body = message_data["asset"]
    message_body['resource'] = json.dumps(message_body['resource'])

    # Extract fields for BigQuery schema
    schema = [
        bigquery.SchemaField('ancestors', 'STRING', mode='REPEATED'),  # List of ancestor resource names
        bigquery.SchemaField('assetType', 'STRING', mode='REQUIRED'),  # Type of the asset
        bigquery.SchemaField('name', 'STRING', mode='REQUIRED'),  # Full resource name of the asset
        bigquery.SchemaField('resource', 'STRING', mode='NULLABLE'),  # JSON string representing the resource details
        bigquery.SchemaField('updateTime', 'TIMESTAMP', mode='REQUIRED'),  # Last update time of the asset
    ]

    # Prepare data to be inserted using list comprehension
    rows_to_insert=[message_body]
    
    asset_api_name = message_data["asset"]["assetType"].split(".googleapis.com",1)[0]
    print(f"Processing data asset type: {asset_api_name}")
    dataset_id = f"{project_id}.cai_dataset"
    table_name = f"cai_{asset_api_name}_table"
    table_id = f"{dataset_id}.{table_name}"

    dataset = client.create_dataset(dataset_id, exists_ok=True)
    table = bigquery.Table(table_id, schema=schema)
    client.create_table(table, exists_ok=True)

    # Insert CAI data into BigQuery table
    errors = client.insert_rows(table, rows_to_insert)
    
    if errors:
        print(f"Errors while inserting CAI data: {errors}")
        
    print("CAI data exported to big query")
    return {}