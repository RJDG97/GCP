# Cloud Asset feed export to BigQuery script 

Within Google Cloud Platform, this is a guide to set up a cloud asset feed that uses pub/sub to trigger cloud function that exports feed data from cloud assets into bigquery.

## Pre-requisites
 - Enable Cloud Asset API & Cloud Resource Manager
 - Create a pub/sub topic
 - Service Account with the following roles:
    - Cloud Run Invoker

## Setup

### Cloud Function
Cloud function once triggered will run the script that exports feed data from cloud assets into bigquery.

 - Under Cloud Functions, create a new function
 - Set the following in the setup page
    #### Basics
    - Environment: 2nd gen
    - Function name: name
    - Region: region
    #### Trigger
    - Trigger type: Cloud Pub/Sub
    - Cloud Pub/Sub topic: Create or use your already created topic
    - More Options/Service account: Select the service account with the roles mentioned in the pre-requisites section (Requires Cloud Run Invoker to trigger function)
    Runtime, build, connections and security settings
    - Runtime service account: Select the service account with the roles mentioned in the pre-requisites section
    - Select Next
    #### Code
    - Runtime: Python 3.12
    - Replace the main and the requirements with the files in this repo
    - You may test the function with the sample_body.txt as a reference
    - Deploy

### Cloud Assets
Cloud Assets creates a feed that sends a message to Pub/Sub whenever the stated asset is created, updated, deleted.

 - Open Cloud Shell (currently this is the only way to create a feed)
 - Run the following shell scipt to create a feed
    - You can specify a specific asset type but ".*.googleapis.com.*" handles all APIs
    - Additionally, you can specify different content-types ([reference](https://cloud.google.com/asset-inventory/docs/overview#content_types))
    - [Reference Documentation](https://cloud.google.com/asset-inventory/docs/monitoring-asset-changes)
 ```shell
 gcloud asset feeds create FEED_NAME-<content-type> --project <project-id>  --pubsub-topic projects/<project-id> /topics/<topic-name> --asset-types ".*.googleapis.com.*" --content-type  <content-type>
 ```

## Code

### Dependencies
These libraries are used within the function for the following reasons

```python
import base64
import json
import functions_framework
from google.cloud import bigquery
from google.auth import default
```
| Library                   | Use case                                                 |
|---------------------------|----------------------------------------------------------|
| base64                    | decode message data                                      |
| json                      | format decoded data to json                              |
| base64                    | decode message data                                      |
| functions_framework       | retrieve the pub/sub message from cloud assets           |
| google.cloud.bigquery     | Access biqguery to create/insert dataset/table/data      |
| google.auth.default       | Get Project ID                                           |

### Initialize API Clients
Initialize with Application Default Credentials (ADC) and bigquery

```python
    # Initialize clients
    _, project_id = default()
    # Connect to BigQuery client
    client = bigquery.Client(project=project_id)
```

### Decoding data
cloud_event is passed into the function containing the message from the feed,
the following code decodes it back into a json that can be used.

```python
    # Decode message
    data_string = base64.b64decode(cloud_event.data["message"]["data"])
    data_string = data_string.decode('utf-8').strip("'")

    message_data = json.loads(data_string)
    message_body = message_data["asset"]
```

### Generate Schema and updated json that to complies with biguery
This section calls the function that generates the schema and new message body that complies to bigquery

```python
    # Extract fields for BigQuery schema
    schema = []
    schema, data = append_data_to_schema(schema, message_body)
    print(f"Log: {asset_api_name}.{asset_name} schema generated: {schema}")
    print(f"Log: {asset_api_name}.{asset_name} updated_body: {data}")
    rows = tuple(data.values())
```

### Recursively go through the json to generate a new schema and json body
The Function is made recursive to allow nested schema to be generated from nested jsons.
A new json is also generated as the message needs to be altered to comply with big query as it does not accept certain special characters.

```python
# Recursive function to define schema
def append_data_to_schema(schema, data):
    # New json to support the bq table names
    new_json={}
    # Define schema dynamically based on top-level data keys and nested data structure
    for key, value in data.items():
        key = key.replace('.', '-')
        key = key.replace('/', '_')

        if isinstance(value, bool):
            # Handle list (adjust data type for elements as needed)
            schema.append(bigquery.SchemaField(key, 'BOOL'))
            new_json[key] = value
        elif isinstance(value, list):
            # Handle list (adjust data type for elements as needed)
            schema.append(bigquery.SchemaField(key, 'STRING', mode='REPEATED'))
            new_json[key] = value
        elif isinstance(value, dict):
            # Handle nested data (adjust data types for nested fields)
            nested_schema = []
            _, json = append_data_to_schema(nested_schema, value)
            schema.append(bigquery.SchemaField(key, 'RECORD', fields=nested_schema))
            new_json[key] = json
        else:
            # Handle other data types (adjust as needed)
            schema_type = 'STRING'
            schema.append(bigquery.SchemaField(key, schema_type))
            new_json[key] = value

    return schema, new_json
```

### Get/Create BigQuery datasets and tables for the data to be imported to
Creates a dataset and table if not already available

```python
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
```
### Import data into BigQuery
Inserts data into BigQuery

```python
    if errors:
        print(f"ERROR: while inserting {asset_api_name}.{asset_name} CAI data: {errors}")
    else:
        print(f"SUCCESS: {asset_api_name}.{asset_name} CAI data succesfully exported to big query")
```
