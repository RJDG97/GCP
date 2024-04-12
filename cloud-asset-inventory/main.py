import base64
import json
import functions_framework
import requests
from google.cloud import bigquery
from google.cloud import asset_v1
from google.auth import default

# Triggered from a message on a Cloud Pub/Sub topic.
@functions_framework.cloud_event
def update_cai(cloud_event):
  _, project_id = default()

  # Decode message
  data_string = base64.b64decode(cloud_event.data["message"]["data"])
  data_string = data_string.decode('utf-8').strip("'")
  message_data = json.loads(data_string)

  # Connect to clients
  asset_client = asset_v1.AssetServiceClient()
  bq_client = bigquery.Client(project=project_id)

  content_list = message_data['message']['content'] # "RESOURCE","RELATIONSHIP","IAM_POLICY","ORG_POLICY","ACCESS_POLICY","OS_INVENTORY"

  # Set Dataset and table to export data to BigQuery
  dataset_name = message_data['message']['dataset']
  dataset_id = f"{project_id}.{dataset_name}"
  dataset = bq_client.create_dataset(dataset_id, exists_ok=True)
  for content in content_list:
    table_name = content.lower() 
    print(table_name)

    # Export CAI to BigQuery
    parent = f"projects/{project_id}"
    output_config = asset_v1.OutputConfig()
    output_config.bigquery_destination.dataset = f"{parent}/datasets/{dataset_name}"
    output_config.bigquery_destination.table = table_name
    output_config.bigquery_destination.force = True
    # Overview export
    request=asset_v1.ExportAssetsRequest(
      parent =  parent,
      content_type = content,
      output_config = output_config
    )
    print(f"Exporting data of content type: {content}")
    response = asset_client.export_assets(request=request)
    print(response.result())
    print("Export complete")
    
    # By Asset
    output_config.bigquery_destination.separate_tables_per_asset_type = True
    request=asset_v1.ExportAssetsRequest(
      parent =  parent,
      content_type = content,
      output_config = output_config
    )
    print(f"Exporting data of content type [{content}] by table")
    response = asset_client.export_assets(request=request)
    print(response.result())
    print("Export complete")
