import functions_framework
import requests
from google.cloud import asset_v1
from google.auth import default

# Triggered from a message on a Cloud Pub/Sub topic.
@functions_framework.cloud_event
def update_cai(cloud_event):
  _, project_id = default()

  # Set Dataset and table to export data to BigQuery
  dataset_name = "cai_dataset" # Replace accordingly
  table_name = "cai_table" # Replace accordingly

  # Connect to clients
  asset_client = asset_v1.AssetServiceClient()

  # Export CAI to BigQuery
  parent = f"projects/{project_id}"
  output_config = asset_v1.OutputConfig()
  output_config.bigquery_destination.dataset = f"{parent}/datasets/{dataset_name}"
  output_config.bigquery_destination.table = table_name
  output_config.bigquery_destination.force = True

  request=asset_v1.ExportAssetsRequest(
    parent =  parent,
    content_type =  "RESOURCE",
    asset_types = [".*.googleapis.com.*"],
    output_config = output_config
  )

  response = asset_client.export_assets(request=request)
  print(response.result())