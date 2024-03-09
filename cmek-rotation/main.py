import base64
import json
import functions_framework

# Triggered from a message on a Cloud Pub/Sub topic.
@functions_framework.cloud_event
def rotate_cmek(cloud_event): 
    # Import libraries
    import time
    import google.cloud.kms_v1 as kms
    import googleapiclient.discovery

    # Decode message
    data_string = base64.b64decode(cloud_event.data["message"]["data"])
    data_string = data_string.decode('utf-8').strip("'")
    message_data = json.loads(data_string)

    # Variables
    projectId = message_data['message']['project-id']
    location = message_data["message"]["location"]
    keyring = message_data["message"]["keyring"]
    cmek = message_data["message"]["cmek"]
    dummy_cmek = message_data["message"]["dummy_cmek"]
    bucket = message_data["message"]["bucket"]

    # Initialize clients
    kms_client = kms.KeyManagementServiceClient()    
    logging_client = googleapiclient.discovery.build('logging', 'v2')

    # Set key path
    key_path=f"projects/{projectId}/locations/{location}/keyRings/{keyring}/cryptoKeys/{cmek}"
    dummy_key_path=f"projects/{projectId}/locations/{location}/keyRings/{keyring}/cryptoKeys/{dummy_cmek}"
    
    # Create new version of key
    print( f"Beginning rotation of CMEK: [{cmek}]")
    new_key = kms_client.create_crypto_key_version(request={"parent": key_path})
    version_id = new_key.name.split('cryptoKeyVersions/',1)[1]
    # Set new version of key as primary
    new_primary = kms_client.update_crypto_key_primary_version(request={"name": key_path, "crypto_key_version_id": version_id})
    time.sleep(60)
    print( f"CMEK: [{cmek}] succesfully rotated")

    # Set log bucket to use the dummy key

    # Create the bucket update request body
    bucket_body = {
        "name": bucket,
        "cmekSettings": {
            "kmsKeyName": dummy_key_path
        }
    }

    # Execute the API request to update the log bucket
    request = logging_client.projects().locations().buckets().patch(
        name=f"projects/{projectId}/locations/{location}/buckets/{bucket}",
        body=bucket_body,
        updateMask="cmekSettings"  # Update only the CMEK key settings
    )
    response = request.execute()

    print(f"Log bucket updated successfully: {response}")
    
    # Set log bucket back to using the new version of main key
    #  Bucket name and CMEK key name (replace with appropriate values)
    # Create the bucket update request body
    bucket_body = {
        "name": bucket,
        "cmekSettings": {
            "kmsKeyName": new_primary.name
        }
    }

    # Execute the API request to update the log bucket
    request = logging_client.projects().locations().buckets().patch(
        name=f"projects/{projectId}/locations/{location}/buckets/{bucket}",
        body=bucket_body,
        updateMask="cmekSettings"  # Update only the CMEK key settings
    )
    response = request.execute()
    print(f"Log bucket updated successfully: {response}")
    return 'Log Buckets have been updated'
