import functions_framework

@functions_framework.http
def rotate_cmek(request):
    import subprocess
    import time
 
    # Import libraries
    import google
    import google.cloud.kms_v1 as kms
    import googleapiclient.discovery

    Variables
    if request.method == 'POST':
        data = request.get_json()
        projectId = data.get('project-id')
        location = data.get('location')
        keyring = data.get('keyring')
        cmek = data.get('cmek')
        dummy_cmek = data.get('dummy_cmek')
        bucket = data.get('bucket')
    else:
        return 'Method not allowed'

    # Initialize clients
    
    # Authentication using Application Default Credentials (ADC)
    # Ensure your environment is set up for ADC before running this script.
    credentials, project = google.auth.default()

    # Create a Cloud Logging API client
    logging_client = googleapiclient.discovery.build('logging', 'v2')
    kms_client = kms.KeyManagementServiceClient()

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
