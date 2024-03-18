# Cloud Asset feed export to BigQuery script 

Within Google Cloud Platform, this is a guide to set up a cloud asset feed that uses pub/sub to trigger cloud function that exports feed data from cloud assets into bigquery.

![Architecture Diagram.](CAI_Export_Architecture.png)

## How it works
The script triggers when Cloud Assets Inventory feed sends a message to the pub/sub topic that the Cloud Function containing the script is subscribed to. 

From there it decodes the message and processes the data by removing [unsupported characters](https://cloud.google.com/bigquery/docs/schemas) and omitting empty json files as they are not supported in BigQuery

It then generates a schema from the data that is used when inserting the data into BigQuery. 

If the defined dataset or table within the script is not available, it will create them (This may result in an initial error when creating as it will try to insert the data into a non-existant dataset/table).

Lastly, it inserts the data into the table

## Pre-requisites
 - Enable Cloud Asset API & Cloud Resource Manager
 - Create a pub/sub topic
 - Service Account with the following roles:
    - Cloud Run Invoker
    - BigQuery Admin
    - Logs Writer
    - Storage Object Admin
    - Artifact Registry Create-on-Push Writer

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

### Looker Studio Dashboard setup
