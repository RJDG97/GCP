# Cloud Asset feed export to BigQuery script 

Within Google Cloud Platform, this is a guide to set up a cloud asset feed that uses pub/sub to trigger cloud function that exports feed data from cloud assets into bigquery.

![Architecture Diagram.](architecture.png)

## How it works
Cloud Scheduler triggers a cloud function through a pub/sub topic, which then calls the Cloud Asset API to export the data onto BigQuery (BigQuery dataset and table need to be created)

## Pre-requisites
 - Enable Cloud Asset API & Cloud Resource Manager
 - Create a pub/sub topic
 - Service Account with the following roles:
    - Cloud Run Invoker
    - BigQuery Admin
    - Logs Writer
    - Storage Object Admin
    - Artifact Registry Create-on-Push Writer
 - Create a BigQuery dataset and table
 - Cloud Scheduler that pushes to the created topic (frequency of update)
## Setup

### Cloud Function
Cloud function once triggered will run the script that exports data from cloud assets into bigquery.

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
    - Change the dataset_name and table_name in the code to the dataset and table previously created
    - Deploy

TBC
