import base64
import datetime
import json
import functions_framework
from google.auth import default
from google.cloud import storage
from google.cloud import tasks_v2

# Triggered from a message on a Cloud Pub/Sub topic.
def create_bucket(project_id, client, bucket_name):
  # Check if bucket already exists
  bucket = client.bucket(bucket_name)
  exists = bucket.exists()

  if exists:
    print(f"Bucket {bucket_name} already exists.")
  else:
    # Create the bucket
    bucket = client.create_bucket(bucket_name)
    print(f"Bucket {bucket_name} created successfully.")

def schedule_deletion(project_id, bucket_name):
  location="us-central1"
  queue_id="test-queue"
  bucket_name = f"{project_id}-bastion-test-bucket"
  task_id = f"{bucket_name}_deletion"

  # Create a Cloud Tasks client
  client = tasks_v2.CloudTasksClient()

  # Create a Cloud Tasks Queue
  # Use the client to send a CreateQueueRequest.
  queue=tasks_v2.Queue(name=client.queue_path(project_id, location, queue_id))
  try:
    queue_list = client.get_queue(name=queue.name)
  except:
    print("Creating queue")
    response = client.create_queue(
        tasks_v2.CreateQueueRequest(
          parent=client.common_location_path(project_id, location),
          queue=tasks_v2.Queue(name=client.queue_path(project_id, location, queue_id)),
        )
      )
    print("Queue created")

  payload = {
    "name": bucket_name,
    "location": "us-central1",
    "queue_id": queue_id,
    "task_id": task_id
  }
  payload_bytes = json.dumps(payload).encode("utf-8")

  # Construct the task.
  task = tasks_v2.Task(
    http_request=tasks_v2.HttpRequest(
      http_method=tasks_v2.HttpMethod.POST,
      # Replace this url with the cloud function url for ssh deletion
      url="https://us-central1-renzo-intern-lab.cloudfunctions.net/delete-bastion-key",
      headers = {
        'Content-Type': 'application/json',
      },
      oidc_token = {
        'service_account_email': "bastion-test@renzo-intern-lab.iam.gserviceaccount.com"
      },
      body=json.dumps(payload).encode(),
    ),
    name=(
      client.task_path(project_id, location, queue_id, task_id)
      if task_id is not None
      else None
    ),
  )

  # Set the scheduled time (5 hours from now)
  timestamp = datetime.datetime.utcnow() + datetime.timedelta(minutes=5)
  task.schedule_time = timestamp

  # Create the task in the queue
  client.create_task(
    tasks_v2.CreateTaskRequest(
      # The queue to add the task to
      parent=queue.name,
      # The task itself
      task=task,
    )
  )

  return "Buckets created and deletion task scheduled."

# Triggered from a message on a Cloud Pub/Sub topic.
@functions_framework.cloud_event
def main(cloud_event):
  credentials, project_id = default()

  # Create a storage client
  client = storage.Client(project=project_id)

  bucket_name = f"{project_id}-bastion-test-bucket"
  create_bucket(project_id, client, bucket_name)
  schedule_deletion(project_id, bucket_name)
  return "Success"