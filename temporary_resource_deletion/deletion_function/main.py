import functions_framework
from google.auth import default
from google.cloud import storage
from google.cloud import tasks_v2

@functions_framework.http
def delete_bucket(request):

    _, project_id = default()
    # Create a storage client
    storage_client = storage.Client(project=project_id)
    # Create a Cloud Tasks client
    tasks_client = tasks_v2.CloudTasksClient()

    print(request)
    request_json = request.get_json(silent=True)
    request_args = request.args

    if request_json and 'name' in request_json:
        name = request_json['name']
        bucket = storage_client.get_bucket(name)
        if bucket.exists:
            bucket.delete()
    else:
        print("no bucket stated")
    print("Bucket deleted")

    location = request_json['location']
    queue_id = request_json['queue_id']
    task_id = request_json['task_id']

    # Initialize request argument(s)
    task_name=tasks_client.task_path(project_id, location, queue_id, task_id)
    request = tasks_v2.DeleteTaskRequest(
        name=task_name,
    )

    # Make the request
    response = tasks_client.delete_task(request=request)
    print(response)
    return "Success"
