import azure.functions as func
import datetime
import json
import logging
from azure.storage.blob import BlobServiceClient
from azure.identity import DefaultAzureCredential
from urllib.parse import urlparse
from dotenv import load_dotenv
import os

load_dotenv()

app = func.FunctionApp()

# Initialize the BlobServiceClient once and reuse it
credential = DefaultAzureCredential()
account_url = os.getenv('StorageAccountUrl')
if not account_url:
    raise ValueError("StorageAccountUrl environment variable is not set")
blob_service_client = BlobServiceClient(account_url=account_url, credential=credential)

@app.route(route="ProcessRequest", auth_level=func.AuthLevel.ANONYMOUS)
def ProcessRequest(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Python HTTP trigger function processed a request.')

    name = req.params.get('name')
    if not name:
        try:
            req_body = req.get_json()
        except ValueError:
            pass
        else:
            name = req_body.get('name')

    if name:
        return func.HttpResponse(f"Hello, {name}. This HTTP triggered function executed successfully.")
    else:
        return func.HttpResponse(
             "This HTTP triggered function executed successfully. Pass a name in the query string or in the request body for a personalized response.",
             status_code=200
        )
    

@app.function_name(name="EventGridTrigger")
@app.event_grid_trigger(arg_name="event", event_type="Microsoft.Storage.BlobCreated")
def EventGridTrigger(event: func.EventGridEvent):
    logging.info('Python EventGrid trigger function processed an event.')

    result = {
        'id': event.id,
        'data': event.get_json(),
        'topic': event.topic,
        'subject': event.subject,
        'event_type': event.event_type,
    }

    logging.info(f'EventGrid event: {json.dumps(result)}')

    # Retrieve the blob URL from the event data
    event_data = event.get_json()
    blob_url = event_data.get('url')
    logging.info(f'Blob URL: {blob_url}')

    # Parse the blob URL for account URL, container name, and blob name
    parsed_url = urlparse(blob_url)
    account_url = f"{parsed_url.scheme}://{parsed_url.netloc}"
    path_parts = parsed_url.path.lstrip('/').split('/')
    container_name = path_parts[0]
    blob_name = '/'.join(path_parts[1:])

    try:
        # Read the contents of the blob
        blob_client = blob_service_client.get_blob_client(container=container_name, blob=blob_name)

    except Exception as e:
        logging.error(f'Error reading blob content: {str(e)}')
        return func.HttpResponse(f'Error reading blob content: {str(e)}', status_code=500)