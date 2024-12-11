import azure.functions as func
import datetime
import json
import logging

app = func.FunctionApp()


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