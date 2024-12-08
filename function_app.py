import azure.functions as func
import logging
from azure.cosmos import CosmosClient, PartitionKey

app = func.FunctionApp()

import os

COSMOS_CONNECTION_STRING = os.getenv("COSMOS_CONNECTION_STRING")
SERVICE_BUS_CONNECTION_STRING = os.getenv("studentchatnotifications_SERVICEBUS")

DATABASE_NAME = "studentchatdb"
CONTAINER_NAME = "notifications"

cosmos_client = CosmosClient.from_connection_string(COSMOS_CONNECTION_STRING)

database = cosmos_client.create_database_if_not_exists(id=DATABASE_NAME)
container = database.create_container_if_not_exists(
    id=CONTAINER_NAME,
    partition_key=PartitionKey(path="/userId"),
    offer_throughput=400
)

@app.service_bus_queue_trigger(
    arg_name="azservicebus",
    queue_name="chatnotifications",
    connection="studentchatnotifications_SERVICEBUS"
)
def NotificationProcessor(azservicebus: func.ServiceBusMessage):
    try:
        message_body = azservicebus.get_body().decode('utf-8')
        logging.info(f"Processing message: {message_body}")
        
        notification = func.json.loads(message_body)
        
        container.upsert_item(notification)
        logging.info("Notification stored in Cosmos DB successfully.")
    except Exception as e:
        logging.error(f"Error processing message: {str(e)}")


@app.route(route="NotificationDispatcher", auth_level=func.AuthLevel.ANONYMOUS)
def NotificationDispatcher(req: func.HttpRequest) -> func.HttpResponse:
    try:
        user_id = req.params.get('userId')
        if not user_id:
            return func.HttpResponse(
                "Missing required parameter: userId",
                status_code=400
            )
        query = (
            f"SELECT * FROM Notifications n "
            f"WHERE n.userId = @userId "
            f"ORDER BY n._ts DESC"
        )
        parameters = [{"name": "@userId", "value": user_id}]
        notifications = list(container.query_items(
            query=query,
            parameters=parameters,
            enable_cross_partition_query=True
        ))

        notification_messages = [
            f'Your message "{notification["message"]}" received {notification["likes"]} likes.'
            for notification in notifications
        ]

        return func.HttpResponse(
            func.json.dumps(notification_messages),
            mimetype="application/json",
            status_code=200
        )
    except Exception as e:
        logging.error(f"Error retrieving notifications: {str(e)}")
        return func.HttpResponse(
            "An error occurred while retrieving notifications.",
            status_code=500
        )
