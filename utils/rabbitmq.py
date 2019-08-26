import pika
import json
import os

def sendEvent(message, priority=1):
    host = os.getenv('RABBIT_HOST', 'localhost')
    port = os.getenv('RABBIT_PORT', '5672')
    connection = pika.BlockingConnection(
        pika.ConnectionParameters(host=host, port=port)
    )
    channel = connection.channel()
    args = {"x-max-priority": 2}
    channel.queue_declare(queue="events", arguments=args)
    channel.basic_publish(properties=pika.BasicProperties(priority=priority),exchange='', routing_key='events', body=json.dumps(message))