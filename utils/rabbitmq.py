import pika
import json
import os

def sendEvent(message, priority=1):
    host = os.getenv('RABBIT_HOST', '127.0.0.1')
    port = os.getenv('RABBIT_PORT', '5672')
    connection = pika.BlockingConnection(
        pika.ConnectionParameters(host=host, port=port)
    )
    channel = connection.channel()
    queue = channel.queue_declare(queue="events",durable=True, exclusive=False, auto_delete=False, passive=True)
    channel.basic_publish(properties=pika.BasicProperties(priority=priority),exchange='', routing_key='events', body=json.dumps(message))