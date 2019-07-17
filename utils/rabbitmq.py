import pika
import json
import os

def sendEvent(message):
    host = os.getenv('RABBIT_HOST', 'localhost')
    port = os.getenv('RABBIT_PORT', '5672')
    connection = pika.BlockingConnection(
        pika.ConnectionParameters(host=host, port=port)
    )
    channel = connection.channel()
    channel.queue_declare(queue='events')
    channel.basic_publish(exchange='', routing_key='events', body=json.dumps(message))