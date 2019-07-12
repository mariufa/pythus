import pika
import json

def get_mime_types():
    return [
        'plain/text'
    ]

def run(message):
    print("running")
    sendEvent(message)



def sendEvent(message):
    connection = pika.BlockingConnection(
        pika.ConnectionParameters(host='localhost')
    )
    channel = connection.channel()
    channel.queue_declare(queue='events')
    channel.basic_publish(exchange='', routing_key='events', body=json.dumps(message))
