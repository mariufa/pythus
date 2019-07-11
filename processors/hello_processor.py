import pika

def run():
    print("hello");


def sendEvent():
    connection = pika.BlockingConnection(
        pika.ConnectionParameters(host='localhost')
    )
    channel = connection.channel()
    channel.queue_declare(queue='events')
    channel.basic_publish(exchange='', routing_key='events', body='Hello world')

if __name__ == "__main__":
    sendEvent()