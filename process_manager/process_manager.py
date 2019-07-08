import pika
import processors

class ProcessManager:

    def __init__(self):
        self.rabbitMQConnection()

    def setup_processors(self):
        self.processors = []
        for p in processors.iter:
            print(p)

    def rabbitMQConnection(self):
        connetion = pika.BlockingConnection(
            pika.ConnectionParameters(host='localhost')
        )
        channel = connetion.channel()
        channel.queue_declare(queue="events")


    def handle_event(self, ch, method, properties, body):
        print(" [x]  Received %r" % body)
        