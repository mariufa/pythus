import pika
import pkgutil
import importlib
import threading

class ProcessManager:

    def __init__(self):
        self.setup_processors()
        self.watch_input_directory()
        self.rabbitMQConnection()

    def setup_processors(self):
        self.processors = []
        processors_module_name = 'processors'
        list_processors_name = 'test'
        for p in pkgutil.walk_packages([processors_module_name]):
            self.processors.append(importlib.import_module(processors_module_name + '.' + p.name))

    def rabbitMQConnection(self):
        connetion = pika.BlockingConnection(
            pika.ConnectionParameters(host='localhost')
        )
        channel = connetion.channel()
        channel.queue_declare(queue="events")


    def handle_event(self, ch, method, properties, body):
        print(" [x]  Received %r" % body)
        
    def watch_input_directory(self):
        pass