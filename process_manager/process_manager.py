import pika
import pkgutil
import importlib
import threading
import json

import time
from os import walk, rename
from os.path import join

class ProcessManager:

    def __init__(self):
        self.setup_processors()
        self.watch_input_directory()
        self.rabbitMQConnection()

    def setup_processors(self):
        print("Setting up processors")
        self.processors = []
        processors_module_name = 'processors'
        list_processors_name = 'test'
        for p in pkgutil.walk_packages([processors_module_name]):
            self.processors.append(importlib.import_module(processors_module_name + '.' + p.name))

    def rabbitMQConnection(self):
        print("Setting up rabbitmq connection")
        connetion = pika.BlockingConnection(
            pika.ConnectionParameters(host='localhost')
        )
        channel = connetion.channel()
        channel.queue_declare(queue="events")
        channel.basic_consume(queue="events", on_message_callback=self.handle_event, auto_ack=True)
        channel.start_consuming()


    def handle_event(self, ch, method, properties, body):
        print(" [x]  Received %r" % json.loads(body))
        
    def watch_input_directory(self):
        watch_thread = threading.Thread(target=watch_input_directory)
        watch_thread.start()


def watch_input_directory():
    DIRECTORY_TO_WATCH = "./input"
    DIRECTORY_TO_MOVE_TO = "./processing"
    while True:
        time.sleep(2)
        for root, dirs, filenames in walk(DIRECTORY_TO_WATCH):
            for name in filenames:
                if (name[0] != '.'):
                    print(name)
                    rename(join(root, name), join(DIRECTORY_TO_MOVE_TO, name))
                    message = {
                        "path": join(DIRECTORY_TO_MOVE_TO, name)
                    }
                    sendToQueue(message)


def sendToQueue(message):
    connection = pika.BlockingConnection(
        pika.ConnectionParameters(host='localhost')
    )
    channel = connection.channel()
    channel.queue_declare(queue='events')
    channel.basic_publish(exchange='', routing_key='events', body=json.dumps(message))
                
        


