import pika
import pkgutil
import importlib
import threading
import json
from nifi.flow_file_read import *
from nifi.flow_file_write import *
import uuid

import time
from os import walk, rename, remove
from os.path import join, getsize
import io

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
        message = json.loads(body)
        print(" [x]  Received %r" % message)

        filetype = message["filetype"]
        history = message["history"]
        processor_to_start = None
        for processor in self.processors:
            if (filetype in processor.get_mime_types() or processor.get_mime_types()[0] == "*") and  processor.__name__ not in history:
                processor_to_start = processor
                break

        if processor_to_start != None:
            message["history"].append(processor_to_start.__name__)
            print(processor_to_start.__name__)
            processor_thread = threading.Thread(target=processor.run, args=(message,))
            processor_thread.start()
        else:
            output_dir = "./output"
            attrs = self.generate_attrs(message)
            with open(message["path"], 'rb') as done_file:
                file_size  = getsize(message["path"])
                with open(join(output_dir, str(uuid.uuid4())), 'wb') as output_file:
                    write_flow_file_stream(output_file, attrs, file_size, done_file)
            remove(message["path"])
            

    def generate_attrs(self, message):
        attrs = {}
        attrs["filename"] = message["filename"]
        attrs["filetype"] = message["filetype"]
        if 'metadata' in message:
            attrs["metadata"] = message["metadata"]
        return attrs
        
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

                    with open(join(root, name), 'rb') as f:
                        for attrs, offset, size in read_flow_file_stream(f):
                            f.seek(offset)
                            #data = f.read(size)
                            new_file_name = join(DIRECTORY_TO_MOVE_TO, str(uuid.uuid4()))
                            with open(new_file_name, 'wb') as new_f:
                                chunk_size = 4096
                                if size < chunk_size:
                                    chunk_size = size
                                chunk = f.read(chunk_size)
                                while chunk:
                                    new_f.write(chunk)
                                    size = size - chunk_size
                                    if size == 0:
                                        break
                                    elif size < chunk_size:
                                        chunk_size = size
                                    chunk = f.read(chunk_size)

                            message = {
                                "path": new_file_name,
                                "filename" : attrs["filename"],
                                "filetype": "plain/text",
                                "nifi_attrs": attrs,
                                "history": [],
                                "metadata": {}
                            }

                            print(message)
                            sendToQueue(message)
                            remove(join(root, name))

def sendToQueue(message):
    connection = pika.BlockingConnection(
        pika.ConnectionParameters(host='localhost')
    )
    channel = connection.channel()
    channel.queue_declare(queue='events')
    channel.basic_publish(exchange='', routing_key='events', body=json.dumps(message))
                
        


