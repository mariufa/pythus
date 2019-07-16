import pika
import pkgutil
import importlib
import threading
import json
from nifi.flow_file_read import *
from nifi.flow_file_write import *
from utils.rabbitmq import sendEvent
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
        print(threading.active_count())

    def setup_processors(self):
        print("Setting up processors")
        self.processors = []
        processors_module_name = 'processors'
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
        print(threading.active_count())

        filetype = message["filetype"]
        history = message["history"]
        processor_to_start = None
        for processor in self.processors:
            if (filetype in processor.get_mime_types() or processor.get_mime_types()[0] == "*") and  processor.__name__ not in history:
                processor_to_start = processor
                break

        if processor_to_start != None:
            if threading.active_count() <= 5:
                message["history"].append(processor_to_start.__name__)
                print(processor_to_start.__name__)
                processor_thread = threading.Thread(target=processor.run, args=(message,))
                processor_thread.start()
            else:
                print("Too many active threads")
                time.sleep(2)
                sendEvent(message)
        else:
            output_dir = "./data/output"
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
        attrs["identifier"] = message["identifier"]
        
        if message["parent"]:
            attrs["parent"] = message["parent"]
        
        if 'metadata' in message:
            attrs["metadata"] = message["metadata"]
        
        return attrs
        
    def watch_input_directory(self):
        watch_thread = threading.Thread(target=watch_input_directory)
        watch_thread.start()


def watch_input_directory():
    DIRECTORY_TO_WATCH = "./data/input"
    DIRECTORY_TO_MOVE_TO = "./data/processing"
    while True:
        time.sleep(2)
        for root, dirs, filenames in walk(DIRECTORY_TO_WATCH):
            for name in filenames:
                if (name[0] != '.'):
                    print(name)

                    with open(join(root, name), 'rb') as f:
                        for attrs, offset, size in read_flow_file_stream(f):
                            f.seek(offset)
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

                            identifier = str(uuid.uuid4())
                            if attrs["identifier"]:
                                identifier = attrs["identifier"]

                            message = {
                                "identifier": identifier,
                                "path": new_file_name,
                                "filename" : attrs["filename"],
                                "filetype": "unknown",
                                "nifi_attrs": attrs,
                                "history": [],
                                "metadata": {}
                            }

                            print(message)
                            sendEvent(message)
                            remove(join(root, name))

