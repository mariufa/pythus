import threading
import time
import uuid
import logging
from os import walk, remove, rename, getenv
from os.path import join, getsize

from multiprocessing import Process
import pika
import os

from utils.rabbitmq import sendEvent
from nifi.flow_file_read import read_flow_file_stream
from nifi.flow_file_write import write_flow_file_stream

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class FileHandler:

    INPUT_DIRECTORY = "./data/input"
    OUTPUT_DIRECTORY = "./data/output"
    PROCESSING_DIRECTORY = "./data/processing"

    def __init__(self):
        self.OUTPUT_ORIGINAL_FILE = getenv('OUTPUT_ORIGINAL_FILE', 'True')

    def watch_input_directory(self):
        #input_thread = threading.Thread(target=self.input_thread)
        #input_thread.start()
        input_thread = Process(target=self.input_thread)
        input_thread.start()


    def input_thread(self):
        host = os.getenv('RABBIT_HOST', '127.0.0.1')
        port = os.getenv('RABBIT_PORT', '5672')
        connection = pika.BlockingConnection(
            pika.ConnectionParameters(host=host, port=port, credentials=pika.credentials.PlainCredentials('guest', 'guest'))
        )
        channel = connection.channel()
        
        while True:
            time.sleep(1)
            for root, dirs, filenames in walk(self.INPUT_DIRECTORY):
                for name in filenames:
                    if (name[0] != '.'):
                        queue = channel.queue_declare(queue="events", durable=True, exclusive=False, auto_delete=False, passive=True)
                        logger.info(queue.method.message_count)

                        # More than 5 file will be picked up. Rabbitmq updates queue count to slow.
                        while (queue.method.message_count >= 15):
                            queue = channel.queue_declare(queue="events", durable=True, exclusive=False, auto_delete=False, passive=True)
                            time.sleep(1)
                        self.handle_input_file(root, name)
                        logger.info("Picked up file: " + name)
    
    def handle_input_file(self, root, name):
        with open(join(root, name), 'rb') as f:
            for attrs, offset, size in read_flow_file_stream(f):
                f.seek(offset)
                new_file_name = join(self.PROCESSING_DIRECTORY, str(uuid.uuid4()))
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
                if "identifier" in attrs:
                    identifier = attrs["identifier"]

                message = {
                    "identifier": identifier,
                    "path": new_file_name,
                    "filename" : attrs["filename"],
                    "filetype": "unknown",
                    "history": [],
                    "metadata": {},
                    "original_file": True
                }

                sendEvent(message)
                remove(join(root, name))

    def handle_output_file(self, message):
        if self.OUTPUT_ORIGINAL_FILE == 'True' or (self.OUTPUT_ORIGINAL_FILE == 'False' and message["original_file"] == False):
            attrs = self.generate_attrs(message)
            logger.info(" [x]  Processed %r" % message)
            tmp_output_filename = join(self.OUTPUT_DIRECTORY, "." + message["identifier"])
            output_filename = join(self.OUTPUT_DIRECTORY, message["identifier"])
            with open(message["path"], 'rb') as done_file:
                file_size  = getsize(message["path"])
                with open(tmp_output_filename, 'wb') as output_file:
                    write_flow_file_stream(output_file, attrs, file_size, done_file)
            rename(tmp_output_filename, output_filename)
        remove(message["path"])

    def generate_attrs(self, message):
        attrs = {}
        attrs["filename"] = message["filename"]
        attrs["filetype"] = message["filetype"]
        attrs["identifier"] = message["identifier"]
        
        if "parent" in message:
            attrs["parent"] = message["parent"]
        
        if "metadata" in message:
            attrs["metadata"] = message["metadata"]
        
        return attrs

