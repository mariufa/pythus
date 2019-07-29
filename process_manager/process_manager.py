import pika
import pkgutil
import importlib
import threading
import json
from utils.rabbitmq import sendEvent
import uuid
import time
import logging
import os

from process_manager.file_handler import FileHandler

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class ProcessManager:

    def __init__(self):
        self.setup_processors()
        self.file_handler = FileHandler()
        self.file_handler.watch_input_directory()
        self.rabbitMQConnection()

    def setup_processors(self):
        logger.info("Setting up processors")
        self.processors = []
        processors_module_name = 'processors'
        for p in pkgutil.walk_packages([processors_module_name]):
            self.processors.append(importlib.import_module(processors_module_name + '.' + p.name))
            logger.info("Importing processor: %r" % p.name)

    def rabbitMQConnection(self):
        logger.info("Setting up rabbitmq connection")

        host = os.getenv('RABBIT_HOST', 'localhost')
        port = os.getenv('RABBIT_PORT', '5672')
        connetion = pika.BlockingConnection(
            pika.ConnectionParameters(host=host, port=port)
        )
        channel = connetion.channel()
        channel.queue_declare(queue="events")
        channel.basic_consume(queue="events", on_message_callback=self.handle_event, auto_ack=True)
        channel.start_consuming()


    def handle_event(self, ch, method, properties, body):
        message = json.loads(body)

        filetype = message["filetype"]
        history = message["history"]
        processor_to_start = None
        for processor in self.processors:
            if (filetype in processor.get_mime_types() or processor.get_mime_types()[0] == "*") and  processor.__name__ not in history:
                processor_to_start = processor
                break

        if processor_to_start != None:
            message["history"].append(processor_to_start.__name__)
            processor_thread = threading.Thread(target=processor.run, args=(message,))
            processor_thread.start()
        else:
            self.file_handler.handle_output_file(message)

