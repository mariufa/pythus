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
import time
from multiprocessing import Pool

from process_manager.file_handler import FileHandler

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
logging.getLogger("pika").setLevel(logging.WARNING)

class ProcessManager:

    def __init__(self):
        self.pool = Pool(processes=1) 
        self.mkdirs()
        self.setup_processors()
        self.file_handler = FileHandler()
        self.file_handler.watch_input_directory()
        self.rabbitMQConnection()

    def mkdirs(self):
        os.makedirs(os.path.join('data', 'input'), exist_ok = True)
        os.makedirs(os.path.join('data', 'processing'), exist_ok = True)
        os.makedirs(os.path.join('data', 'output'), exist_ok = True)

    def setup_processors(self):
        logger.info("Setting up processors")
        self.processors = []
        processors_module_name = 'processors'
        for p in pkgutil.walk_packages([processors_module_name]):
            self.processors.append(importlib.import_module(processors_module_name + '.' + p.name))
            logger.info("Importing processor: %r" % p.name)

    def rabbitMQConnection(self):
        logger.info("Setting up rabbitmq connection")

        host = os.getenv('RABBIT_HOST', '127.0.0.1')
        port = os.getenv('RABBIT_PORT', '5672')
        connetion = pika.BlockingConnection(
            pika.ConnectionParameters(host=host, port=port)
        )
        channel = connetion.channel()
        queue = channel.queue_declare(queue="events", durable=True, exclusive=False, auto_delete=False)
        channel.basic_qos(prefetch_count=1)
        channel.basic_consume(queue="events", on_message_callback=self.handle_event, auto_ack=False)
        channel.start_consuming()


    def handle_event(self, ch, method, properties, body):
        message = json.loads(body)
        ch.basic_ack(method.delivery_tag)
        history = message["history"]
        processor_to_start = None
        for processor in self.processors:
            if processor.want(message) and  processor.__name__ not in history:
                processor_to_start = processor
                break

        if processor_to_start != None:
            message["history"].append(processor_to_start.__name__)
            #self.pool.apply_async(processor.run, (message,))
            processor.run(message)
            #processor_thread = threading.Thread(target=processor.run, args=(message,))
            #processor_thread.start()
        else:
            self.file_handler.handle_output_file(message)

