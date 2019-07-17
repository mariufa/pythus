import threading
import time
import uuid
import logging
from os import walk, remove, rename
from os.path import join, getsize

from utils.rabbitmq import sendEvent
from nifi.flow_file_read import read_flow_file_stream
from nifi.flow_file_write import write_flow_file_stream

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class FileHandler:

    INPUT_DIRECTORY = "./data/input"
    OUTPUT_DIRECTORY = "./data/output"
    PROCESSING_DIRECTORY = "./data/processing"


    def watch_input_directory(self):
        input_thread = threading.Thread(target=self.input_thread)
        input_thread.start()

    def input_thread(self):
        while True:
            time.sleep(1)
            for root, dirs, filenames in walk(self.INPUT_DIRECTORY):
                for name in filenames:
                    if (name[0] != '.'):
                        self.handle_input_file(root, name)
    
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
                    "nifi_attrs": attrs,
                    "history": [],
                    "metadata": {}
                }

                sendEvent(message)
                remove(join(root, name))

    def handle_output_file(self, message):
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

