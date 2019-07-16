from os.path import getsize
from utils.rabbitmq import sendEvent

def get_mime_types():
    return [
        "*"
    ]

def run(message):
    metadata = message["metadata"]
    metadata["filesize"] = getsize(message["path"])
    sendEvent(message)