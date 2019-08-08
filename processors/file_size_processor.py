from os.path import getsize
from utils.rabbitmq import sendEvent

def want(message):
    return True

def run(message):
    metadata = message["metadata"]
    metadata["filesize"] = getsize(message["path"])
    sendEvent(message)