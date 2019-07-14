from os.path import getsize
from utils.rabbitmq import sendEvent

def get_mime_types():
    return [
        "*"
    ]

def run(message):
    message["filesize"] = getsize(message["size"])
    sendEvent(message)