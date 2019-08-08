from utils.rabbitmq import sendEvent
from langdetect import detect

def want(message):
    supported_filetype = [
        "plain/text"
    ]
    filetype = message["filetype"]
    return filetype in supported_filetype

def run(message):
    
    sendEvent(message)