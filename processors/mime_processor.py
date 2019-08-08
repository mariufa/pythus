from utils.rabbitmq import sendEvent
import magic

def want(message):
    return True

def run(message):
    message["filetype"] = magic.from_file(message["path"], mime=True)
    sendEvent(message)

