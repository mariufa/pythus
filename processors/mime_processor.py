from utils.rabbitmq import sendEvent
import magic

def get_mime_types():
    return [
        "unknown"
    ]

def run(message):
    message["filetype"] = magic.from_file(message["path"], mime=True)
    sendEvent(message)

