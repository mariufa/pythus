from utils.rabbitmq import sendEvent

def get_mime_types():
    return [
        "image/png",
        "image/jpeg"
    ]

def run(message):
    #TODO ocr
    sendEvent(message)