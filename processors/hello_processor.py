from utils.rabbitmq import sendEvent

def get_mime_types():
    return [
        'plain/text'
    ]

def run(message):
    print("running")
    sendEvent(message)

