from utils.rabbitmq import sendEvent
from langdetect import detect

def want(message):
    supported_filetype = [
        "text/plain"
    ]
    filetype = message["filetype"]
    return filetype in supported_filetype

def run(message):
    MAX_TEXT_BUFFER = 50000000
    with open(message["path"], 'rb') as f:
        text_bytes = f.read(MAX_TEXT_BUFFER)

    text = text_bytes.decode("utf-8")
    if text != None and len(text) > 20:
        language = detect(text)
        metadata = message["metadata"]
        metadata["language"] = language
    sendEvent(message)