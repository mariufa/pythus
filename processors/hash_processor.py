from utils.rabbitmq import sendEvent
import hashlib

def get_mime_types():
    return [
        "*"
    ]

def run(message):
    data = None
    with open(message["path"], 'rb') as f:
        data = f.read()
    md5 = hashlib.md5(data)
    sha1 = hashlib.sha1(data)
    sha256 = hashlib.sha256(data)
    metadata = message["metadata"]
    metadata["md5"] = md5.hexdigest()
    metadata["sha1"] = sha1.hexdigest()
    metadata["sha256"] = sha256.hexdigest()
    sendEvent(message)