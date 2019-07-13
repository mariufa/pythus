from utils.rabbitmq import sendEvent
import hashlib

def get_mime_types():
    return [
        "*"
    ]

def run(message):
    md5 = hashlib.md5()
    sha1 = hashlib.sha1()
    sha256 = hashlib.sha256()
    chunk_size = 4096
    with open(message["path"], 'rb') as f:
        data = f.read(chunk_size)
        while data:
            md5.update(data)
            sha1.update(data)
            sha256.update(data)
            data = f.read(chunk_size)

    metadata = message["metadata"]
    metadata["md5"] = md5.hexdigest()
    metadata["sha1"] = sha1.hexdigest()
    metadata["sha256"] = sha256.hexdigest()
    sendEvent(message)