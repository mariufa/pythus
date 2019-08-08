from utils.rabbitmq import sendEvent
from PIL import Image
import pytesseract
import uuid

def want(message):
    supported_filetypes = [
        "image/png",
        "image/jpeg"
    ]
    filetype = message["filtype"]
    return filetype in supported_filetypes

def run(message):
    text = pytesseract.image_to_string(Image.open(message["path"]))
    if text:
        processing_dir = "./data/processing/"
        identifier = str(uuid.uuid4())
        with open(processing_dir + identifier, 'wb') as f:
            f.write(text.encode('UTF-8'))

        new_message = {
            "identifier": identifier,
            "parent": message["identifier"],
            "path": processing_dir + identifier,
            "filename" : "ocr.txt",
            "filetype": "plain/text",
            "history": [],
            "metadata": {},
            "original_file": False
        }
        sendEvent(new_message)

    sendEvent(message)
