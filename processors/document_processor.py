from utils.rabbitmq import sendEvent
from zipfile import ZipFile
from tikapp import TikaApp
import os
import shutil
import uuid

def want(message):
    supported_mime_types = [
        "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
        "application/vnd.oasis.opendocument.text"
    ]
    filetype = message["filetype"]
    return filetype in supported_mime_types

def run(message):
    # Text
    tika_client = TikaApp(file_jar="./tika-app/tika-app-1.21.jar")
    tika_result = tika_client.extract_only_content(message["path"])

    if (tika_result != None):
        processing_dir = "./data/processing/"
        identifier = str(uuid.uuid4())
        workfile = processing_dir + identifier
        with open(workfile, 'wb') as f:
            f.write(tika_result.encode('UTF-8'))

        new_message = {
                "identifier": identifier,
                "parent": message["identifier"],
                "path": workfile,
                "filename" : "doc.txt",
                "filetype": "unknown",
                "history": [],
                "metadata": {},
                "original_file": False
            }
        sendEvent(new_message)

    # Images
    with ZipFile(message["path"], 'r') as zipObj:
        processng_dir = "./data/processing"
        tmp_identifier = os.path.join(processng_dir, str(uuid.uuid4()))
        zipObj.extractall(tmp_identifier)
    
        for root, dirs, files in os.walk(tmp_identifier):
            for filename in files:
                if ".png" in filename or ".jpeg" in filename or ".jpg" in filename:
                    new_identifier = str(uuid.uuid4())
                    processing_dest = os.path.join(processng_dir, new_identifier)
                    new_message = {
                        "identifier": new_identifier,
                        "parent": message["identifier"],
                        "path": processing_dest,
                        "filename" : filename,
                        "filetype": "unknown",
                        "history": [],
                        "metadata": {},
                        "original_file": False
                    }
                    shutil.move(os.path.join(root, filename), processing_dest)
                    sendEvent(new_message)
        shutil.rmtree(tmp_identifier)

    sendEvent(message)
