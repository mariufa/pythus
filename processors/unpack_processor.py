from utils.rabbitmq import sendEvent
from pyunpack import Archive
import os
import uuid
import shutil

def want(message):
    supported_mime_types = [
        "application/zip"
    ]
    filetype = message["filetype"]
    return filetype in supported_mime_types

def run(message):
    processng_dir = "./data/processing"
    tmp_identifier = os.path.join(processng_dir, str(uuid.uuid4()))
    tmp_folder = os.path.join(tmp_identifier, "tmp")
    os.makedirs(tmp_folder, exist_ok = True)

    file_dest = os.path.join(tmp_identifier, message["filename"])
    shutil.copyfile(message["path"], file_dest)

    Archive(file_dest).extractall(tmp_folder)

    for root, dirs, files in os.walk(tmp_folder):
        for filename in files:

            new_identifier = str(uuid.uuid4())
            processing_dest = os.path.join(processng_dir, new_identifier)
            metadata = {}
            metadata["archive_file_path"] = root.replace(tmp_folder, '')
            new_message = {
                "identifier": new_identifier,
                "parent": message["identifier"],
                "path": processing_dest,
                "filename" : filename,
                "filetype": "unknown",
                "history": [],
                "metadata": metadata,
                "original_file": False
            }
            shutil.move(os.path.join(root, filename), processing_dest)
            sendEvent(new_message)

    shutil.rmtree(tmp_identifier)
    sendEvent(message)


if __name__ == "__main__":
    os.makedirs('tmp', exist_ok = True)
    Archive('test-tull.iso').extractall('tmp')
