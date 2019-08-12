from utils.rabbitmq import sendEvent
from tikapp import TikaApp
import uuid
import PyPDF2
from PIL import Image
import shutil

def want(message):
    supported_mime_types = [
        "application/pdf"
    ]
    filetype = message["filetype"]
    return filetype in supported_mime_types

def run(message):
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
                "filename" : "pdf.txt",
                "filetype": "unknown",
                "history": [],
                "metadata": {},
                "original_file": False
            }
        sendEvent(new_message)
    extract_images(message)

    
    sendEvent(message)

def extract_images(message):
    input1 = PyPDF2.PdfFileReader(open(message["path"], "rb"))
    for page_index in range(input1.getNumPages()):
        page = input1.getPage(page_index)
        xObject = page['/Resources']['/XObject'].getObject()

        for obj in xObject:
            if xObject[obj]['/Subtype'] == '/Image':

                processing_dir = "./data/processing/"
                identifier = str(uuid.uuid4())
                workfile = processing_dir + identifier

                size = (xObject[obj]['/Width'], xObject[obj]['/Height'])
                data = xObject[obj].getData()
                if xObject[obj]['/ColorSpace'] == '/DeviceRGB':
                    mode = "RGB"
                else:
                    mode = "P"

                filename = None
                tmp_workfile = workfile
                if xObject[obj]['/Filter'] == '/FlateDecode':
                    img = Image.frombytes(mode, size, data)
                    tmp_workfile += ".png"
                    img.save(tmp_workfile)
                    filename = obj[1:] + ".png"
                elif xObject[obj]['/Filter'] == '/DCTDecode':
                    tmp_workfile += ".jpg"
                    img = open(tmp_workfile, "wb")
                    filename = obj[1:] + ".jpg"
                    img.write(data)
                    img.close()
                elif xObject[obj]['/Filter'] == '/JPXDecode':
                    tmp_workfile += ".jp2"
                    img = open(tmp_workfile, "wb")
                    filename = obj[1:] + ".jp2"
                    img.write(data)
                    img.close()

                if filename != None:
                    shutil.move(tmp_workfile, workfile)
                    new_message = {
                        "identifier": identifier,
                        "parent": message["identifier"],
                        "path": workfile,
                        "filename" : filename,
                        "filetype": "unknown",
                        "history": [],
                        "metadata": {},
                        "original_file": False
                    }
                    sendEvent(new_message)
