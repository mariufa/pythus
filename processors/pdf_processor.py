from utils.rabbitmq import sendEvent
from tikapp import TikaApp
import uuid
import PyPDF2
from PIL import Image
import shutil
import fitz

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
    extract_images_pymupdf(message)

    
    sendEvent(message)

def extract_images_pymupdf(message):
    doc = fitz.open(message["path"])
    for i in range(len(doc)):
        for img in doc.getPageImageList(i):
            processing_dir = "./data/processing/"
            identifier = str(uuid.uuid4())
            workfile = processing_dir + identifier

            xref = img[0]
            pix = fitz.Pixmap(doc, xref)
            if pix.n < 5:
                pix.writePNG(workfile)
            else:
                pix1 = fitz.Pixmap(fitz.czRGB, pix)
                pix1.writePNG(workfile)
                pix1 = None
            pix = None
            filename = str(i) + "-" + str(xref) + ".png"
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
    doc.close()

def extract_images_pypdf2(message):
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
                
                if xObject[obj]['/ColorSpace'] == '/DeviceRGB':
                    mode = "RGB"
                else:
                    mode = "P"

                filename = None
                tmp_workfile = workfile
                if xObject[obj]['/Filter'] == '/FlateDecode':
                    data = xObject[obj].getData()
                    img = Image.frombytes(mode, size, data)
                    tmp_workfile += ".png"
                    img.save(tmp_workfile)
                    filename = obj[1:] + ".png"
                elif xObject[obj]['/Filter'] == '/DCTDecode':
                    data = xObject[obj]._data
                    tmp_workfile += ".jpg"
                    img = open(tmp_workfile, "wb")
                    filename = obj[1:] + ".jpg"
                    img.write(data)
                    img.close()
                elif xObject[obj]['/Filter'] == '/JPXDecode':
                    data = xObject[obj].getData()
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
