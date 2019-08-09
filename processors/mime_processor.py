from utils.rabbitmq import sendEvent
import magic
import mimetypes

def want(message):
    return True

def run(message):
    result = magic.from_file(message["path"], mime=True)
    if result == "application/octet-stream":
        mime_result = mimetypes.MimeTypes().guess_type(message["path"])[0]
        if mime_result != None:
            result = mime_result

    message["filetype"] = result
    sendEvent(message)


if __name__ == "__main__":
    result = magic.from_file('test-tull.iso', mime=True)
    result2 = mimetypes.MimeTypes().guess_type('./aa/b/test-tull.iso')[0]
    print(result)
    print(result2)
