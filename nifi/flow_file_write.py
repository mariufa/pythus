import shutil 
import struct
import io
import json

# Example usage
# import io 
#
# with open('flowfiles.pkg', 'wb') as f:
#     data = b"Hello world!"
#     write_flow_file_stream(f, {'filename': 'file1'}, len(data), io.BytesIO(data))
#     write_flow_file_stream(f, {'filename': 'file2'}, len(data), io.BytesIO(data))

MAGIC_HEADER = b'NiFiFF3'
MAX_VALUE_2_BYTES = 65535

def write_string(fp, data):
    data = data.encode('UTF-8')
    length = len(data)

    if length < MAX_VALUE_2_BYTES:
        fp.write(struct.pack('>H', length))
    else:
        fp.write(b'\xff\xff')
        fp.write(struct.pack('>I', length))
    fp.write(data)

def write_flow_file_stream(fp, attrs, size, fileobj):
    fp.write(MAGIC_HEADER)
    fp.write(struct.pack('>H', len(attrs)))

    for key, value in attrs.items():
        write_string(fp, key)
        string_json_value = json.dumps(value)
        if string_json_value[0] == '"' and string_json_value[len(string_json_value)-1] == '"':
            write_string(fp, value)
        else:
            write_string(fp, string_json_value)

    fp.write(struct.pack('>Q', size))
    shutil.copyfileobj(fileobj, fp)

if __name__ == "__main__":
    with open('flowfiles.pkg', 'wb') as f:
        data = b"hello world"
        write_flow_file_stream(f, {'filename': 'file1', 'identifier': '123'}, len(data), io.BytesIO(data))