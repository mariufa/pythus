import os
import struct

# Example usage
# import hashlib
#
# with open('flowfiles.pkg', 'rb') as f:
#     for attrs, offset, size in read_flow_file_stream(f):
#         m = hashlib.md5()
#
#         f.seek(offset)
#         m.update(f.read(size))
#
#         print("{}=={}".format(m.hexdigest(), attrs['hash.value']))

MAGIC_HEADER = b'NiFiFF3'

def read_field_length(fp):
    i = fp.read(2)
    if i == bytearray.fromhex('ffff'):
        return struct.unpack('>I', fp.read(4))[0]
    else:
        return struct.unpack('>H', i)[0]

def read_string(fp):
    length = read_field_length(fp)
    return fp.read(length).decode('UTF-8')

def read_flow_file_stream(fp):
    while True:
        if  fp.read(len(MAGIC_HEADER)) != MAGIC_HEADER:
            break

        attrs = {}
        num_attrs = read_field_length(fp)
        for _ in range(num_attrs):
            key = read_string(fp)
            value = read_string(fp)
            attrs[key] = value

        size, = struct.unpack('>Q', fp.read(8))
        offset = fp.tell()
        yield attrs, offset, size
        fp.seek(offset + size)