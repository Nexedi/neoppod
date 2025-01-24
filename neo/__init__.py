# This file is exclusively for Py2/Py3 compatibility and anything that is
# defined here should not be imported in another way than 'from neo import *'.
# When support for Python 2 is dropped, this file should be emptied
# and all 'from neo import *' should be removed.

try:
    __import__('operator').call
except AttributeError: # Python < 3.11
    __import__('operator').call = lambda f: f()

bytes2str = bytes.decode
str2bytes = str.encode

def decodeAddress(address):
    if address:
        host, port = address
        return host.decode(), port

def encodeAddress(address):
    if address:
        host, port = address
        return host.encode(), port

if str is bytes:
    globals().update(dict.fromkeys(filter(lambda x: x[0] != '_', globals()),
                                   lambda x: x))

    from six.moves import filter, map, range, zip

import six
