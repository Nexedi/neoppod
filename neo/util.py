from zlib import adler32

def dump(s):
    """Dump a binary string in hex."""
    ret = []
    for c in s:
        ret.append('%02x' % ord(c))
    return ''.join(ret)


def makeChecksum(s):
    """Return a 4-byte integer checksum against a string."""
    return adler32(s) & 0xffffffff
