def dump(s):
    ret = []
    for c in s:
        ret.append('%02x' % ord(c))
    return ''.join(ret)

