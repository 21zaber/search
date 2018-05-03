import struct

file = '../data/index_000.id'
hfile = '../data/index_000.ih'

def _read(f, frmt='I', sz=4):
    a = f.read(sz)
    return struct.unpack(frmt, a)[0]

l1 = []
l2 = []

with open(file, 'br') as f, open(hfile, 'br') as h: 
    _read(h)
    for i in range(100):
        l1.append(_read(h))

    for i in range(100):
        l = _read(f, 'I', 4)
        #print(l)
        s = b''
        for i in range(l):
            s += _read(f, frmt='c', sz=1)
        print(s)
        l = _read(f, 'I', 4)
        #print(l)
        f.seek(l, 1)

        l2.append(f.tell())

print(l1)
print(l2)


