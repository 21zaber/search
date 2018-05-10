import struct
import storage

file = '../data/index_000.id'
hfile = '../data/index_000.ih'
N = 100
offset = 6836195

def _read(f, frmt='I', sz=4):
    a = f.read(sz)
    return struct.unpack(frmt, a)[0]

with open(file, 'br') as f:
    f.seek(offset, 0)
    
    block_size = storage.read_int_raw(f)
    length = storage.read_int(f)
    p, o = storage.get_jump_op(length)
    pos = None

    d = {}


    for i in range(length):
        doc = storage.read_int(f)
        if i not in d:
            d[i] = doc
                
        lst = storage.read_list(f)
        if storage.ENABLE_JUMPS and i % p == 0 and o > 2 and i < length-1:
            pos = storage.read_int_raw(f)

            m = f.tell()
            f.seek(pos, 1)
            print('JUMP pos: {}, from {} to {}'.format(i, m, f.tell()))
            doc = storage.read_int(f)
            d[i+o] = doc
            f.seek(m, 0)
        else:
            pos = None



rasd

l1 = []
l2 = []

with open(file, 'br') as f, open(hfile, 'br') as h: 
    _read(h)
    l1.append(_read(h))
    for i in range(N-1):
        l1.append(_read(h)+l1[-1])

    for i in range(N):
        l = _read(f, 'I', 4)
        print(l)
        s = b''
        for i in range(l):
            s += _read(f, frmt='c', sz=1)
        print(s)
        l = _read(f, 'I', 4)
        print(l)
        f.seek(l, 1)

        l2.append(f.tell())

print(l1)#[-10:])
print(l2)#[-11:-1])


