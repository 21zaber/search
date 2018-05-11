import struct
from math import sqrt
import math
from utils import round_up


# Encoding/decoding, varbite8

def encode_int(a):
    b = b''                           
    base = 2 ** 7                     
    while a >= base:                  
        b += struct.pack('B', a%base) 
        a //= base                    
    b += struct.pack('B', base + a)   

    return b

def decode_int(b):
    a = 0                        
    base = 2 ** 7                
    osn = 1                      
    l = 0       
    f = True    
    for i in b:                  
        l += 1                   
        if i >= base:            
            a += (i - base) * osn
            f = False
            break                
        a += i * osn             

        osn *= base              

    if f:
        return None, None

    return a, l                  


# Output
# Convert some structures to bytes

int_size = 4
int_format = 'I'
jump_size = 1 * int_size

ENABLE_VARBITE = True
ENABLE_LIST_CMP = True
ENABLE_JUMPS = True

def get_jump_op(l):
    p = max(1, round_up(sqrt(l-1) / 3)) # jump number
    o = min(p-1, max(1, round_up(math.log(l, 1.3)))) # jump len

    return p, o

def write_int_raw(a):
    return struct.pack(int_format, a)

def write_int(a):
    if ENABLE_VARBITE:
        return encode_int(a)
    return write_int_raw(a)

def write_str(s):
    b = bytes(s, encoding='utf8')
    return write_int(len(s)) + struct.Struct('{}s'.format(len(b))).pack(b)

def write_term(s, idf):
    return write_int(int(1000*idf)) + write_str(s)

def write_list(l):
    if ENABLE_LIST_CMP:
        nl = [l[0]]
        for i in range(1, len(l)):
            nl.append(l[i] - l[i-1])
        l = nl
        
    b = b''.join([write_int(i) for i in l])
    return write_int(len(b)) + b

def write_entrance(doc_id, doc_len, coords):
    return write_int(doc_id) + write_int(doc_len) + write_int(len(coords)) + write_list(coords)

def write_jump(offset):
    return write_int_raw(offset)

def write_block(block, doc_lens):
    l = len(block)
    p, o = get_jump_op(l)   

    entrances = []
    sizes = [0]
    
    doc_ids = list(block.keys())
    doc_ids.sort()
    
    for i in range(l): # encode entrances
        doc_id = doc_ids[i]
        entrance = list(block[doc_id])
        entrance.sort()
        entrances.append(write_entrance(doc_id, doc_lens[doc_id], entrance))
        sizes.append(sizes[-1] + len(entrances[-1]))
        
    sizes = sizes[1:]
    
    b = write_int(l)
    
    for i in range(l): # add jumps and join all bites
        b += entrances[i]
        if ENABLE_JUMPS and i % p == 0 and o > 2 and i < l-1:
            to = min(l-1, i+o-1)
            b += write_jump(sizes[to] - sizes[i])

    b = write_int_raw(len(b)) + b
        
    return b

# Some input operations

def read_int_raw(f):
    return struct.unpack(int_format, f.read(int_size))[0]

def read_int(f):
    if not ENABLE_VARBITE:
        return read_int_raw(f)

    l = 4
    b = f.read(4)
    a, _l = decode_int(b)
    while a is None:
        l += 4
        b += f.read(4)
        a, _l = decode_int(b)

    f.seek(_l-l, 1)
    return a

def read_list(f):
    l = read_int(f)
    b = f.read(l)

    if not ENABLE_VARBITE:
        lst = []
        lst.append(struct.unpack(int_format, b[0:4])[0])
        for i in range(4, len(b)-4, 4):
            lst.append(struct.unpack(int_format, b[i:i+4])[0] + lst[-1])
        return lst

    a, p = decode_int(b)
    b = b[p:]
    l = [a]
    
    while b:
        a, p = decode_int(b)
        b = b[p:]
        l.append(a+l[-1])
        
    return l
    
def read_str(f):
    l = read_int(f)
    b = f.read(l)
    return str(b, encoding='utf8')

def skip_list(f):
    l = read_int(f)
    f.seek(l, 1)
    return []


if __name__ == '__main__':
    sample = {i:[1,2,8] for i in range(10)}
    doc_lens = {i: 10 for i in range(10)}
    print("Some test, convert {} to bytes".format(sample))
    print("Result:", write_block(sample, doc_lens))
