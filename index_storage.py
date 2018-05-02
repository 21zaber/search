import struct
import os
import shutil
from copy import deepcopy
from utils import *
from searchd import *

class IndexStorage:
    encoding = 'utf8'
    
    len_format = 'I'
    len_size = 4
    int_format = 'I'
    int_size = 4
    
    @staticmethod
    def _round_size(sz):
        if sz % 4:
            sz += 4 - (sz % 4)
            
        return sz
    
    @classmethod
    def _calc_str_size(cls, l):
        return cls._round_size(cls.len_size + l)
    
    @classmethod
    def _calc_lst_size(cls, l):
        return cls._round_size(cls.len_size + l * cls.int_size)
    
    @classmethod
    def _str_struct(cls, l): 
        if l > 256 ** cls.len_size:
            raise Exception("[IndexStorage._str_struct] Too long string(len={})".format(l))
        return struct.Struct('{}{}s'.format(cls.len_format, l))
    
    @classmethod
    def _lst_struct(cls, l): 
        if l > 256 ** cls.len_size:
            raise Exception("[IndexStorage._lst_struct] Too long list(len={})".format(l))
        return struct.Struct('{}{}'.format(cls.len_format, cls.int_format * l))
    
    @classmethod
    def _int_struct(cls):
        return struct.Struct(cls.int_format)

    @classmethod
    def _len_struct(cls):
        return struct.Struct(cls.len_format)

    @classmethod
    def str2byte(cls, s):
        try:
            b = s.encode(cls.encoding)
        except:
            raise Exception("[IndexStorage.str2byte] Cannot encode string(s='{}')".format(s))
            
        return cls._str_struct(len(s)).pack(len(s), b)
    
    @classmethod
    def byte2str(cls, b):
        try:
            l = struct.unpack(cls.len_format, b[:cls.len_size])[0]
        except:
            raise Exception("[IndexStorage.byte2str] Cannot extract string length(b={})".format(b))
        
        try:
            _l, s = cls._str_struct(l).unpack(b[:cls._calc_str_size(l)])
        except:
            raise Exception("[IndexStorage.byte2str] Cannot unpack string(b={})".format(b))
        
        try:
            s = s.decode(cls.encoding)
        except:
            raise Exception("[IndexStorage.byte2str] Cannot decode string(s='{}')".format(s))
            
        return s
    
    @classmethod
    def lst2byte(cls, lst):
        l = len(lst)
        return cls._lst_struct(l).pack(l, *lst)        
    
    @classmethod
    def byte2lst(cls, b):
        try:
            l = struct.unpack(cls.len_format, b[:cls.len_size])[0]
            
        except:
            raise Exception("[IndexStorage.byte2lst] Cannot extract list length(b={})".format(b))
            
        try:
            t = cls._lst_struct(l).unpack(b[:cls._calc_lst_size(l)])
        except:
            raise Exception("[IndexStorage.byte2lst] Cannot unpack list(b={})".format(b))            
        
        return list(t)[1:]
    
    @classmethod
    def int2byte(cls, i):
        return cls._int_struct().pack(i)

    @classmethod
    def byte2int(cls, b):
        return cls._int_struct().unpack(b[:cls.int_size])[0]

    @classmethod
    def byte2len(cls, b):
        return cls._len_struct().unpack(b[:cls.len_size])[0]

    @classmethod
    def test(cls):
        test_str = 'adf' * 40
        b = IndexStorage.str2byte(test_str)
        s = IndexStorage.byte2str(b)
        if test_str != s:
            raise Exception("[IndexStrorage] String coding-encoding error.")
    
        test_lst = [1, 1231231231, 1000] * 40
        b = IndexStorage.lst2byte(test_lst)
        l = IndexStorage.byte2lst(b)
        if test_lst != l:
            raise Exception("[IndexStrorage] List coding-encoding error.")

class Index:
    def _index_name(self):
        return 'index_{}'.format(str(self.current_idx).rjust(3, '0'))

    def _header_file(self, fname=None):
        if not fname:
            return os.path.join(self.dir, '{}.ih'.format(self._index_name()))
        return '{}.ih'.format(fname)

    def _index_file(self, fname=None):
        if not fname:
            return os.path.join(self.dir, '{}.id'.format(self._index_name()))
        return '{}.id'.format(fname)

    def __init__(self, storage, dir='', threshold=1000*1000):
        self.storage = storage
        self.dir = dir
        self.current_idx = 0
        self.headers = {}
        self.indexes = []
        self.index = {}

        self.term_size = 0
        self.term_cnt = 0
        self.entr_cnt = 0
        self.doc_cnt = 0

        self.threshold = threshold

        self.forget()

    def set_index_list(self, indexes):
        self.indexes = indexes

    def update_index_list(self):
        files = os.listdir(self.dir)
        files = {i[:-3] for i in files if i.endswith('.id')}
        self.indexes = [os.path.join(self.dir, f) for f in files]
        log("Index list updated: {}".format(', '.join(self.indexes)))

    def read_headers(self):
        for idx in self.indexes:
            log("Read header {}".format(idx))
            self.headers[idx] = self._read_header(idx)

    def __len__(self):
        len_size = self.storage.len_size
        int_size = self.storage.int_size
        return len_size * (2 * self.term_cnt + self.doc_cnt) + int_size * self.entr_cnt + self.term_size

    def write_index(self):
        with open(self._index_file(), 'w+b') as index_file:
            st = self.storage
            idx = [(k, self.index[k]) for k in self.index]
            idx.sort(key=lambda x: x[0])

            term_count = len(self.index)
            header = []

            hsz, isz = 0, 0

            for i in idx:
                term, block = i
                position = index_file.tell()
                header.append(position)
                self._write_term(index_file, term)
                self._write_block(index_file, block)

        self._write_header(header, os.path.join(self.dir, self._index_name()))

        log("Index stored in {}".format(self._index_file()))
        self.indexes.append(self._index_name())

    def forget(self):
        while os.path.isfile(self._index_file()) or os.path.isfile(self._header_file()):
            self.current_idx += 1

        self.index = {}
        self.term_size = 0
        self.term_cnt = 0
        self.entr_cnt = 0
        self.doc_cnt = 0

    def add_doc(self, doc_id, term_dict):
        doc_id = int(doc_id)

        for term in term_dict:
            if term not in self.index:
                self.index[term] = {}
                self.term_size += len(term)
            self.index[term][doc_id] = term_dict[term]
            self.term_cnt += 1
            self.doc_cnt += 1
            self.entr_cnt += len(term_dict[term])

        if len(self) > self.threshold:
            self.write_index()
            self.forget()
        
    def _read_header(self, fname):
        header = []
        with open(self._header_file(fname=fname), 'br') as f:
            return self._read_list(f, 0)
            n = f.read(self.storage.int_size)
            n = self.storage.byte2int(n)
            for i in range(n):
                header.append(self.storage.byte2int(f.read(self.storage.int_size)))
                if i > 0:
                    header[-1] += header[-2]

        return header

    def _write_header(self, header, fname):
        with open(self._header_file(fname=fname), 'bw') as f:
            self._write_list(f, header)
        return
        data = [header[0]]

        for i in range(1, len(header)):
            data.append(header[i]-header[i-1])

        with open(self._header_file(fname=fname), 'bw') as f:
            f.write(self.storage.lst2byte(data))  

    def _read_term(self, f, p):
        f.seek(p, 0)
        lb = f.read(self.storage.len_size)
        l = self.storage.byte2len(lb)

        strb = lb + f.read(l)
        return self.storage.byte2str(strb)

    def _write_term(self, f, term):
        f.write(self.storage.str2byte(term))

    def _encode_int8(self, a):
        b = b''
        base = 2 ** 7
        while a >= base:
            b += struct.pack('B', a%base)
            a //= base
        b += struct.pack('B', base + a)
            
        return b

    def _decode_int8(self, b):
        a = 0
        base = 2 ** 7
        osn = 1
        l = 0
        for i in b:
            l += 1
            if i >= base:
                a += (i - base) * osn
                break
            a += i * osn
            
            osn *= base
            
        return a, l

    def _encode_list8(self, l):
        b = b''
        b += self._encode_int8(l[0])
        t = l[0]
        for i in l[1:]:
            b += self._encode_int8(i-t)
            t = i

        return self.storage.int2byte(len(b)) + b

    def _decode_list8(self, b):
        length = self.storage.byte2len(b[0])
        l = []
        i = 1
        while i < length+1:
            a, p = self._decode_int8(b[i:])
            l.append(a)
            i += p
        return l

    def _read_list(self, f, p):
        f.seek(p, 0)
        lb = f.read(self.storage.len_size)
        length = self.storage.byte2len(lb)
        l = []
        b = f.read(length)
        
        while b:
            a, p = self._decode_int8(b)
            b = b[p:]
            l.append(a)
            
        return l

    def _write_list(self, f, l): 
        f.write(self._encode_list8(l))

    def _read_block(self, f, p):
        f.seek(p, 0)
        block = {}

        lb = f.read(self.storage.int_size)
        l = self.storage.byte2int(lb)

        bblock = f.read(l)
        p = 0
        while p < len(bblock) - self.storage.int_size * 3:
            doc = self.storage.byte2int(bblock[p:])
            p += self.storage.int_size
            block[doc] = self._decode_list8(bblock[p:])
            #block[doc] = self.storage.byte2lst(bblock[p:])
            p += self.storage.int_size * (len(block[doc]) + 1)

        return block

    def _write_block(self, f, block):
        st = self.storage
        bblock = bytes()
        doc_ids = list(block.keys())
        doc_ids.sort()

        for doc_id in doc_ids:
            pos = list(block[doc_id])
            pos.sort()
            bblock += st.int2byte(doc_id)
            bblock += self._encode_list8(pos)

        f.write(st.int2byte(len(bblock)) + bblock)

    def _merge(self, fname1, fname2, outfname):
        ''' Merge 2 indexes in files <fname1> and <fname2> to one file <outfname>'''


        h = [self._read_header(fname1), self._read_header(fname2)]
        hout = []

        n = [len(h[0]), len(h[1])]
        pos = [0, 0]
        p = [0, 0]
        term = ['', '']

        with open(self._index_file(fname=fname1), 'br') as f1, \
             open(self._index_file(fname=fname2), 'br') as f2, \
             open(self._index_file(fname=outfname), 'bw') as fout:

            f = [f1, f2]

            def read_term(a):
                term[a] = self._read_term(f[a], h[a][pos[a]])
                p[a] = f[a].tell() 
                pos[a] += 1

            read_term(0)
            read_term(1)

            while pos[0] < n[0] or pos[1] < n[1]:
                if pos[0] < n[0] and pos[1] < n[1]:
                    if term[0] == term[1]:
                        l1 = self._read_block(f[0], p[0])
                        l2 = self._read_block(f[1], p[1])
                        lo = Res(l1) | Res(l2)
                        termo = term[0]
                        read_term(0)
                        read_term(1)
                    else:
                        if term[0] < term[1]:
                            cur = 0
                        else:
                            cur = 1

                        lo = self._read_block(f[cur], p[cur])
                        termo = term[cur]
                        read_term(cur)
                else:
                    if pos[0] >= n[0]:
                        cur = 1
                    else:
                        cur = 0

                    lo = self._read_block(f[cur], p[cur])
                    termo = term[cur]
                    read_term(cur)


                hout.append(fout.tell())
                fout.write(self.storage.str2byte(termo))
                self._write_block(fout, lo)
#                fout.write(self.storage.lst2byte(list(lo)))

        self._write_header(hout, outfname)
        
        log("Merged {} and {} to {}".format(fname1, fname2, outfname))
        return

    def merge(self, outfname, tmp_dir):
        if len(self.indexes) == 0:
            raise Exceprion("Nothing to merge")
        if len(self.indexes) == 1:
            os.rename(self.indexes[0], outfname)
            return 

        self.indexes = list(self.indexes)
        self.indexes.sort()

        try:
            os.makedirs(tmp_dir)
        except:
            log("Temporary dir({}) exist".format(tmp_dir))

        indexes = []
        for i in self.indexes:
            fn = self._index_file(fname=i)
            tfn = os.path.join(tmp_dir, fn.rsplit('/')[-1])
            shutil.copyfile(fn, tfn)
            shutil.copyfile(fn[:-1]+'h', tfn[:-1]+'h')
            log('Copy {} to {}'.format(fn, tfn))
            log('Copy {} to {}'.format(fn[:-1]+'h', tfn[:-1]+'h'))

            indexes.append(tfn[:-3])

        cur_idx = 0
        get_idx_name = lambda x: os.path.join(tmp_dir, 'tindex_{}'.format(cur_idx))

        while len(indexes) > 1:
            new_indexes = []

           #i,l,s = -2, len(indexes), 2
           #while i < l:
           #    i+=2
            for i in range(0, len(indexes)-1, 2):
                nfn = get_idx_name(cur_idx)
                cur_idx += 1

                self._merge(indexes[i], indexes[i+1], nfn)
                new_indexes.append(nfn)
                os.remove(self._index_file(fname=indexes[i]))
                os.remove(self._header_file(fname=indexes[i]))
                os.remove(self._index_file(fname=indexes[i+1]))
                os.remove(self._header_file(fname=indexes[i+1]))

            if len(indexes) % 2 == 1:
                new_indexes.append(indexes[-1])

            indexes = deepcopy(new_indexes)
            log(indexes)

        os.rename(self._index_file(fname=indexes[0]), self._index_file(outfname))
        os.rename(self._header_file(fname=indexes[0]), self._header_file(outfname))

        shutil.rmtree(tmp_dir)
        return 



        self._merge(self.indexes[0], self.indexes[1], outfname)

        fls = [outfname, tmp_file]

        for i in range(2, len(self.indexes)):
            self._merge(fls[0], self.indexes[i], fls[1])
            fls = fls[::-1]

        if fls[1] != outfname:
            os.rename(tmp_file, outfname)

        try:
            os.remove(self._index_file(fname=tmp_file))
            os.remove(self._header_file(fname=tmp_file))
        except:
            pass

        return

    def search(self, query):
        s = parse_query(query)
        idx = self.indexes[0]
        return process_query(s, self, self.headers[idx], idx)

        for idx in self.indexes:
            r = process_query(s, self, self.headers[idx], idx)
            res |= r

        return res

IndexStorage.test()
