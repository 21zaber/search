import struct
import os

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
        return cls._int_struct().unpack(i)

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
        return 'index_{}'.format(self.current_idx)

    def _header_file(self):
        return os.path.join(self.dir, '{}.ih'.format(self._index_name()))

    def _index_file(self):
        return os.path.join(self.dir, '{}.id'.format(self._index_name()))

    def __init__(self, storage, dir='', threshold=1000*1000):
        self.storage = storage
        self.dir = dir
        self.current_idx = 0
        self.indexes = []
        self.index = {}
        self.term_size = 0
        self.list_len = 0
        self.threshold = threshold

        self.forget()

    def __len__(self):
        return self.storage.int_size * len(self.index) + self.term_size + self.list_len * self.storage.int_size

    def write_index(self):
        with open(self._index_file(), 'w+b') as index_file, open(self._header_file(), 'w+b') as header_file:
            st = self.storage
            idx = [(k, self.index[k]) for k in self.index]
            idx.sort(key=lambda x: x[0])
            def wi(b):
                index_file.write(b)
            def wh(b):
                header_file.write(b)

            term_count = len(self.index)
            print('term_count:', term_count)
            wh(st.int2byte(term_count))

            hsz, isz = 0, 0

            for i in idx:
                term, lst = i
                position = index_file.tell()

                wh(st.int2byte(position))
                wi(st.str2byte(term))
                wi(st.lst2byte(lst))

        print("Index stored in {}".format(self._index_file()))

    def forget(self):
        self.indexes.append(self._index_name())

        while os.path.isfile(self._index_file()) or os.path.isfile(self._header_file()):
            self.current_idx += 1

        self.index = {}
        self.term_size = 0
        self.list_len = 0


    def add_doc(self, doc_id, term_set):
        doc_id = int(doc_id)

        for term in term_set:
            if term not in self.index:
                self.index[term] = set()
                self.term_size += len(term)
            self.index[term].add(doc_id)
            self.list_len += 1

        if len(self) > self.threshold:
            self.write_index()
            self.forget()
            







































IndexStorage.test()
