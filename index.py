import os
import shutil
from copy import deepcopy
from utils import *
from searchd import *
import storage

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

    def __init__(self, dir='', threshold=1000*1000):
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
            self.headers[idx] = self.read_header(idx)

    def __len__(self):
        return storage.int_size * (2 * self.term_cnt + self.doc_cnt + self.entr_cnt) + self.term_size

    def _write_term(self, f, term):
        f.write(storage.write_str(term))

    def _write_block(self, f, block):
        b = storage.write_block(block)
        f.write(b)
        return len(b)

    def write_header(self, header):
        with open(self._header_file(), 'w+b') as f:
            f.write(storage.write_list(header))

    def read_header(self, fname):
        with open(self._header_file(fname=fname), 'br') as f:
            return storage.read_list(f)

    def write_index(self):
        with open(self._index_file(), 'w+b') as index_file:
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

        self.write_header(header)

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
        
#   def _merge(self, fname1, fname2, outfname):
#       ''' Merge 2 indexes in files <fname1> and <fname2> to one file <outfname>'''
#
#       h = [self._read_header(fname1), self._read_header(fname2)]
#       hout = []
#
#       n = [len(h[0]), len(h[1])]
#       pos = [0, 0]
#       p = [0, 0]
#       term = ['', '']
#
#       with open(self._index_file(fname=fname1), 'br') as f1, \
#            open(self._index_file(fname=fname2), 'br') as f2, \
#            open(self._index_file(fname=outfname), 'bw') as fout:
#
#           f = [f1, f2]
#
#           def read_term(a):
#               term[a] = self._read_term(f[a], h[a][pos[a]])
#               p[a] = f[a].tell() 
#               pos[a] += 1
#
#           read_term(0)
#           read_term(1)
#
#           while pos[0] < n[0] or pos[1] < n[1]:
#               if pos[0] < n[0] and pos[1] < n[1]:
#                   if term[0] == term[1]:
#                       l1 = self._read_block(f[0], p[0])
#                       l2 = self._read_block(f[1], p[1])
#                       lo = Res(l1) | Res(l2)
#                       termo = term[0]
#                       read_term(0)
#                       read_term(1)
#                   else:
#                       if term[0] < term[1]:
#                           cur = 0
#                       else:
#                           cur = 1
#
#                       lo = self._read_block(f[cur], p[cur])
#                       termo = term[cur]
#                       read_term(cur)
#               else:
#                   if pos[0] >= n[0]:
#                       cur = 1
#                   else:
#                       cur = 0
#
#                   lo = self._read_block(f[cur], p[cur])
#                   termo = term[cur]
#                   read_term(cur)
#
#
#               hout.append(fout.tell())
#               fout.write(self.storage.str2byte(termo))
#               self._write_block(fout, lo)
#                fout.write(self.storage.lst2byte(list(lo)))
#
#       self._write_header(hout, outfname)
#       
#       log("Merged {} and {} to {}".format(fname1, fname2, outfname))
#       return
#
#   def merge(self, outfname, tmp_dir):
#       if len(self.indexes) == 0:
#           raise Exceprion("Nothing to merge")
#       if len(self.indexes) == 1:
#           os.rename(self.indexes[0], outfname)
#           return 
#
#       self.indexes = list(self.indexes)
#       self.indexes.sort()
#
#       try:
#           os.makedirs(tmp_dir)
#       except:
#           log("Temporary dir({}) exist".format(tmp_dir))
#
#       indexes = []
#       for i in self.indexes:
#           fn = self._index_file(fname=i)
#           tfn = os.path.join(tmp_dir, fn.rsplit('/')[-1])
#           shutil.copyfile(fn, tfn)
#           shutil.copyfile(fn[:-1]+'h', tfn[:-1]+'h')
#           log('Copy {} to {}'.format(fn, tfn))
#           log('Copy {} to {}'.format(fn[:-1]+'h', tfn[:-1]+'h'))
#
#           indexes.append(tfn[:-3])
#
#       cur_idx = 0
#       get_idx_name = lambda x: os.path.join(tmp_dir, 'tindex_{}'.format(cur_idx))
#
#       while len(indexes) > 1:
#           new_indexes = []
#
#          #i,l,s = -2, len(indexes), 2
#          #while i < l:
#          #    i+=2
#           for i in range(0, len(indexes)-1, 2):
#               nfn = get_idx_name(cur_idx)
#               cur_idx += 1
#
#               self._merge(indexes[i], indexes[i+1], nfn)
#               new_indexes.append(nfn)
#               os.remove(self._index_file(fname=indexes[i]))
#               os.remove(self._header_file(fname=indexes[i]))
#               os.remove(self._index_file(fname=indexes[i+1]))
#               os.remove(self._header_file(fname=indexes[i+1]))
#
#           if len(indexes) % 2 == 1:
#               new_indexes.append(indexes[-1])
#
#           indexes = deepcopy(new_indexes)
#           log(indexes)
#
#       os.rename(self._index_file(fname=indexes[0]), self._index_file(outfname))
#       os.rename(self._header_file(fname=indexes[0]), self._header_file(outfname))
#
#       shutil.rmtree(tmp_dir)
#       return 
#
#
#
#       self._merge(self.indexes[0], self.indexes[1], outfname)
#
#       fls = [outfname, tmp_file]
#
#       for i in range(2, len(self.indexes)):
#           self._merge(fls[0], self.indexes[i], fls[1])
#           fls = fls[::-1]
#
#       if fls[1] != outfname:
#           os.rename(tmp_file, outfname)
#
#       try:
#           os.remove(self._index_file(fname=tmp_file))
#           os.remove(self._header_file(fname=tmp_file))
#       except:
#           pass
#
#       return

    def search(self, query):
        s = parse_query(query)
        idx = self.indexes[0]
        
        resp = process_query(s, self, self.headers[idx], idx)
        return resp

        for idx in self.indexes:
            r = process_query(s, self, self.headers[idx], idx)
            res |= r

        return res
