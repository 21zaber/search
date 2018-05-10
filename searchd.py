from utils import log, round_up
import re
import time
import storage

RES_TYPE_OP = 1
RES_TYPE_RES = 2
RES_TYPE_EMPTY = 3

class FakeRes():
    def __init__(self, d):
        self.d = d
        self.f = True

    def next(self):
        if self.f:
            self.f = False
            return self.d
        return None

class ResIter:
    def __init__(self, index=None, op=None, fname=None, pos=0, children=None, empty=False, term=None, read_list=False):
        self.term = term
        self.fname = fname
        self.position = pos
        self.op = op
        self.neg = False
        self.children = children
        self.index = index
        self.read_list = read_list

        self.jump_pos = None
        self.unjump_data = None

        if empty:
            self.type = RES_TYPE_EMPTY
        elif op and children:
            self.type = RES_TYPE_OP
            if self.op == '!':
                self.neg = True
        else:
            self.type = RES_TYPE_RES
            with open(self.index._index_file(fname=fname), 'br') as fidx:
                fidx.seek(pos, 0)
                self.block_size = storage.read_int_raw(fidx)
                self.length = storage.read_int(fidx)
                self.current_pos = fidx.tell()
                self.current_i = 0

                self.jump_p, self.jump_o = storage.get_jump_op(self.length)

    @staticmethod
    def _quote(a, b, d):
        ch1, ch2 = a.next(), b.next()
        while ch1 and ch2:
            if ch1[0] == ch2[0]:
                poses = []
                bset = set(ch2[1])
                for i in ch1[1]:
                    for k in range(d):
                        if i+k+1 in bset:
                            poses.append(i)
                            break
                if poses:
                    return (ch1[0], poses)
                ch1, ch2 = a.next(), b.next()
            elif ch1[0] < ch2[0]:
                ch1 = a.next()
            else:
                ch2 = b.next()
        return None

    def quote(self, children, d):
        if len(children) == 1:
            return children[0].next()
        r = self._quote(children[-2], children[-1], d)
        while r:
            t = self.quote(children[:-2]+[FakeRes(r)], d)
            if t:
                return t
            r = self._quote(children[-2], children[-1], d)

        return None

    def next(self):
        if self.type == RES_TYPE_EMPTY:
            return None
        if self.type == RES_TYPE_OP:
            if self.op == '&':
                ch1, ch2 = self.children[0].next(), self.children[1].next()
                while ch1 and ch2:
                    if ch1[0] == ch2[0]:
                        return ch1
                    elif ch1[0] < ch2[0]:
                        if self.children[0].can_jump():
                            j = self.children[0].jump()
                            if j[0] <= ch2[0]:
                                ch1 = j
                                log('SUCCESS JUMP', self.children[0].term)
                                continue
                            ch1 = self.children[0].unjump()

                        ch1 = self.children[0].next()
                    else:
                        if self.children[1].can_jump():
                            j = self.children[1].jump()
                            if j[0] <= ch1[0]:
                                ch2 = j
                                log('SUCCESS JUMP', self.children[1].term)
                                continue
                            ch2 = self.children[1].unjump()

                        ch2 = self.children[1].next()
            if self.op == '|':
                for i in self.children:
                    r = i.next()
                    while r:
                        return r
            if self.op == '!':
                ch1 = self.children[0].next()
                while ch1:
                    return ch1
            if self.op[0] == '/':
                n, d = map(int, self.op[1:].split('_'))
                for i in self.children:
                    i.read_list = True
                return self.quote(self.children, d)
        else:
            with open(self.index._index_file(fname=self.fname), 'br') as fidx:
                fidx.seek(self.current_pos, 0)
                if self.current_i < self.length:
                    doc = storage.read_int(fidx)
                    if self.read_list:
                        lst = storage.read_list(fidx)
                    else:
                        lst = storage.skip_list(fidx)
                    if storage.ENABLE_JUMPS and self.current_i % self.jump_p == 0 and self.jump_o > 2 and self.current_i < self.length-1:
                        self.jump_pos = storage.read_int_raw(fidx)
                    else:
                        self.jump_pos = None
                    self.current_pos = fidx.tell()
                    self.current_i += 1
                    return (doc, lst,)

        return None

    def can_jump(self):
        return self.jump_pos is not None

    def jump(self):
        log("Try to jump", self.jump_pos)
        self.unjump_data = (self.current_i, self.current_pos)
        self.current_i = min(self.jump_o + self.current_i-1, self.length-1)
        self.current_pos += self.jump_pos
        self.jump_pos = None
        return self.next()

    def unjump(self):
        self.current_i, self.current_pos = self.unjump_data
        return self.next()

def _prepare_word(query):
    return query.lower()

def search_in_index(index, header, fname, query):
    query = _prepare_word(query)
    log('Search for "{}" in {}'.format(query, fname))
    ts = time.time()

    with open(index._index_file(fname=fname), 'br') as idx:

        def read_term(p):
            idx.seek(header[p], 0)
            return storage.read_str(idx)

        l, r = 0, len(header)-1
        m = int(r/2)
        term = read_term(m)

        while term != query and l < r:
            if query > term:
                l = m+1
            else:
                r = m-1

            m = int((l+r) / 2)
            term = read_term(m)

        log("Block found for {} sec".format(time.time() - ts))

        if l > r:
            res = ResIter(empty=True)
        else:
            res = ResIter(index=index, fname=fname, pos=idx.tell(), term=query)

    return res
                                                                                           
def parse_query(s):
    re_to_space = re.compile("[-,^&\\\\\']")
    re_to_nothing = re.compile("[\*<>=+]")

    def digit(c, debug=False):
        return c >= '0' and c <= '9'

    def letter(c, debug=False):                                                                            
        return ((c >= 'a' and c <= 'z') or (c >= 'A' and c <= 'Z'))

    def word(s, debug=False):
        if debug:
            print('[word]', s)
        q = ''
        l = 0
        while l < len(s):
            if letter(s[l], debug=debug):
                q += s[l]
            else:
                break
            l += 1
        return [q], l

    def query(s, stop=False, debug=False):
        if debug:
            print('[query]', s)
        
        stack, p = [], 0
        
        if not s:
            return stack, p

        if s[0] == '!':
            rstack, rp = query(s[1:], debug=debug, stop=True)
            stack, p = rstack + ['!'], rp+1
        elif s[0] == '(':
            rstack, rp = query(s[1:], debug=debug)
            if rp+1 < len(s) and s[rp+1] == ')':
                stack, p = rstack, rp+2
            else:
                raise Exception()
        elif s[0] == '"':
            print(s)
            p = 1
            wrd_cnt = 0
            dist = 0
            wrd, rp = word(s[1:])
            while wrd:
                wrd_cnt += 1
                stack += wrd
                p += rp
                while p < len(s) and s[p] == ' ':
                    p+=1
                if s[p] == '"':
                    break
                wrd, rp = word(s[p:])
            if p >= len(s) or s[p] != '"':
                raise Exception()
            p += 1
            if p+1 < len(s) and s[p] == '/':
                p+=1
                while p < len(s) and digit(s[p]):
                    dist = dist * 10 + int(s[p])
                    p += 1
                
            if dist == 0:
                dist = 1

            op = '/{}_{}'.format(wrd_cnt, dist)
            stack.append(op)

        else:
            stack, p = word(s, debug=debug)
            
        if p == 0:
            return stack, 0
            
        while p < len(s) and not stop:
            or_flag = False
            sep = set(' |')
            while p < len(s) and s[p] in sep:
                if s[p] == '|':
                    or_flag = True
                p += 1
            
            rstack, rp = query(s[p:], stop=True, debug=debug)
            if rp < 1:
                break
            
            stack += rstack
            p += rp
            
            if or_flag:
                stack.append('|')
            else:
                stack.append('&')
            
        return stack, p

    s = s.lower()                   
    s = s.strip()                   
                                    
    s = re.sub(re_to_space, ' ', s) 
    s = re.sub(re_to_nothing, '', s)

    stack, _ = query(s)             

    return stack

class Res:
    def __init__(self, data, n=False):
        self.data = data
        self.n = n

    @staticmethod
    def _or(a, b):
        doc_ids = set(a.keys()) | set(b.keys())
        data = {}
        for i in doc_ids:
            data[i] = list(set(a.get(i, [])) | set(b.get(i, [])))

        return data
    
    @staticmethod
    def _and(a, b):
        doc_ids = set(a.keys()) & set(b.keys())
        data = {}
        for i in doc_ids:
            if i in a and i in b:
                data[i] = list(set(a[i]) | set(b[i]))
            elif i in a:
                data[i] = a[i]
            else:
                data[i] = b[i]

        return data

    @staticmethod
    def _sub(a, b):
        data = {}
        for doc_id in a.keys():
            if doc_id in b:
                data[doc_id] = list(set(a[doc_id]) - set(b[doc_id]))
            else:
                data[doc_id] = a[doc_id]

        return data

    def __or__(a, b):
        if a.n == b.n:
            return type(a)(type(a)._or(a.data, b.data), n=a.n)

        if a.n:
            return type(b)(type(b)._sub(b.data, a.data))
        return type(a)(type(a)._sub(a.data, b.data))

    def __and__(a, b):
        if a.n == b.n:
            return type(a)(type(a)._and(a.data, b.data), n=a.n)

        if a.n:
            return type(b)(type(b)._sub(b.data, a.data))
        return type(a)(type(a)._sub(a.data, b.data))

    def __neg__(a):
        return type(a)(a.data, n= not a.n)

    @staticmethod
    def quote(a, b, d):
        doc_ids = set(a.data.keys()) & set(b.data.keys())
        data = {}

        for i in doc_ids:
            data[i] = []
            bset = b.data[i]
            for j in a.data[i]:
                for k in range(d):
                    if j+k in bset:
                        data[i].append(j)
                        break
            if not data[i]:
                del data[i]
        return Res(data)


def process_query(s, index, header, fname):
    log("Converted query:", s)
    stack = []

    ops = {'!', '&', '|', '/'}

    for i in s:
        if i[0] not in ops:
            stack.append(search_in_index(index, header, fname, i))
            continue

        if i == '!':
            stack[-1] = ResIter(op='!', children=[stack[-1]])
            continue
        
        if i == '|':
            stack[-2] = ResIter(op='|', children=[stack[-1], stack[-2]])
            del stack[-1]
            continue

        if i == '&':
            stack[-2] = ResIter(op='&', children=[stack[-1], stack[-2]])
            del stack[-1]
            continue

        if i[0] == '/':
            n, d = map(int, i[1:].split('_'))
            r = ResIter(op=i, children=[stack[-i] for i in range(n, 0, -1)])

            for j in range(n):
                del stack[-1]

            stack.append(r)

    if len(stack) == 1:
        return stack[0]
    else:
        raise Exception()
