from utils import log, round_up
import re
import time
import storage
import tokenizer
import math

RES_TYPE_OP = 1
RES_TYPE_RES = 2
RES_TYPE_EMPTY = 3

def TF_F(x):
    thld = 0.2
    if x < thld:
        return x
    return thld + (x-thld)**2

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
    def __init__(self, index=None, op=None, fname=None, pos=0, children=None, empty=False, term=None, read_list=False, idf=None, coef=1):
        self.term = term
        self.fname = fname
        self.position = pos
        self.op = op
        self.neg = False
        self.children = children
        self.index = index
        self.read_list = read_list
        self.backup = None

        self.idf = idf
        self.coef = coef

        self.jump_cnt = 0
        self.jump_suc = 0
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
            if ch1['id'] == ch2['id']:
                poses = []
                bset = set(ch2['lst'])
                for i in ch1['lst']:
                    for k in range(d):
                        if i+k+1 in bset:
                            poses.append(i)
                            break
                if poses:
                    return (ch1['id'], poses)
                ch1, ch2 = a.next(), b.next()
            elif ch1['id'] < ch2['id']:
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
                ch1, ch2 = self.children
                r1, r2 = ch1.next(), ch2.next()
                if ch1.neg == ch2.neg:
                    self.neg = ch1.neg
                    while r1 and r2:
                        if r1['id'] == r2['id']:
                            return {'id': r1['id'], 'tf-idf': r1['tf-idf']+r2['tf-idf'], 'lst': [], 'q':r1['q']+r2['q']}
                        elif r1['id'] < r2['id']:
                            if ch1.can_jump():
                                j = ch1.jump()
                                if j['id'] < r2['id']:
                                    r1 = j
                                    continue
                                r1 = ch1.unjump()
                            r1 = ch1.next()
                        else:
                            if ch2.can_jump():
                                j = ch2.jump()
                                if j['id'] < r1['id']:
                                    r2 = j
                                    continue
                                r2 = ch2.unjump()
                            r2 = ch2.next()
                else:
                    if ch1.neg:
                        ch1, ch2 = ch2, ch1
                        r1, r2 = r2, r1

                    while r1 and r2:
                        if r1['id'] == r2['id']:
                            r1, r2 = ch1.next(), ch2.next()
                            continue
                        elif r1['id'] < r2['id']:
                            ch2.revert()
                            return {'id': r1['id'], 'tf-idf': r1['tf-idf']+r2['tf-idf'], 'lst': [], 'q':r1['q']}
                        else:
                            if ch2.can_jump():
                                j = ch2.jump()
                                if j['id'] < r1['id']:
                                    r2 = j
                                    continue
                                r2 = ch2.unjump()
                            r2 = ch2.next()
                
            if self.op == '|':
                if len(self.children) != 2:
                    for i in self.children:
                        r = i.next()
                        while r:
                            return r
                else:
                    ch1, ch2 = self.children
                    r1, r2 = ch1.next(), ch2.next()
                    if ch1.neg == ch2.neg:
                        self.neg = ch1.neg     
                        while r1 or r2:
                            if not r1: return r2
                            if not r2: return r1

                            if r1['id'] == r2['id']:
                                return {'id': r1['id'], 'tf-idf': r1['tf-idf']+r2['tf-idf'], 'lst': [], 'q': r1['q']+r2['q']}
                            elif r1['id'] < r2['id']:
                                ch2.revert()
                                return r1
                            else:
                                ch1.revert()
                                return r2
                    else:
                        if ch1.neg:
                            ch1, ch2 = ch2, ch1
                            r1, r2 = r2, r1

                        self.neg = True

                        while r1 and r2:
                            if r1['id'] == r2['id']:
                                r1, r2 = ch1.next(), ch2.next()
                                continue
                            elif r1['id'] < r2['id']:
                                if ch1.can_jump():   
                                    j = ch1.jump()
                                    if j['id'] < r2['id']:
                                        r1 = j
                                        continue
                                    r1 = ch2.unjump()
                                r1 = ch2.next()
                            else:
                                ch1.revert()
                                return r2
            if self.op == '!':
                self.neg = True
                return self.children[0].next()
            if self.op[0] == '/':
                n, d = map(int, self.op[1:].split('_'))
                for i in self.children:
                    i.read_list = True
                r = self.quote(self.children, d)
                if not r:
                    return None
                return {'id': r[0], 'lst': r[1], 'tf-idf': n*n*len(r[1])*0.012, 'q': [i.term for i in self.children]}
        else:
            self.save()
            with open(self.index._index_file(fname=self.fname), 'br') as fidx:
                fidx.seek(self.current_pos, 0)
                if self.current_i < self.length:
                    doc = storage.read_int(fidx)
                    doc_len = storage.read_int(fidx)
                    rate = storage.read_int(fidx)
                    tf = rate / doc_len
                    tf = TF_F(tf) * self.coef
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
                    return {'id': doc, 'tf-idf': tf*self.idf, 'lst': lst, 'q':[self.term]}

        return None

    def can_jump(self):
        return self.jump_pos is not None

    def jump(self):
        #log("Try to jump", self.jump_pos)
        self.unjump_data = (self.current_i, self.current_pos)
        self.current_i = min(self.jump_o + self.current_i-1, self.length-1)
        self.current_pos += self.jump_pos
        self.jump_pos = None
        self.jump_cnt += 1
        self.jump_suc += 1
        return self.next()

    def unjump(self):
        self.current_i, self.current_pos = self.unjump_data
        self.jump_suc -= 1
        return self.next()

    def update_jump_cnt(self):
        if self.type == RES_TYPE_OP:
            self.jump_cnt = 0
            self.jump_suc = 0
            for i in self.children:
                self.jump_cnt += i.jump_cnt
                self.jump_suc += i.jump_suc

    def save(self):
        self.backup = (self.current_i, self.current_pos)

    def revert(self):
        if self.backup:
            self.current_i, self.current_pos = self.backup

def _prepare_word(query):
    return tokenizer.prepare_token(query.lower())

def search_in_index(index, header, fname, query):
    query = _prepare_word(query)
    log('Search for "{}" in {}'.format(query, fname))
    ts = time.time()

    with open(index._index_file(fname=fname), 'br') as idx:

        def read_term(p):
            idx.seek(header[p], 0)
            return storage.read_int(idx), storage.read_str(idx)

        l, r = 0, len(header)-1
        m = int(r/2)
        idf, term = read_term(m)

        while term != query and l < r:
            if query > term:
                l = m+1
            else:
                r = m-1

            m = int((l+r) / 2)
            idf, term = read_term(m)

        log("Block found for {} sec".format(time.time() - ts))

        if l > r:
            res = ResIter(empty=True)
        else:
            res = ResIter(index=index, fname=fname, pos=idx.tell(), term=query, idf=idf/1000)

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

import heapq

class TFIDF_iterator:
    def __init__(self, iter):
        h = []
        r = iter.next()
        while r:
            heapq.heappush(h, (r['tf-idf'], r['id'], r['q']))
            r = iter.next()
            if len(h) > 1000:
                heapq.heappop(h)

        self.data = [heapq.heappop(h) for i in range(len(h))][::-1]
        self.i = 0

    def next(self):
        if self.i < len(self.data):
            self.i += 1
            d = self.data[self.i-1]
            return {'id': d[1], 'tf-idf': d[0], 'q':d[2]} 
        return None


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
