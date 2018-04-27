from utils import log
import re
import time

def _prepare_word(query):
    return query.lower()

def search_in_index(index, header, fname, query):
    query = _prepare_word(query)
    log('Search for "{}" in {}'.format(query, fname))
    ts = time.time()

    with open(index._index_file(fname=fname), 'br') as idx:

        def read_term(p):
            return index._read_term(idx, header[p])

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

        if l > r:
            res = []
        else:
            res = index._read_list(idx, idx.tell())

    ts = time.time() - ts
    log("Search finished, {} results, {} sec.".format(len(res), ts))

    return res
                                                                                           
def parse_query(s):
    re_to_space = re.compile("[-,^&\\\\\']")
    re_to_nothing = re.compile("[\*<>=+]")

    def letter(c, debug=False):                                                                            
        return ((c >= 'a' and c <= 'z') or (c >= 'A' and c <= 'Z') or (c >= '0' and c <= '9'))

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
                data[i] = list(set(a[i])) | set(b[i])))
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
            return type(b)(type(b)._sub(b.data, a.data)
        return type(a)(type(a)._sub(a.data, b.data)

    def __and__(a, b):
        if a.n == b.n:
            return type(a)(type(a)._and(a.data, b.data), n=a.n)

        if a.n:
            return type(b)(type(b)._sub(b.data, a.data)
        return type(a)(type(a)._sub(a.data, b.data)

    def __neg__(a):
        return type(a)(a.data, n= not a.n)



def process_query(s, index, header, fname):
    stack = []

    ops = {'!', '&', '|'}

    for i in s:
        if i not in ops:
            stack.append(Res(search_in_index(index, header, fname, i)))
            continue

        if i == '!':
            stack[-1] = -stack[-1]
            continue
        
        if i == '|':
            stack[-2] |= stack[-1]
            del stack[-1]
            continue

        if i == '&':
            stack[-2] &= stack[-1]
            del stack[-1]
            continue

    if len(stack) == 1:
        return stack[0]._ids
    else:
        raise Exception()
