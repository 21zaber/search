from index_storage import Index
from utils import log
import re

def _prepare_word(query):
    return query.lower()

def search_in_index(index, header, fname, query):
    query = _prepare_word(query)
    log('Search for "{}" in {}'.format(query, fname))

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
            return []
        else:
            return index._read_list(idx, idx.tell())

                                                                                           
def parse_query(s):
    re_to_space = re.compile("[,^&\\\\\']")
    re_to_nothing = re.compile("[\-*<>=+]")

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
            rstack, rp = query(s[1:], debug=debug)
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

def process_query(s, index, header, fname):
    stack = []

    ops = {'!', '&', '|'}

    for i in s:
        if i not in ops:
            stack.append(set(search_in_index(index, header, fname, i)))
            continue

        if i == '!':
            stack[-2] -= stack[-1]
            del stack[-1]
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
        return stack[0]
    else:
        raise Exception()

def search(index_storage_cls, fname, query):
    index = Index(index_storage_cls)
    h = index._read_header(fname)
    s = parse_query(query)
    resp = process_query(s, index, h, fname)
    return resp




