import time
import requests
import random
import string
from json import loads

MEMORY_BASE = 1000
MEMORY_NAMES = ['', 'K', 'M', 'G', 'T', 'E']

TIME_BASE = [60, 60, 24, 365]
TIME_NAMES = ['s', 'm', 'h', 'y']

def format_memory(n, base=MEMORY_BASE, sufix='b'):
    p = 0
    b = base
    while n > b:
        b *= base
        p += 1

    b //= base

    return '{:.3f} {}{}'.format(n/b, MEMORY_NAMES[p], sufix)

def format_time(n, sufix=''):
    p = 0
    while n > 2 * TIME_BASE[p]:
        n /= TIME_BASE[p]
        p += 1

    return '{:.4f} {}{}'.format(n, TIME_NAMES[p], sufix)

def get_ts():
    return time.strftime("%Y.%m.%d %H:%M:%S", time.gmtime())

def log(*args, **kwargs):
    s = '[{ts}]'.format(ts=get_ts())
    print(s, *args, **kwargs)

def round_up(a):
    ia = int(a)
    return (ia, ia+1)[ia<a]

def get_page_url(page_id):
    url_template = 'https://en.wikipedia.org/w/api.php?action=query&prop=info&pageids={}&inprop=url'
    headers = {
        'format':'json',
    }
    url = url_template.format(page_id)
    resp = requests.get(url, headers)
    data = loads(resp.text)
    try:
        return data['query']['pages'][str(page_id)]['fullurl']
    except:
        log("ERROR: Cannot get page link, page id '{}'".format(page_id))
        return ''

def rand_str(n):                                                                           
    return ''.join([random.choice(string.ascii_letters + string.digits) for i in range(n)])


SNIPPET_MAX_LEN = 200
SNIPPET_FMTSTR = '...{}...'
stop_sym = set(' .!?,')

def extract_snippet(text, q):
    l, r = 0, 1
    curlen = 0
    curans = 0
    spt = []
    term = ''

    ans = []
    mx = 0

    def check_term(term):
        for i in q:
            if term.startswith(i):
                return True
        return False
           
    i = 0
    while i < len(text):
        if text[i] in stop_sym:
            while i < len(text) and text[i] in stop_sym:
                term += text[i]
                i += 1

            curlen += len(term)
            spt.append(term)
            if curlen > SNIPPET_MAX_LEN:
                if check_term(spt[0]):
                    curans -= 1
                spt = spt[1:]
            if check_term(term):
                curans += 1

            if mx <= curans:
                mx = len(ans)-1
                ans = spt[::]

            term = ''
        else:
            term += text[i]
            i += 1

    spt = ''.join(ans)
    if spt:
        return SNIPPET_FMTSTR.format(spt.strip())
    return ''


