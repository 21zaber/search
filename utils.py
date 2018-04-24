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
        raise Exception("Cannot get page link")

def rand_str(n):                                                                           
    return ''.join([random.choice(string.ascii_letters + string.digits) for i in range(n)])

