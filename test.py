from wikidb import DB
import tokenizer 
from pprint import pprint
from collections import defaultdict as DD
from index_storage import IndexStorage, Index
from time import gmtime, strftime, time as ctime
from utils import format_memory, format_time 

def obj_gen():
    req = DB().select()
    while True:
        obj = req.fetchone()
        if not obj:
            break
        yield dict(obj)

d = tokenizer.get_statistics(obj_gen)

pprint(d)

#   MIN_RATE = 100
#
#   rate = DD(int)
#
#   for i in DB().select(limit=10000):
#       r = tokenizer.extract_token_rate(dict(i))
#       for k in r:
#           rate[k] += r[k]
#
#   l = [(v, k) for k, v in rate.items() if v >= MIN_RATE]
#   l.sort()
#   l = l[::-1]
#   for i in l:
#       print('{}: {}'.format(i[1], i[0]))
