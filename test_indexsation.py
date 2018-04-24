from wikidb import DB
import tokenizer 
from pprint import pprint
from collections import defaultdict as DD
from index_storage import IndexStorage, Index
from time import gmtime, strftime, time as ctime
from utils import format_memory, format_time 

def obj_gen():
    req = DB().select(chunks=10000, limit=100000)
    while True:
        obj = req.fetchone()
        if not obj:
            break
        yield dict(obj)

index = Index(IndexStorage, dir='../data_test', threshold=2*1*1000*1000)

start_ts = ctime()
total_len = 0
total_cnt = 0

for doc in obj_gen():
    total_cnt +=1
    total_len += len(doc.get('text', ''))
    index.add_doc(doc['id'], tokenizer.extract_token_set(doc))

index.write_index()

total_time = ctime() - start_ts

print('Total time:', total_time)
print('Total data:', format_memory(total_len))
print('Total docs:', total_cnt)
print('Average time:', total_time / total_cnt)
print('Speed:', format_memory(total_len / total_time, sufix='b/sec'))
