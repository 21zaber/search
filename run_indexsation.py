from wikidb import DB
import tokenizer 
from pprint import pprint
from collections import defaultdict as DD
from index import Index
from time import gmtime, strftime, time as ctime
from utils import format_memory, format_time, log
import os


LIMIT = 10**5
DB_IDS_FILE = '../files/docids.txt'
step = LIMIT // 10


db = DB()

def get_ids():
    if os.path.isfile(DB_IDS_FILE):
        with open(DB_IDS_FILE, 'r') as f:
            for i in range(LIMIT):
                yield dict(db.select(f.readline()[:-1]).fetchone())
    else:
        with open(DB_IDS_FILE, 'w') as f:
            req = db.select(chunks=10000, limit=LIMIT)
            while True:
                obj = req.fetchone()
                if not obj:
                    break
                f.write(obj['id']+'\n')
                yield dict(obj)

if __name__ == '__main__':
    log("Indexsation started")

    index = Index(dir='../data', threshold=4*100*1000*1000)
    tindex = Index(dir='../data', threshold=4*100*1000*1000, prefix='ttlidx')

    start_ts = ctime()
    total_len = 0
    total_cnt = 0

    for doc in get_ids():
        doc_id = doc['id']
        text = doc.get('text', '')
        title = doc.get('title')

        total_len += len(text) + len(title)
        total_cnt += 1

        index.add_doc(doc_id, tokenizer.extract_token_positions(text))
        tindex.add_doc(doc_id, tokenizer.extract_token_positions(title))

        if total_cnt % step == 0:
            log("{}/{} documents parsed".format(total_cnt, LIMIT))

    index.write_index()
    tindex.write_index()

    total_time = ctime() - start_ts

    print('Total time:', total_time)
    print('Total data:', format_memory(total_len))
    print('Total docs:', total_cnt)
    print('Average time:', total_time / total_cnt)
    print('Speed:', format_memory(total_len / total_time, sufix='b/sec'))
