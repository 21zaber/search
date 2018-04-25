#!/usr/bin/python3.5

from flask import Flask, request
from flask_cors import CORS
from json import dumps, loads
from os import makedirs
import time

from wikidb import DB
from index_storage import IndexStorage, Index
from searchd import *
from utils import get_page_url 

INDEX_FILE = '../data/index_14'
PAGE_SIZE = 10
db = DB()

APP = Flask(__name__)
CORS(APP)

def get_doc_descr(doc_id):
    resp = {
        'id': doc_id,
    }
    row = db.select(obj_id=str(doc_id)).fetchone()
    doc = dict(row)
    resp['title'] = doc.get('title', 'Empty title')
    resp['url'] = get_page_url(doc_id)

    return resp

@APP.route('/search/<query>/<page>', methods=['GET'])
def api_search(query, page):
    #data = request.json
    print(query, page)
    page = int(page)

#   query = data.get('query', '')
#   page = data.get('page', 0)

    ts = time.time()
    doc_ids = list(search(IndexStorage, INDEX_FILE, query))
    ts = time.time() - ts

    page_n = len(doc_ids) / PAGE_SIZE
    if page_n > int(page_n):
        page_n += 1
    page_n = int(page_n)
    doc_cnt = len(doc_ids)

    if page >= page_n:
        raise Exception()
    if page == page_n-1:
        doc_ids = doc_ids[page*PAGE_SIZE:]
    else:
        doc_ids = doc_ids[page*PAGE_SIZE:(page+1)*PAGE_SIZE]

    docs = [get_doc_descr(i) for i in doc_ids]

    resp = {
        'docs': docs,
        'page_number': page_n,
        'count': doc_cnt,
        'time': ts,
    }

    from pprint import pprint
    pprint(resp)
    return dumps(resp)+'\n'

if __name__ == '__main__':
    APP.run(host='192.168.77.40')

