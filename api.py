#!/usr/bin/python3.5

from flask import Flask, request
from flask_cors import CORS
from json import dumps, loads
from os import makedirs
import time
import copy

from wikidb import DB
from index_storage import IndexStorage, Index
from searchd import *
from utils import get_page_url, rand_str 

INDEX_FILE = '../data/index_tmp/tindex_20'
PAGE_SIZE = 10
db = DB()

index = Index(IndexStorage)
header = index._read_header(INDEX_FILE)

APP = Flask(__name__)
CORS(APP)

cache = {}

def get_doc_descr(doc_id):
    resp = {
        'id': doc_id,
    }
    row = db.select(obj_id=str(doc_id)).fetchone()
    doc = dict(row)
    resp['title'] = doc.get('title', 'Empty title')
    resp['url'] = get_page_url(doc_id)

    return resp

@APP.route('/search/', methods=['POST'])
def api_search():
    data = request.json

    query = data.get('query', '')

    ts = time.time()
    doc_ids = list(search(IndexStorage, header, INDEX_FILE, query))
    ts = time.time() - ts

    page_n = len(doc_ids) / PAGE_SIZE
    if page_n > int(page_n):
        page_n += 1
    page_n = int(page_n)
    doc_cnt = len(doc_ids)

    resp = {
        'id': rand_str(10),
        'doc_ids': doc_ids,
        'page_number': page_n,
        'count': doc_cnt,
        'time': ts,
    }

    cache[resp['id']] = resp
    return resp['id']

@APP.route('/get_results/<id>/<page>', methods=['GET'])
def api_get_results(id, page):
    resp = copy.deepcopy(cache[id])
    page = int(page)

    if page >= resp['page_number']:
        raise Exception()

    if page == resp['page_number']-1:
        doc_ids = resp['doc_ids'][page*PAGE_SIZE:]
    else:
        doc_ids = resp['doc_ids'][page*PAGE_SIZE:(page+1)*PAGE_SIZE]

    resp['docs'] = [get_doc_descr(i) for i in doc_ids]
    del resp['doc_ids']

    from pprint import pprint
    pprint(resp)

    return dumps(resp) + '\n'

@APP.after_request
def after_request(response):
  response.headers.add('Access-Control-Allow-Origin', '*')
  response.headers.add('Access-Control-Allow-Headers', 'Content-Type,Authorization')
  response.headers.add('Access-Control-Allow-Methods', 'GET,PUT,POST,DELETE,OPTIONS')
  return response

if __name__ == '__main__':
    APP.run(host='192.168.77.40')

