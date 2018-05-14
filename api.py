#!/usr/bin/python3.5

from flask import Flask, request
from flask_cors import CORS
from json import dumps, loads
from os import makedirs
import time

from wikidb import DB
from index import Index
from utils import get_page_url, rand_str, log 
from searchd import ResIter

PAGE_SIZE = 10
ROOT_DIR = '../data'

log("DB initialization")
db = DB()

log("Index initialization, base directory {}".format(ROOT_DIR))
index = Index(dir=ROOT_DIR, prefix='111')
index.update_index_list()
index.read_headers()

tindex = Index(dir=ROOT_DIR, prefix='ttlidx', coef=10)
tindex.update_index_list()
tindex.read_headers()

log("API initialization")
APP = Flask(__name__)
CORS(APP)

cache = {}

def search(query):
    #r = index.search(query)
    rt = tindex.search(query)
    return rt#ResIter(op='|', children=[r, rt, ResIter(empty=True)])

def get_doc_descr(doc):
    doc_id = doc['id']
    row = db.select(obj_id=str(doc_id)).fetchone()
    if not row:
        return {}
    row = dict(row)
    doc['title'] = row.get('title', 'Empty title')
    doc['url'] = get_page_url(doc_id)
    try:
        del doc['lst']
    except:
        pass

    return doc

@APP.route('/search/', methods=['POST'])
def api_search():
    ts = time.time()
    data = request.json
    log('[api_search] {}'.format(data))

    query = data.get('query', '')

    res = search(query)
    ts = time.time() - ts
    log("Query searched for {} sec".format(ts))

    resp = {
        'id': rand_str(10),
        'res': res,
        'time': ts,
    }

    cache[resp['id']] = resp
    return resp['id']

@APP.route('/get_results/<id>/<page>', methods=['GET'])
def api_get_results(id, page):
    log('[api_get_results] id: {}, page: {}'.format(id, page))
    resp = cache[id]
    page = int(page)
    ats = 0

    docs = []
    for i in range(PAGE_SIZE):
        ts = time.time()
        doc = resp['res'].next()
        ats += time.time() - ts
        if not doc:
            break
        docs.append(get_doc_descr(doc))

    return dumps({'docs':docs, 'ats':ats / PAGE_SIZE, 'ts':ats}) + '\n'

@APP.after_request
def after_request(response):
  response.headers.add('Access-Control-Allow-Origin', '*')
  response.headers.add('Access-Control-Allow-Headers', 'Content-Type,Authorization')
  response.headers.add('Access-Control-Allow-Methods', 'GET,PUT,POST,DELETE,OPTIONS')
  return response

if __name__ == '__main__':
    APP.run(host='192.168.77.40')

