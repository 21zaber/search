#!/usr/bin/python3.5

from flask import Flask, request
from flask_cors import CORS
from json import dumps, loads
from os import makedirs
import time
import copy

from wikidb import DB
from index_storage import IndexStorage, Index
from utils import get_page_url, rand_str, log 

PAGE_SIZE = 10
ROOT_DIR = '../data'

log("DB initialization")
db = DB()

log("Index initialization, base directory {}".format(ROOT_DIR))
index = Index(IndexStorage, dir=ROOT_DIR)
index.update_index_list()
index.read_headers()

log("API initialization")
APP = Flask(__name__)
CORS(APP)

cache = {}

def get_doc_descr(doc):
    log(doc)
    doc_id = doc.keys()[0]
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
    res = index.search(query)
    ts = time.time() - ts

    resp = {
        'id': rand_str(10),
        'res': res,
        'time': ts,
    }

    cache[resp['id']] = resp
    return resp['id']

@APP.route('/get_results/<id>/<page>', methods=['GET'])
def api_get_results(id, page):
    resp = copy.deepcopy(cache[id])
    page = int(page)

    resp['docs'] = [get_doc_descr(i) for i in resp['res']]
    del resp['res']

    return dumps(resp) + '\n'

@APP.after_request
def after_request(response):
  response.headers.add('Access-Control-Allow-Origin', '*')
  response.headers.add('Access-Control-Allow-Headers', 'Content-Type,Authorization')
  response.headers.add('Access-Control-Allow-Methods', 'GET,PUT,POST,DELETE,OPTIONS')
  return response

if __name__ == '__main__':
    APP.run(host='192.168.77.40')

