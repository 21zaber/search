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
query = 'Jacobowitz  | underemployed'

resp = index.search(query)
print(resp)

#   index = Index(IndexStorage, dir='data', threshold=2*100*1000*1000)
#   h = index._read_header(fout)
#
#   #resp = search_in_index(index, h, fout, input())
#
#   #query = 'Jacobowitz  | underemployed'
#   query = 'underemployed'
#
#   s = parse_query(query)
#
#   resp = process_query(s, index, h, fout)
#
#
#   print(resp)


