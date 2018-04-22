from index_storage import IndexStorage, Index
from searchd import *
import os

fout = 'data/index_full'
query = 'Jacobowitz  | underemployed'

resp = search(IndexStorage, fout, query)
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


