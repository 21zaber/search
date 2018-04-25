from index_storage import IndexStorage, Index
from time import gmtime, strftime, time as ctime
import os

dir = '../data'
#   fin = '../data/index_tmp/tindex_19'
#   fout = '../data/index_tmp/tfindex0'
fin = '../data/index_tmp/tindex_20'
fout = '../data/index_tmp/tfindex1'

index = Index(IndexStorage, dir='data', threshold=2*100*1000*1000)

h = index._read_header_old(fin)
index._write_header(h, fout)

