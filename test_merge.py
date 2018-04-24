from index_storage import IndexStorage, Index
from time import gmtime, strftime, time as ctime
import os

fout = '../data/index_full'
ftmp = '../data/index_tmp'
dir = '../data'

index = Index(IndexStorage, dir='data', threshold=2*100*1000*1000)

files = os.listdir(dir)
files = {i[:-3] for i in files}
print(files)

index.indexes = [os.path.join(dir, i) for i in files]

start_ts = ctime()

index.merge(fout, ftmp)

total_time = ctime() - start_ts

print('Total time:', total_time)


