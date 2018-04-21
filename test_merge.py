from index_storage import IndexStorage, Index
import os

fout = 'data/index_full'
ftmp = 'data/index_tmp'
dir = 'data'

index = Index(IndexStorage, dir='data', threshold=2*100*1000*1000)

files = os.listdir(dir)
files = {i[:-3] for i in files}
print(files)

index.indexes = [os.path.join(dir, i) for i in files]

index.merge(fout, ftmp)


