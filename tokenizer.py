import re
from collections import defaultdict as DD
from time import gmtime, strftime, time as ctime

from utils import format_memory, format_time, log 
from stemmer import Stemmer

st = Stemmer(method='wordnet')

MIN_TOKEN_LENGTH = 3

# wrong symbol re
wsre = re.compile('[^a-zA-Z\ ]')

def check_token(token):
    return len(token) >= MIN_TOKEN_LENGTH

def prepare_token(token):
    stemed = st.stem(token)
    #log('{} -> {}'.format(token, stemed))
    return stemed
    return token

def extract_token_list(data):
    text = data

    #prepare text
    text = re.sub(wsre, ' ', text)   
    text = text.lower()

    token_list = text.split()   
    token_list = [prepare_token(i) for i in token_list if i and check_token(i)]
    return token_list

def extract_token_set(obj):
    return {i for i in extract_token_list(obj) if i and check_token(i)}

def extract_token_rate(obj):
    token_list = extract_token_list(obj) 
    
    tokens = DD(int)
    for i in range(len(token_list)):
        token = token_list[i]
        if token and check_token(token):
                tokens[token] += 1

    return dict(tokens)

def extract_token_positions(obj):
    token_list = extract_token_list(obj) 
    
    tokens = DD(set)
    p = 0
    for i in range(len(token_list)):
        token = token_list[i]
        if token: 
            if check_token(token):
                tokens[token].add(p)
            p += 1

    return dict(tokens)

def get_statistics(obj_generator):
    rate = DD(int)

    count = 0
    start_ts = ctime()
    total_len = 0

    for obj in obj_generator():
        count += 1
        total_len += len(data)
        r = extract_token_rate(data)
        for k in r:
            rate[k] += r[k]

    total_time = ctime() - start_ts

    l = [(v, k) for k, v in rate.items()]
    l.sort()

    s, n = 0, 0
    for i in l:
        s += len(i[1]) * i[0]
        n += i[0]

    report = {
        'word_number':  n,
        'token_number': len(l),
        'top_rated':    l[::-1][:10],
        'speed':        format_memory(total_len/total_time, sufix='b/sec'),
        'total_mem':    format_memory(total_len), 
        'average_len':  '{:.3f} symbols'.format(s/n),
        'total_time':   format_time(total_time), 
        'average_time': format_time(total_time / count),
    }

    return report
