import xml.etree.ElementTree as ET
from wikidb import DB
from time import gmtime, strftime, time as ctime
import re


cleanrs = [
           re.compile('<.*?>'), 
#           re.compile('{{.*?}}'),
           re.compile('\[*[0-9]\]'),
#           re.compile('\[\['),
#           re.compile('\]\]'),
]
brackets = [
    ('{{', '}}'),
    ('{\|', '\|}'),
    ('\[\[', '\]\]'),
#    ('\[http', '\]'),
]

def get_ts():
    return strftime("%Y.%m.%d %H:%M:%S", gmtime())

def extract_xml(s):
    s = s.replace('\n', ' ')
    s = s.strip()
    return s

def clear_brackets(s, ob, cb): 
    ore = re.compile(ob)
    cre = re.compile(cb)

    obl = []
    obl.append(ore.search(s))
    while obl[-1]:
        obl.append(ore.search(s, obl[-1].end()))
    cbl = []
    cbl.append(cre.search(s))
    while cbl[-1]:
        cbl.append(cre.search(s, cbl[-1].end()))
    obl = [i.start() for i in obl[:-1]]
    cbl = [i.end() for i in cbl[:-1]]
    #print(ob, obl, cbl)
    l, r = 0, 0
    rmv = []
    while l < len(obl) and r < len(cbl):
        while r+1 < len(cbl) and obl[l] >= cbl[r]:
            r += 1
        lc = obl[l] 
        while r+1 < len(cbl) and l+1 < len(obl) and obl[l+1] < cbl[r]:
            l += 1
            r += 1
        rc = cbl[r]
        if lc < rc:
            rmv.append((lc, rc,))
        l += 1
        r += 1
    for i in rmv[::-1]:
        s = s[:i[0]] + s[i[1]:]
    return s


def extract_page(s):    
    data = {}
    
    def clear_spesials(s, cleanr):
        return re.sub(cleanr, '', s)
    
    root = ET.fromstring(s)
    rev = root.find('revision')
    text = str(rev.find('text').text)

   #print(text)
   #print('\n\n', '*'*20, '\n\n')
   #print(len(text))
    
    for b in brackets:
        text = clear_brackets(text, *b)
    for cleanr in cleanrs:
        #print(type(text), len(text))
        text = clear_spesials(text, cleanr)
    
    data['text'] = text.strip()    
    data['title'] = root.find('title').text
    data['id'] = root.find('id').text
    
    ts = rev.find('timestamp').text
    
    data['timestamp'] = ts
    #print(data['text'])
    
    return data


f = open('enwiki-20170820-pages-articles.xml', 'r')

start_str = 0#5000000

ferr = open('fails', 'w')

db = DB()
db.recreate()

buf = ''
i = 0
a = 0 # success articles 
e = 0 #   error articles 
redir = 0 # redirect articles


print("[{}] Parsing started.".format(get_ts()))
start_ts = ctime()

try:
    while True:
        s = f.readline()[:-1]
        i += 1

       #if not s:
       #    break

        if i < start_str:
            if i % 1000000 == 0:
                print("[{}] Skiped {} strings.".format(get_ts(), i))
            continue

        buf += s
        
        if len(buf) > 10000000:
            print("long article")
        
        if '</page>' in s:
            xml = extract_xml(buf)
            if xml.startswith('<page>'):
                data = extract_page(xml)   

                if data and data.get('text'):
                    if data['text'].startswith('#REDIRECT'):
                        redir += 1
                    else:
                        db.insert(data)
            a += 1
           #if a > 5:
           #    break

            if a % 10000 == 0:
                print("[{}] Added {} articles.".format(get_ts(), a))
           #except:
           #    e += 1
           #    ferr.write(buf+'\n')

            buf = ""
except KeyboardInterrupt:
    print("Keyboard interrupt.")
    

print("[{}] Parsing finished, {} articles parsed, {} strings parsed".format(get_ts(), a, i))
print("Total redirects: {}".format(redir))
print("Total errors: {}({:.2f}%)".format(e, e / a * 100))

total_time = ctime() - start_ts
from datetime import datetime
d = datetime.fromtimestamp(total_time)
print("Total time spend: {} days {} hours {} minutes {} seconds".format(d.day-1, d.hour-3, d.minute, d.second))
print("Total seconds", total_time)

