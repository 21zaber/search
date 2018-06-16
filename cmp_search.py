import requests
from pprint import pprint
import wikipedia

res_file_name = '../files/res'

out_file = open(res_file_name, 'w')

def msearch(query):                                      
    h = {'content-type': 'application/json'}
    r = requests.post("http://192.168.77.40:5000/search/", headers=h, json={'query':query})
    search_id = r.text

    r = requests.get("http://192.168.77.40:5000/get_results/{}/{}".format(search_id, 0), headers=h)

    response = r.json()

    data = []
    for i in response['docs']:
        data.append(i['title'])

    out_file.write('Query: {}'.format(query))
    for i, doc in enumerate(response['docs'][:10]):
        out_file.write('\n{}. {}\n {}\n'.format(i, doc['title'], doc['snippet'], doc['q']))
    out_file.write('\n\n\n')

    r = requests.get("http://192.168.77.40:5000/get_results/{}/{}".format(search_id, 0), headers=h)

    response = r.json()

    for i in response['docs']:
        data.append(i['title'])



    return data

def wiki_search(q):
    data = wikipedia.search(q, results=100)
    return data
    


QUERIES_FILE = '../files/query_list'

queries = [i[:-1] for i in open(QUERIES_FILE, 'r').readlines()]

for q in queries:
    print('"{}"'.format(q))
    mr = msearch(q)
    wr = wiki_search(q)
    print(set(mr) & set(wr))
    print('Intersection size: {}\n'.format(len(set(mr) & set(wr))))

