import sys,os,json,requests

ret = 0
noret = 0
def hitkg(query):
    try:
        url = 'http://ltdocker:8894/sparql/' #8892 for dbpedia1604, ltdocker:8894 for dbpedia1510
        r = requests.get(url, params={'format': 'json', 'query': query})
        json_format = r.json()
        #print(entid,json_format)
        results = json_format
        global ret
        ret += 1
        return results
    except Exception as err:
        print(err)
        global noret
        noret += 1
        return ''

out = []
good = 0
bad = 0
goodids = []
d = json.loads(open('qald5train311.json').read())
d += json.loads(open('qald5test311.json').read())

print(len(d), " entries total ")

for idx,item in enumerate(d):
    res = {}
    if 'query' in item:
        sparql = item['query']
    if 'pseudoquery'  in item:
        sparql = item['pseudoquery']
    #print(idx,item['id'],item['question'])
    #print(sparql)
    result = hitkg(sparql)
    #print(result) 
    if 'results' in result:
        if 'bindings' in result['results']:
            if len(result['results']['bindings']) > 0:
                good += 1
                goodids.append(item['id'])
            else:
                print(idx,item['id'],item['question'])
                print(sparql)
                print(result)
                bad += 1
    elif 'boolean' in result:
        good += 1
        goodids.append(item['id'])
    else:
        print(idx,item['id'],item['question'])
        print(sparql)
        print(result)
        bad += 1
    
   
    res['id'] = item['id']
    res['question'] = item['question']
    res['sparql'] = sparql
    res['answer'] = result
    out.append(res)
#
#f = open(sys.argv[2],'w')
#f.write(json.dumps(goodids))
#f.close
#sys.exit(1)
#f = open(sys.argv[2],'w')
#f.write(json.dumps(out,indent=4, sort_keys=True))
#f.close()
print("goodids: ",goodids)
print(len(d),good,bad)
