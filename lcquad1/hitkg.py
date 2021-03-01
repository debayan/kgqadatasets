import sys,os,json,requests

ret = 0
noret = 0
def hitkg(query):
    try:
        url = 'http://localhost:8892/sparql/'
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
d = json.loads(open(sys.argv[1]).read())
for idx,item in enumerate(d):
    res = {}
    sparql = item['sparql_query']
    print(idx,item['_id'],item['corrected_question'])
    print(sparql)
    result = hitkg(sparql)
    print(result) 
    res['id'] = item['_id']
    res['question'] = item['corrected_question']
    res['sparql'] = sparql
    res['answer'] = result
    print("ret: ",ret," noret: ",noret)
    out.append(res)

f = open(sys.argv[2],'w')
f.write(json.dumps(out,indent=4, sort_keys=True))
f.close()
    
