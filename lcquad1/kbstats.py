import sys,os,json,rdflib,re,copy,requests


def hitkg(query):
    try:
        url = 'http://localhost:8892/sparql/'
        r = requests.get(url, params={'format': 'json', 'query': query})
        json_format = r.json()
        #print(entid,json_format)
        results = json_format
        return results
    except Exception as err:
        print(err)
        return ''

#goldd = {}
#goldq = {}
#d = json.loads(open(sys.argv[1]).read()) #lcq1gold
#
#for item in d:
#    result = hitkg(item['sparql_query'])
##    print(item)
##    print(result)
#    goldd[str(item['_id'])] = result
#    goldq[str(item['_id'])] = item['sparql_query']

d = json.loads(open(sys.argv[2]).read()) #model output

em = 0
nem = 0
qem = 0
qnem = 0
for idx,item in enumerate(d):
    print(item['uid'])
    print(item['question'])
    target = item['target']
    answer = item['answer']
    ents = item['goldents']
    rels = item['goldrels']
    print("target: ",target)
    print("answer: ",answer)
    if target == answer:
        qem += 1
        print("querymatch")
    else:
        qnem += 1
        print("querynotmatch")
    for idx,ent in enumerate(ents):
        if ent:
            target = target.replace('entpos@@'+str(idx+1),ent)
    for idx,rel in enumerate(rels):
        if rel:
            target = target.replace('predpos@@'+str(idx+1),rel)
    resulttarget = hitkg(target)
    for idx,ent in enumerate(ents):
        if ent:
            answer = answer.replace('entpos@@'+str(idx+1),ent)
    for idx,rel in enumerate(rels):
        if rel:
            answer = answer.replace('predpos@@'+str(idx+1),rel)
    resultanswer = hitkg(answer)
    if resulttarget == resultanswer:
        print("match")
        em += 1
    else:
        print("nomatch")
        nem += 1
    print("target: ",target)
    print("answer: ",answer)
    print("gold: ",resulttarget)
    print("result: ",resultanswer)
    print('................')
    print("exactmatch: ",em, "  notmatch: ",nem," total: ",idx)
    print("querymatch: ",qem," querynotmatch: ",qnem)
