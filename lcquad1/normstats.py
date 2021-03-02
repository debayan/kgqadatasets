import sys,os,json,rdflib,re,copy

g = rdflib.Graph()
#g.load('https://www.wikidata.org/wiki/Special:EntityData/Q42.rdf')

d = json.loads(open(sys.argv[1]).read())

def checkeq(target,answer):
    print("target: ",target)
    print("answer: ",answer)
    if target.split() == answer.split():
        print('match')
        return True


em = 0
nem = 0
valid = 0
invalid = 0
for idx,item in enumerate(d):
    print(item['uid'])
    print(item['question'])
    target = item['target']
    answer = item['answer']
    if checkeq(target,answer):
        em += 1
    else:
        print("nomatch")
        nem += 1
    try:
        qres = g.query(answer)
        valid += 1
        print("valid sparql")
    except Exception as err:
        print("invalid sparql ",err)
        invalid += 1
    print('................')
    print("exactmatch: ",em, "  notmatch: ",nem," total: ",idx)
    print("validsparql: ",valid," nonvalidsparql: ",invalid)
