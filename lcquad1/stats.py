import sys,os,json,rdflib

g = rdflib.Graph()
#g.load('https://www.wikidata.org/wiki/Special:EntityData/Q42.rdf')

d = json.loads(open(sys.argv[1]).read())

em = 0
nem = 0
valid = 0
invalid = 0
for idx,item in enumerate(d):
    print(item['uid'])
    print(item['question'])
    target = item['target']
    answer = item['answer']
    if target.split() == answer.split():
        em += 1
        print('match')
        print(target)
        print(answer)
    else:
        answer = answer.replace('?vr0','?x').replace('?vr1','?y').replace('?x','?vr1').replace('?y','?vr0')
        if target.split() == answer.split():
            print("match")
            print(target)
            print(answer)
            em += 1
        else:
            print("nomatch")
            print(target)
            print(answer)
            nem+=1
    try:
        answer = answer.replace('entpos@@1','http://www.wikidata.org/entity/Q42').replace('entpos@@2','http://www.wikidata.org/entity/Q42').replace('predpos@@1','http://www.wikidata.org/entity/P1477').replace('predpos@@2','http://www.wikidata.org/entity/P1477')
        qres = g.query(answer)
        valid += 1
        print("valid sparql")
    except Exception as err:
        print("invalid sparql ",err)
        invalid += 1
    print('................')
print("exactmatch: ",em, "  notmatch: ",nem," total: ",idx)
print("validsparql: ",valid," nonvalidsparql: ",invalid)
