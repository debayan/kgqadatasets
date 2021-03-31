import sys
import json
import re
import requests
from elasticsearch import Elasticsearch
import random

es = Elasticsearch(host="localhost",port=9200)
es9800 = Elasticsearch(host="localhost",port=9800)
embnf = 0
labnf = 0
embf = 0
labf = 0
#
entembedcache = {}
labelembedcache = {}
relembedcache = {}
labelcache = {}
overallcache = {}
def getkgembedding(enturl):
    if 'dbo:' in enturl:
        enturl = 'http://dbpedia.org/ontology/'+enturl[4:]
    if 'dbp:' in enturl:
        enturl = 'http://dbpedia.org/property/'+enturl[4:]
    if 'res:' in enturl:
        enturl = 'http://dbpedia.org/resource/'+enturl[4:]
    res = es.search(index="dbpediaembedsindex01", body={"query":{"term":{"key":{"value":enturl}}}})
    try:
        embedding = [float(x) for x in res['hits']['hits'][0]['_source']['embedding'].split(' ')] 
        if len(embedding) == 199:
            embedding += [0.0]
        global embf
        embf += 1
        return embedding
    except Exception as e:
        print(enturl,' entity embedding not found')
        global embnf
        embnf += 1
        return 200*[1.0]
    return 200*[1.0]

def gettextmatchmetric(label,word):
    return [fuzz.ratio(label,word)/100.0,fuzz.partial_ratio(label,word)/100.0,fuzz.token_sort_ratio(label,word)/100.0]

def getlabelembedding(enturl):
    if 'dbo:' in enturl:
        enturl = 'http://dbpedia.org/ontology/'+enturl[4:]
    if 'dbp:' in enturl:
        enturl = 'http://dbpedia.org/property/'+enturl[4:]
    if 'res:' in enturl:
        enturl = 'http://dbpedia.org/resource/'+enturl[4:]
    res = es9800.search(index="dbpedialabelindex01", body={"query":{"term":{"uri":{"value":enturl}}}})
    if len(res['hits']['hits']) == 0:
        return [0]*300
    try:
        description = res['hits']['hits'][0]['_source']['dbpediaLabel']
        labelcache[enturl] = description
        r = requests.post("http://localhost:8887/ftwv",json={'chunks': [description]},headers={'Connection':'close'})
        labelembedding = r.json()[0]
        labelembedcache[enturl] = labelembedding
        global labf
        labf += 1
        return labelembedding
    except Exception as e:
        print("getlabelembedding err: ",e)
        global labnf
        labnf += 1
        return [1.0]*300
    return [1.0]*300
    
        
def vectorise(nlquery, sparql):
    if not nlquery:
        return []
    q = re.sub("\s*\?", "", nlquery.strip())
    ents = []
    rels = []
    ents = ['res:'+ent for ent in re.findall( r'res:(.*?) ', sparql) if ent]
    rels = ['dbo:'+rel for rel in re.findall( r'dbo:(.*?) ',sparql) if rel]
    rels += ['dbp:'+rel for rel in re.findall( r'dbp:(.*?) ',sparql) if rel]
    print("sparql: ",sparql)
    print("ents: ",ents)
    print("rels: ",rels)
    
    print("question: ",nlquery)
    print("sparql: ", sparql)
    print("ents: ",ents)
    print("rels: ",rels)
    #print("relations: ",rels)
    candidatetokens = []
    candidatevectors = []
    #questionembedding
    tokens = [token for token in q.split(" ") if token != ""]
    r = requests.post("http://localhost:8887/ftwv",json={'chunks': tokens},headers={'Connection':'close'})
    #print("r: ",r)
    questionembeddings = r.json()
    candidatevectors = [embedding+200*[1.0] for embedding in questionembeddings]#list(map(lambda x: sum(x)/len(x), zip(*questionembeddings)))
    candidatetokens += tokens
    candidatevectors.append(500*[-2.0]) #SEParator
    candidatetokens.append('[SEP]')
    entitycandvecs = []
    entitycandtokens = []
    for ent in ents:
        entityembedding = getkgembedding(ent)
        if len(entityembedding) != 200:
            print("not 200 ",ent,embedding)
            sys.exit(1)
        labelembedding = getlabelembedding(ent)
#            print("ent: ",ent)
#            print("embed: ",entityembedding)
#            print("labelembedding: ",labelembedding)
        entitycandvecs.append(labelembedding+entityembedding) 
        entitycandtokens.append(ent)
    candidatetokens += entitycandtokens
    candidatevectors += entitycandvecs
    #ents done, now rels
    candidatevectors.append(500*[-2.0]) #SEParator
    candidatetokens.append('[SEP]')
    relcandtokens = []
    relcandvecs = []
    for rel in rels:
        relembedding = getkgembedding(rel)
        if len(relembedding) != 200:
            print("not 200 ",rel,relembedding)
            sys.exit(1)
        labelembedding = getlabelembedding(rel)
        #print("rel: ",rel)
        #print("embed: ",relembedding)
        #print("labelembedding: ",labelembedding)
        #sys.exit(1)
        relcandvecs.append(labelembedding+relembedding)
        relcandtokens.append(rel)
    candidatetokens += relcandtokens
    candidatevectors += relcandvecs
#        overallcache[nlquery] = [candidatetokens,candidatevectors,ents,rels,finalentities,finalrels]
    return candidatetokens,candidatevectors,ents,rels
        
        
        
if __name__ == '__main__':
    d = json.loads(open(sys.argv[1]).read())
    f = open(sys.argv[2],'w')
    for item in d:
        uid = item['id']
        question = item['question']
        sparql = item['query_reduced']
        if question and sparql:
            candtokens,candvecs,ents,rels = vectorise(question,sparql)
            f.write(json.dumps([uid,question,candtokens,candvecs,ents,rels,[],[],sparql])+'\n')
        print("embfound : %d  embnotfound: %d  labfound: %d labnotfound: %d"%(embf, embnf,labf,labnf))
    f.close()
