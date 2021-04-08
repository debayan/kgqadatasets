import sys
import json
import re
import requests
from elasticsearch import Elasticsearch
import random

labeld = json.loads(open('compwebqslabels.json').read())

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
    res = es.search(index="freebaseembedindex01", body={"query":{"term":{"key":{"value":enturl}}}})
    try:
        embedding = [float(x) for x in res['hits']['hits'][0]['_source']['embedding']]
        global embf
        embf += 1
        return embedding
    except Exception as e:
        print(enturl,' entity embedding not found')
        global embnf
        embnf += 1
        return 50*[1.0]
    return 50*[1.0]

def gettextmatchmetric(label,word):
    return [fuzz.ratio(label,word)/100.0,fuzz.partial_ratio(label,word)/100.0,fuzz.token_sort_ratio(label,word)/100.0]

def getlabelembedding(enturl):
    global labnf
    try:
        if enturl in labeld:
            label = labeld[enturl]
            r = requests.post("http://localhost:8887/ftwv",json={'chunks': [label]},headers={'Connection':'close'})
            labelembedding = r.json()[0]
            labelembedcache[enturl] = labelembedding
            global labf
            labf += 1
            return labelembedding
        else:
            labnf += 1
            return [1.0]*300
    except Exception as e:
        print("getlabelembedding err: ",e)
        labnf += 1
        return [1.0]*300
    return [1.0]*300
    
        
def vectorise(nlquery, sparql):
    if not nlquery:
        return []
    q = re.sub("\s*\?", "", nlquery.strip())
    ents = []
    rels = []
    sparql = sparql.replace('(',' ( ').replace(')',' ) ')
    entsrels = re.findall( r'ns:(.*?) ', sparql)
    for item in entsrels:
        if item[:2] == 'm.':
            ents.append(item)
        else:
            rels.append(item)
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
    candidatevectors = [embedding+50*[1.0] for embedding in questionembeddings]#list(map(lambda x: sum(x)/len(x), zip(*questionembeddings)))
    candidatetokens += tokens
    candidatevectors.append(350*[-2.0]) #SEParator
    candidatetokens.append('[SEP]')
    entitycandvecs = []
    entitycandtokens = []
    for ent in ents:
        entityembedding = getkgembedding(ent)
        if len(entityembedding) != 50:
            print("not 50 ",ent,embedding)
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
    candidatevectors.append(350*[-2.0]) #SEParator
    candidatetokens.append('[SEP]')
    relcandtokens = []
    relcandvecs = []
    for rel in rels:
        relembedding = getkgembedding(rel)
        if len(relembedding) != 50:
            print("not 50 ",rel,relembedding)
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
        uid = item['ID']
        question = item['question']
        sparql = item['sparql_reduced_vars']
        if question and sparql:
            candtokens,candvecs,ents,rels = vectorise(question,sparql)
            f.write(json.dumps([uid,question,candtokens,candvecs,ents,rels,[],[],sparql])+'\n')
        print("embfound : %d  embnotfound: %d  labfound: %d labnotfound: %d"%(embf, embnf,labf,labnf))
    f.close()
