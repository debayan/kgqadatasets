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

def getlabelembedding(entid):
    res = es9800.search(index="dbpedialabelindex01", body={"query":{"term":{"uri":{"value":entid}}}})
    if len(res['hits']['hits']) == 0:
        return [0]*300
    try:
        description = res['hits']['hits'][0]['_source']['dbpediaLabel']
        labelcache[entid] = description
        r = requests.post("http://localhost:8887/ftwv",json={'chunks': [description]},headers={'Connection':'close'})
        labelembedding = r.json()[0]
        labelembedcache[entid] = labelembedding
        global labf
        labf += 1
        return labelembedding
    except Exception as e:
        print("getlabelembedding err: ",e)
        global labnf
        labnf += 1
        return [1.0]*300
    return [1.0]*300
    
        
def getdisambcands(ents):
    disambcands = []
    if len(ents) == 0:
        return []
    listsize = int((30.0 - len(ents)) / len(ents))
    for ent in ents:
        try:
            res = es.search(index="wikidataentitylabelindex01", body={"query":{"multi_match":{"query":labelcache[ent]}},"size":listsize})
            esresults = res['hits']['hits']
            if len(esresults) > 0:
                for entidx,esresult in enumerate(esresults):
                    uri = esresult['_source']['uri']
                    disambcands.append(uri[37:])
        except Exception as err:
            print("oops: ",err)
            
    return disambcands
    
    
def getmorerels(rels, propdict):
    relcands = []
    return random.sample(propdict.keys(),30 - len(rels))



def vectorise(nlquery, sparql):
    if not nlquery:
        return []
    q = re.sub("\s*\?", "", nlquery.strip())
    ents = []
    rels = []
    entsrels = re.findall( r'<(.*?)>', sparql)
    for er in entsrels:
        if '/property/' in er or '/ontology/' in er:
            rels.append(er)
        else:
            ents.append(er)
    print("question: ",nlquery)
    print("sparql: ", sparql)
    print("er: ",entsrels)
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
        uid = item['_id']
        question = item['corrected_question']
        sparql = item['sparql_query_reduced_vars']
        if question and sparql:
            candtokens,candvecs,ents,rels = vectorise(question,sparql)
            f.write(json.dumps([uid,question,candtokens,candvecs,ents,rels,[],[],sparql])+'\n')
        print("embfound : %d  embnotfound: %d  labfound: %d labnotfound: %d"%(embf, embnf,labf,labnf))
    f.close()
