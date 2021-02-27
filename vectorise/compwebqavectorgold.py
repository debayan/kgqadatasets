import sys
import json
import re
import requests
from elasticsearch import Elasticsearch
import random
import numpy as np

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
def getkgembedding(entrel,entreld,entrelvec):
    entrel = entrel.replace('ns:','')
    try:
        global embf
        emb = list(entrelvec[int(entreld[entrel])])
        embf += 1
        return emb + 150*[0.0]
    except Exception as err:
        print(entrel,err)
        global embnf
        embnf += 1
        return 200*[0.0]

def gettextmatchmetric(label,word):
    return [fuzz.ratio(label,word)/100.0,fuzz.partial_ratio(label,word)/100.0,fuzz.token_sort_ratio(label,word)/100.0]

def getlabelembedding(entid):
    
    try:
        url = 'https://aqqu.cs.uni-freiburg.de/sparql/'
        query = '''PREFIX ns: <http://rdf.freebase.com/ns/>
                   PREFIX rdfs:<http://www.w3.org/2000/01/rdf-schema#>
                   PREFIX rdf:<http://www.w3.org/1999/02/22-rdf-syntax-ns#>
                   select ?o where {
                   %s ns:type.object.name ?o
                   }'''%(entid)
        r = requests.get(url, params={'format': 'json', 'query': query})
        json_format = r.json()
        #print(entid,json_format)
        label = json_format['results']['bindings'][0]['o']['value']
        #print(label)
        r = requests.post("http://localhost:8887/ftwv",json={'chunks': [label]},headers={'Connection':'close'})
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



def vectorise(nlquery, sparql, ents, rels,entd,entvec,reld,relvec):
    if not nlquery:
        return []
    q = re.sub("\s*\?", "", nlquery.strip())
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
        entityembedding = getkgembedding(ent, entd, entvec)
        labelembedding = getlabelembedding(ent)
##            print("ent: ",ent)
##            print("embed: ",entityembedding)
##            print("labelembedding: ",labelembedding)
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
        relembedding = getkgembedding(rel,reld,relvec)
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

    entd = {}
    reld = {}
    
    lines = open('Freebase/knowledge graphs/entity2id.txt').readlines()
    
    for line in lines[1:]:
        ent,uid = line.strip().split('\t')
        entd[ent] = uid
    
    lines = open('Freebase/knowledge graphs/relation2id.txt').readlines()
    
    for line in lines[1:]:
        rel,uid = line.strip().split('\t')
        reld[rel] = uid
    del lines
    entvec = np.memmap('Freebase/embeddings/dimension_50/transe/entity2vec.bin' , dtype='float32', mode='r', shape=(86054151,50))
    relvec = np.memmap('Freebase/embeddings/dimension_50/transe/relation2vec.bin' , dtype='float32', mode='r', shape=(14824,50))

    d = json.loads(open(sys.argv[1]).read())
    f = open(sys.argv[2],'w')
    for item in d:
        uid = item['ID']
        question = item['question']
        sparql = item['sparql_reduced_vars']
        entities = item['entities']
        relations = item['relations']
        if question and sparql:
            candtokens,candvecs,ents,rels = vectorise(question,sparql,entities,relations,entd,entvec,reld,relvec)
            f.write(json.dumps([uid,question,candtokens,str(candvecs),ents,rels,sparql])+'\n')
        print("embfound : %d  embnotfound: %d  labfound: %d labnotfound: %d"%(embf, embnf,labf,labnf))
    f.close()
