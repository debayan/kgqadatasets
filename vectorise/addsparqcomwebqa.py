import sys,os,json

f1 = open(sys.argv[1])#comwebqanewinput
f3 = open(sys.argv[3],'w')#newdump

sparqldict = {}
d = json.loads(f1.read())
for item in d:
    uid = item['ID']
    sparqldict[uid] = item['sparql_reduced_vars']

with open(sys.argv[2]) as fin: #wrongdump
    for line in fin:
        arr = json.loads(line.strip())
        arr[8] = sparqldict[arr[0]]
        f3.write(json.dumps(arr)+'\n')


