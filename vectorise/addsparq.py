import sys,os,json

f1 = open(sys.argv[1])#lcq2
f3 = open(sys.argv[3],'w')#right

sparqldict = {}
d = json.loads(f1.read())
for item in d:
    uid = item['ID']
    sparqldict[uid] = item['sparql_reduced_vars']

with open(sys.argv[2]) as fin: #wrong
    for line in fin:
        arr = json.loads(line.strip())
        arr.append([]) #empty placeholdrs for finents finrels
        arr.append([])
        arr.append(sparqldict[arr[0]])
        print(arr[8],len(arr))
        f3.write(json.dumps(arr)+'\n')


