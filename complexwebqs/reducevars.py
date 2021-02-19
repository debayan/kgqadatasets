import sys,os,json,re,copy

d = json.loads(open(sys.argv[1]).read())


newvars = ['?vr0','?vr1','?vr2','?vr3','?vr4','?vr5','?vr6','?vr7','?vr8','?vr9','?vr10','?vr11','?vr12']
newarr = []
for item in d:
    newitem = copy.deepcopy(item)
    sparql = item['sparql']
    sparql_split = sparql.replace('(',' ( ').replace(')',' ) ').replace('{',' { ').replace('}',' } ').replace('\n',' ').split()
    print(sparql)
    variables = set([x for x in sparql_split if x[0] == '?'])
    print(variables)
    for idx,var in enumerate(variables):
        sparql = sparql.replace(var,newvars[idx])
    print(sparql)
    newitem['sparql_reduced_vars'] = sparql
    newarr.append(newitem)

f = open(sys.argv[2],'w')
f.write(json.dumps(newarr, indent=4, sort_keys=True))
f.close()
