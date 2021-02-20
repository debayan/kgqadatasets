import sys,os,json,re,copy

d = json.loads(open(sys.argv[1]).read())


newvars = ['?vr0','?vr1','?vr2','?vr3','?vr4','?vr5','?vr6','?vr7','?vr8','?vr9','?vr10','?vr11','?vr12']
newarr = []
for item in d:
    newitem = copy.deepcopy(item)
    sparql = item['sparql_query']
    sparql_split = sparql.replace('(',' ( ').replace(')',' ) ').replace('{',' { ').replace('}',' } ').replace('\n',' ').split()
    variables = set([x for x in sparql_split if x[0] == '?'])
    for idx,var in enumerate(variables):
        sparql = sparql.replace(var,newvars[idx])
    sparql = sparql.replace('PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#> PREFIX : <http://rdf.freebase.com/ns/> ','').replace('\n',' ').replace('(',' ( ').replace(')',' ) ').replace('{',' { ').replace('}',' } ')
    newitem['sparql_query_reduced_vars'] = sparql
    newarr.append(newitem)

f = open(sys.argv[2],'w')
f.write(json.dumps(newarr, indent=4, sort_keys=True))
f.close()
