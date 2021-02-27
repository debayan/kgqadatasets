import sys,os,json



s = json.loads(open(sys.argv[1]).read())
symbols = set()
for item in s:
    sparql = item['sparql_reduced_vars']#.replace('{',' { ').replace('}',' } ').replace('COUNT(','COUNT ( ').replace('vr0)','vr0 ) ').replace('vr1)','vr1 )')
    tokens = sparql.split(' ')
    for token in tokens:
        if 'ns:' in token or 'xsd:' in token or '"' in token:
            continue
        symbols.add(token)
for symb in symbols:
    print(symb)
