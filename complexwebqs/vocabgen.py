import sys,os,json



s = json.loads(open(sys.argv[1]).read())
symbols = set()
for item in s:
    sparql = item['sparql_reduced_vars']
    tokens = sparql.split(' ')
    for token in tokens:
        if 'ns:' in token:
            continue
        symbols.add(token)

s = json.loads(open(sys.argv[2]).read())
symbols = set()
for item in s:
    sparql = item['sparql_reduced_vars']
    tokens = sparql.split(' ')
    for token in tokens:
        if 'ns:' in token:
            continue
        symbols.add(token)

s = json.loads(open(sys.argv[3]).read())
symbols = set()
for item in s:
    sparql = item['sparql_reduced_vars']
    tokens = sparql.split(' ')
    for token in tokens:
        if 'ns:' in token:
            continue
        symbols.add(token)

for symb in symbols:
    print(symb)
