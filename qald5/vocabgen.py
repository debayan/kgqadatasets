import sys,os,json



s = json.loads(open(sys.argv[1]).read())
symbols = set()
for item in s:
    sparql = item['query_reduced'].replace('{',' { ').replace('}',' } ').replace('(', ' ( ').replace(')',' ) ')
    tokens = sparql.split(' ')
    for token in tokens:
        if 'dbo:' in token or 'dbp:' in token or 'res:' in token:
            continue
        symbols.add(token)

symbols.add('dbo:')
symbols.add('dbp:')
symbols.add('res:')

s = json.loads(open(sys.argv[2]).read())
symbols = set()
for item in s:
    sparql = item['query_reduced'].replace('{',' { ').replace('}',' } ').replace('(', ' ( ').replace(')',' ) ')
    tokens = sparql.split(' ')
    for token in tokens:
        if 'dbo:' in token or 'dbp:' in token or 'res:' in token:
            continue
        symbols.add(token)
for symb in symbols:
    print(symb)
