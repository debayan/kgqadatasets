import sys,os,json,rdflib,re,copy

g = rdflib.Graph()
#g.load('https://www.wikidata.org/wiki/Special:EntityData/Q42.rdf')

d = json.loads(open(sys.argv[1]).read())

def checkeq(target,answer):
    if target.split() == answer.split():
        print('match')
        print("target: ",target)
        print("answer: ",answer)
        return True
    else:
        answer = answer.replace('?vr0','?x').replace('?vr1','?y').replace('?x','?vr1').replace('?y','?vr0')
        if target.split() == answer.split():
            print("match")
            print("target: ",target)
            print("answer: ",answer)
            return True
        else:
            print("nomatch")
            print("target: ",target)
            print("answer: ",answer)
            return False


em = 0
nem = 0
valid = 0
invalid = 0
for idx,item in enumerate(d):
    print(item['uid'])
    print(item['question'])
    target = item['target']
    answer = item['answer']
    if checkeq(target,answer):
        em += 1
    else:
        nem += 1
    try:
        qres = g.query(answer)
        valid += 1
        print("valid sparql")
    except Exception as err:
        print("invalid sparql ",err)
        invalid += 1
        er = err.__repr__()
        print("er: ",er)
        if "found '<'  (at char" in er:
            char = re.findall( r'found \'<\'  \(at char (.*?)\),', er)
            print(char)
            char = int(char[0])
            #print("char: ",char)
            editanswer = answer[:char-5] + '?vr1 . ' + answer[char:]
            print("wrong query: ",answer)
            print("edit  query: ",editanswer)
            if checkeq(target,editanswer):
                print("corrected match")
                invalid -= 1
                valid += 1 
                em += 1
                nem -= 1
    print('................')
    print("exactmatch: ",em, "  notmatch: ",nem," total: ",idx)
    print("validsparql: ",valid," nonvalidsparql: ",invalid)
