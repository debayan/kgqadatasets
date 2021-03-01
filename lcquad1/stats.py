import sys,os,json

d = json.loads(open(sys.argv[1]).read())

em = 0
nem = 0
for idx,item in enumerate(d):
    print(item['uid'])
    print(item['question'])
    target = item['target']
    answer = item['answer']
    if target.split() == answer.split():
        em += 1
        print('match')
        print(target)
        print(answer)
    else:
        answer = answer.replace('?vr0','?x').replace('?vr1','?y').replace('?x','?vr1').replace('?y','?vr0')
        if target.split() == answer.split():
            print("match")
            print(target)
            print(answer)
            em += 1
        else:
            print("nomatch")
            print(target)
            print(answer)
            nem+=1
    print('................')
print("exactmatch: ",em, "  notmatch: ",nem,idx)
