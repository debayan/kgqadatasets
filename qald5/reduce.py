import sys,os,json
import xml.etree.ElementTree as ET

tree = ET.parse(sys.argv[1])
root = tree.getroot()

questions = []

for child in root:
    d = {}
    print(child.tag,child.attrib)
    d['id'] = child.attrib['id']
    for subchild in child:
        if subchild.tag == 'string':    
            if 'lang' in subchild.attrib:
                if subchild.attrib['lang'] == 'en':
                    d['question'] = subchild.text
        if subchild.tag == 'query':
            d['query'] = subchild.text.replace('\n','')
        print('    ',subchild.tag, subchild.attrib, subchild.text)
    print(d)
    questions.append(d)

f = open(sys.argv[2],'w')
f.write(json.dumps(questions, sort_keys=True, indent=4))
f.close()
