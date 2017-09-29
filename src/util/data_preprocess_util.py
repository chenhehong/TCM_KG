# -*- coding:utf-8 -*-
import codecs


with codecs.open('../../data/fangjidaquan.txt','r',encoding='utf-8') as f:
    f.readline()
    idNum = 1
    data = ''
    for line in f:
        splits = line.split("||")
        medicines = splits[3].strip()
        symptoms = splits[5].strip()
        if(len(medicines)==0 or len(symptoms)==0):
            continue
        data += str(idNum)+u'||'+symptoms+u'||'+medicines+'\n'
        idNum+=1
with codecs.open('../../data/fangji_qing.txt','r',encoding='utf-8') as f:
    f.readline()
    for line in f:
        splits = line.split("||")
        medicines = splits[11].strip()
        symptoms = splits[3].strip()
        if(len(medicines)==0 or len(symptoms)==0):
            continue
        data += str(idNum)+u'||'+symptoms+u'||'+medicines+'\n'
        idNum+=1
with open('../../data/symptom2medicine.txt', 'w') as f:
    f.write(data.encode('utf-8'))


