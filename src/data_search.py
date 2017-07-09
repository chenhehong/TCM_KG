# -*- coding:utf-8 -*-
from src.datastore.graph.neo_datagraph_opt import NeoDataGraphOpt
from src.datastore.mysql2neo4j import Mysql2Neo4j


class DataSearch(object):

    SEARCHVALUEKEY = 'value'
    SEARCHWEIGHTKEY = 'weight'

    def get_symptom_from_medicine(self,medicine):
        return self.get_public_link_node(Mysql2Neo4j.SYMPTOMTAG, Mysql2Neo4j.SYMPTOM2MEDICINETAG, Mysql2Neo4j.MEDICINETAG, 'name',medicine)

    def get_medicine_from_symptom(self,syptom):
        return self.get_public_link_node(Mysql2Neo4j.MEDICINETAG,Mysql2Neo4j.SYMPTOM2MEDICINETAG,Mysql2Neo4j.SYMPTOMTAG,'name',syptom,firstArrow='<-',secondArrow='-')

    def get_public_link_node(self,startNodeTag,relationTag,endNodeTag,endNodeKey,endNodeValue,relationOrderKey = 'weight',seq = 'DESC',firstArrow='-',secondArrow='->'):
        if len(endNodeValue)==1:
            cql = 'MATCH (s1:'+startNodeTag+')'+firstArrow+'[r1:'+relationTag+']'+secondArrow+'(e1:'+endNodeTag+') WHERE e1.'+endNodeKey+'={1} return s1 as '\
                  +self.SEARCHVALUEKEY+',r1.'+relationOrderKey+' as '+self.SEARCHWEIGHTKEY+' order by r1.'+relationOrderKey+' '+seq
            parameters = {"1":endNodeValue[0]}
            print cql
            print parameters
            neoDb = NeoDataGraphOpt()
            return neoDb.graph.data(cql,parameters)
        i = 0
        cql = ''
        parameters = {}
        for value in endNodeValue:
            i+=1
            parameters[str(i)] = value
            s = 's'+str(i)
            ssub = 's'+str(i-1)
            r = 'r'+str(i)
            e = 'e'+str(i)
            match = 'MATCH ('+s+':'+startNodeTag+')'+firstArrow+'['+r+':'+relationTag+' ]'+secondArrow+'('+e+':'+endNodeTag+') '
            if(i==1):
                cql+=match
                cql+='WHERE '+e+'.'+endNodeKey+'={'+str(i)+'} with '+s+','+r+' '
            elif(i<len(endNodeValue)):
                cql+=match
                cql+='WHERE '+e+'.'+endNodeKey+'={'+str(i)+'} and '+s+'='+ssub
                rTotal = ''
                flag = False
                for j in range(i):
                    if flag:
                        rTotal+=','
                    else:
                        flag=True
                    rTotal+='r'+str(j+1)
                cql+=' with '+s+','+rTotal+' '
            elif(i==len(endNodeValue)):
                cql+=match
                cql += 'WHERE ' + e + '.' + endNodeKey + '={' + str(i) + '} and ' + s + '=' + ssub+' '
                w = ''
                flag = False
                for j in range(i):
                    if flag:
                        w+='+'
                    else:
                        flag = True
                    w+='r'+str(j+1)+'.'+relationOrderKey
                cql +='return '+s+' as '+self.SEARCHVALUEKEY+','+w+' as '+self.SEARCHWEIGHTKEY+' order by '+w+' '+seq
        print cql
        print parameters
        neoDb = NeoDataGraphOpt()
        return neoDb.graph.data(cql, parameters)


if __name__ =='__main__':
    dataSerach = DataSearch()
    symptoms = dataSerach.get_symptom_from_medicine(['麻黄','当归'])
    for symptom in symptoms:
        print '症状：'+symptom[DataSearch.SEARCHVALUEKEY]['name'].encode('utf8')+' 权重：'+str(symptom[DataSearch.SEARCHWEIGHTKEY])
    medicines = dataSerach.get_medicine_from_symptom(['头痛','发热'])
    for medicine in medicines:
        print '中药：'+medicine[DataSearch.SEARCHVALUEKEY]['name'].encode('utf8')+' 权重：'+str(medicine[DataSearch.SEARCHWEIGHTKEY])