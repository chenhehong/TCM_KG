# -*- coding:utf-8 -*-

from src.datastore.mysql2neo4j import Mysql2Neo4j
from src.datastore.neo4j_opt import Neo4jOpt


class BaseDataQuery(object):

    SEARCHVALUEKEY = 'value'
    SEARCHWEIGHTKEY = 'weight'
    SEARCHIDSKEY = 'ids'

    def get_symptom_from_medicine(self,medicine):
        return self.get_public_link_node(Mysql2Neo4j.SYMPTOMTAG, Mysql2Neo4j.SYMPTOM2MEDICINETAG, Mysql2Neo4j.MEDICINETAG, 'name',medicine)

    def get_tonguezhi_from_medicine(self,medicine):
        return self.get_public_link_node(Mysql2Neo4j.TONGUEZHITAG, Mysql2Neo4j.TONGUEZHI2MEDICINETAG, Mysql2Neo4j.MEDICINETAG, 'name',medicine)

    def get_tonguetai_from_medicine(self, medicine):
        return self.get_public_link_node(Mysql2Neo4j.TONGUETAITAG, Mysql2Neo4j.TONGUETAI2MEDICINETAG, Mysql2Neo4j.MEDICINETAG, 'name',medicine)

    def get_pulse_from_medicine(self,medicine):
        return self.get_public_link_node(Mysql2Neo4j.PULSETAG, Mysql2Neo4j.PULSE2MEDICINETAG, Mysql2Neo4j.MEDICINETAG, 'name',medicine)

    def get_medicine_from_symptom(self,syptom):
        return self.get_public_link_node(Mysql2Neo4j.MEDICINETAG,Mysql2Neo4j.SYMPTOM2MEDICINETAG,Mysql2Neo4j.SYMPTOMTAG,'name',syptom,firstArrow='<-',secondArrow='-')

    def get_medicine_from_tonguezhi(self,tonguezhi):
        return self.get_public_link_node(Mysql2Neo4j.MEDICINETAG,Mysql2Neo4j.TONGUEZHI2MEDICINETAG,Mysql2Neo4j.TONGUEZHITAG,'name',tonguezhi,firstArrow='<-',secondArrow='-')

    def get_medicine_from_tonguetai(self,tonguetai):
        return self.get_public_link_node(Mysql2Neo4j.MEDICINETAG,Mysql2Neo4j.TONGUETAI2MEDICINETAG,Mysql2Neo4j.TONGUETAITAG,'name',tonguetai,firstArrow='<-',secondArrow='-')

    def get_medicine_from_pulse(self,pulse):
        return self.get_public_link_node(Mysql2Neo4j.MEDICINETAG,Mysql2Neo4j.PULSE2MEDICINETAG,Mysql2Neo4j.PULSETAG,'name',pulse,firstArrow='<-',secondArrow='-')

    def get_public_link_node(self,startNodeTag,relationTag,endNodeTag,endNodeKey,endNodeValue,relationOrderKey = 'weight',seq = 'DESC',firstArrow='-',secondArrow='->'):
        if len(endNodeValue)==1:
            cql = 'MATCH (s1:'+startNodeTag+')'+firstArrow+'[r1:'+relationTag+']'+secondArrow+'(e1:'+endNodeTag+') WHERE e1.'+endNodeKey+'={1} return s1 as '\
                  +self.SEARCHVALUEKEY+',r1.'+Mysql2Neo4j.RELATIONWEIGHT+' as '+self.SEARCHWEIGHTKEY+',r1.'+Mysql2Neo4j.RELATIONTID+' as '+self.SEARCHIDSKEY+' order by r1.'+relationOrderKey+' '+seq
            parameters = {"1":endNodeValue[0]}
            # print cql
            # print parameters
            neoDb = Neo4jOpt()
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
                tidReturn = ''
                for j in range(i):
                    tidReturn=',r'+str(j+1)+'.'+Mysql2Neo4j.RELATIONTID+' as '+self.SEARCHIDSKEY+str(j+1)+tidReturn
                cql +='return '+s+' as '+self.SEARCHVALUEKEY+','+w+' as '+self.SEARCHWEIGHTKEY+tidReturn+' order by '+w+' '+seq
        # print cql
        # print parameters
        neoDb = Neo4jOpt()
        return neoDb.graph.data(cql, parameters)

if __name__ =='__main__':
    dataSerach = BaseDataQuery()
