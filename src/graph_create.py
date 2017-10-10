# -*- coding:utf-8 -*-
import codecs

from src.datastore.neo4j_opt import Neo4jOpt


class GraphCreate(object):

    MEDICINETAG = 'medicine'
    SYMPTOMTAG = "symptom"
    SYMPTOM2MEDICINETAG = "symptom2medicine"
    NODENAME = "name"

    def store_zhongyaocai_file(self):
        print('开始保存中药材文件')
        neoDb = Neo4jOpt()
        neoDb.graph.data('MATCH (n) DETACH DELETE n')
        with codecs.open('../data/medicine2symptom.txt', 'r', encoding='utf-8') as f:
            for line in f:
                splits = line.split('||')
                medicine = splits[1]
                print medicine
                symptomsList = splits[2].split(',')
                medicineNode = neoDb.selectFirstNodeElementsFromDB(self.MEDICINETAG, properties={self.NODENAME: medicine})
                if (medicineNode == None):
                    medicineNode = neoDb.createNode([self.MEDICINETAG],
                                     {self.NODENAME: medicine})
                for symptom in symptomsList:
                    symptomNode = neoDb.selectFirstNodeElementsFromDB(self.SYMPTOMTAG, properties={self.NODENAME: symptom})
                    if (symptomNode == None):
                        symptomNode = neoDb.createNode([self.SYMPTOMTAG],
                                         {self.NODENAME:symptom})
                    s2mrel = neoDb.selectFirstRelationshipsFromDB(symptomNode, self.SYMPTOM2MEDICINETAG, medicineNode)
                    if (s2mrel == None):
                        neoDb.createRelationship(self.SYMPTOM2MEDICINETAG, symptomNode, medicineNode)

if __name__ == '__main__':
    graphCreate = GraphCreate()
    graphCreate.store_zhongyaocai_file()