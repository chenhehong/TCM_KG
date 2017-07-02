# -*- coding:utf-8 -*-
'''
将mysql中的中医结构化数据存入neo4j数据库
'''
from src.datastore.mysql_opt import TcmMysql
from src.datastore.graph.neo_datagraph_opt import NeoDataGraphOpt

class Mysql2Neo4j(object):

    MEDICINETAG = 'medicine'
    SYMPTOMTAG = "symptom"
    DISEASETAG = "disease"
    DISEASE2SYMPTOMTAG = "disease2symptom"
    DISEASE2MEDICINETAG = "disease2medicine"
    SYMPTOM2MEDICINETAG = 'symptom2medicine'
    RELATIONWEIGHT = 'weight'

    def store_medicine(self):
        mysqlDb = TcmMysql()
        neoDb = NeoDataGraphOpt()
        # 先删除数据库中存在的medicine节点以及和其有关联的关系
        medicineNodes = neoDb.selectNodeElementsFromDB(self.MEDICINETAG)
        neoDb.deleteNodesFromDB(medicineNodes)
        medicineMysql = mysqlDb.select('medicine')
        for medicineUnit in medicineMysql:
            print("输出id为%d的药物:%s"%(medicineUnit[0],medicineUnit[1].encode('utf8')))
            node = neoDb.createNode([self.MEDICINETAG], {'mid':medicineUnit[0],'name':medicineUnit[1]})
            neoDb.constructSubGraphInDB(node)
        mysqlDb.close()

    def store_symptom(self):
        mysqlDb = TcmMysql()
        neoDb = NeoDataGraphOpt()
        # 先删除数据库中存在的symptom节点以及和其有关联的关系
        symptomNodes = neoDb.selectNodeElementsFromDB(self.SYMPTOMTAG)
        neoDb.deleteNodesFromDB(symptomNodes)
        symptomMysql = mysqlDb.select('symptom')
        for symptomUnit in symptomMysql:
            print("输出id为%d的症状:%s"%(symptomUnit[0],symptomUnit[1].encode('utf8')))
            node = neoDb.createNode([self.SYMPTOMTAG], {'sid':symptomUnit[0],'name':symptomUnit[1]})
            neoDb.constructSubGraphInDB(node)
        mysqlDb.close()

    def store_desease(self):
        mysqlDb = TcmMysql()
        neoDb = NeoDataGraphOpt()
        # 先删除数据库中存在的desease节点以及和其有关联的关系
        deseaseNodes = neoDb.selectNodeElementsFromDB(self.DISEASETAG)
        neoDb.deleteNodesFromDB(deseaseNodes)
        diseaseMysql = mysqlDb.select('treatment','distinct disease')
        for diseaseUnit in diseaseMysql:
            print("输出病症:%s"%(diseaseUnit[0].encode('utf8')))
            node = neoDb.createNode([self.DISEASETAG], {'name':diseaseUnit[0]})
            neoDb.constructSubGraphInDB(node)
        print ("总共%d个病症"%mysqlDb.affected_num())
        mysqlDb.close()

    def store_desease2symptom(self):
        TID = 'tid'
        mysqlDb = TcmMysql()
        neoDb = NeoDataGraphOpt()
        treamentMysql = mysqlDb.select('treatment','id,symptomIds,disease')
        for treamentUnit in treamentMysql:
            syptomeIds = treamentUnit[1].split(",")
            disease = treamentUnit[2]
            diseaseNodes = neoDb.selectNodeElementsFromDB(self.DISEASETAG,condition=[],properties={'name':disease})
            tid = int(treamentUnit[0])
            for syptomeid in syptomeIds:
                syptomeNodes = neoDb.selectNodeElementsFromDB(self.SYMPTOMTAG,condition=[],properties={'sid':int(syptomeid)})
                if(len(diseaseNodes)>0 and len(syptomeNodes)>0):
                    diseaseNode = diseaseNodes[0]
                    syptomeNode = syptomeNodes[0]
                    print(diseaseNode['name'])
                    print(syptomeNode['name'])
                    print("输出病症%s和症状%s的关系:%d"%(disease.encode('utf8'),syptomeNode['name'].encode('utf8'),tid))
                    # 如果节点之间已经存在关系了,则权重加1,否则创建关系
                    relations = neoDb.selectRelationshipsFromDB(diseaseNode,self.DISEASE2SYMPTOMTAG,syptomeNode)
                    if(len(relations)>0):
                        print("更新病症%s和症状%s的关系" % (disease.encode('utf8'), syptomeNode['name'].encode('utf8')))
                        relation = relations[0]
                        tids = relation[TID]
                        weight = relation[self.RELATIONWEIGHT]
                        tids.append(tid)
                        weight+=1
                        neoDb.updateKeyInRelationship(relation,updateProperty=True,relationshipName=None,properties = {TID:tids,self.RELATIONWEIGHT:weight})
                    else:
                        tids = [tid]
                        weight = 1
                        rel = neoDb.createRelationship(self.DISEASE2SYMPTOMTAG, diseaseNode, syptomeNode, propertyDic={TID:tids,self.RELATIONWEIGHT:weight})
                        neoDb.constructSubGraphInDB(rel)
        mysqlDb.close()

    def store_desease2medicine(self):
        TID = 'tid'
        mysqlDb = TcmMysql()
        neoDb = NeoDataGraphOpt()
        treamentMysql = mysqlDb.select('treatment','id,prescriptionIds,disease')
        for treamentUnit in treamentMysql:
            priscriptionMysql = mysqlDb.select('prescription','medicineIds','id = '+treamentUnit[1])
            if(len(priscriptionMysql)>0):
                medicineIds = priscriptionMysql[0][0].split(",")
            else:
                continue
            disease = treamentUnit[2]
            diseaseNodes = neoDb.selectNodeElementsFromDB(self.DISEASETAG,condition=[],properties={'name':disease})
            tid = int(treamentUnit[0])
            for medicineid in medicineIds:
                medicineNodes = neoDb.selectNodeElementsFromDB(self.MEDICINETAG,condition=[],properties={'mid':int(medicineid)})
                if(len(diseaseNodes)>0 and len(medicineNodes)>0):
                    diseaseNode = diseaseNodes[0]
                    medicineNode = medicineNodes[0]
                    print(diseaseNode['name'])
                    print(medicineNode['name'])
                    print("输出病症%s和药物%s的关系:%d"%(disease.encode('utf8'),medicineNode['name'].encode('utf8'),tid))
                    # 如果节点之间已经存在关系了,则权重加1,否则创建关系
                    relations = neoDb.selectRelationshipsFromDB(diseaseNode,self.DISEASE2MEDICINETAG,medicineNode)
                    if(len(relations)>0):
                        print("更新病症%s和药物%s的关系" % (disease.encode('utf8'), medicineNode['name'].encode('utf8')))
                        relation = relations[0]
                        tids = relation[TID]
                        weight = relation[self.RELATIONWEIGHT]
                        tids.append(tid)
                        weight+=1
                        neoDb.updateKeyInRelationship(relation,updateProperty=True,relationshipName=None,properties = {TID:tids,self.RELATIONWEIGHT:weight})
                    else:
                        tids = [tid]
                        weight = 1
                        rel = neoDb.createRelationship(self.DISEASE2MEDICINETAG, diseaseNode, medicineNode, propertyDic={TID:tids,self.RELATIONWEIGHT:weight})
                        neoDb.constructSubGraphInDB(rel)
        mysqlDb.close()

    def store_symptom2medicine(self):
        TID = 'tid'
        mysqlDb = TcmMysql()
        neoDb = NeoDataGraphOpt()
        treamentMysql = mysqlDb.select('treatment','id,symptomIds,prescriptionIds')
        for treamentUnit in treamentMysql:
            priscriptionMysql = mysqlDb.select('prescription', 'medicineIds', 'id = ' + treamentUnit[2])
            if (len(priscriptionMysql) > 0):
                medicineIds = priscriptionMysql[0][0].split(",")
            else:
                continue
            syptomeIds = treamentUnit[1].split(",")
            tid = int(treamentUnit[0])
            for medicineId in medicineIds:
                for syptomeId in syptomeIds:
                    medicineNodes = neoDb.selectNodeElementsFromDB(self.MEDICINETAG, condition=[],properties={'mid': int(medicineId)})
                    syptomeNodes = neoDb.selectNodeElementsFromDB(self.SYMPTOMTAG, condition=[],properties={'sid': int(syptomeId)})
                    if(len(medicineNodes)>0 and len(syptomeNodes)>0):
                        medicineNode = medicineNodes[0]
                        syptomeNode = syptomeNodes[0]
                        print(medicineNode['name'])
                        print(syptomeNode['name'])
                        print("输出症状%s和药物%s的关系:%d"%(syptomeNode['name'].encode('utf8'),medicineNode['name'].encode('utf8'),tid))
                        # 如果节点之间已经存在关系了,则权重加1,否则创建关系
                        relations = neoDb.selectRelationshipsFromDB(syptomeNode, self.SYMPTOM2MEDICINETAG, medicineNode)
                        if (len(relations) > 0):
                            print("更新症状%s和药物%s的关系" % (syptomeNode['name'].encode('utf8'), medicineNode['name'].encode('utf8')))
                            relation = relations[0]
                            tids = relation[TID]
                            weight = relation[self.RELATIONWEIGHT]
                            tids.append(tid)
                            weight += 1
                            neoDb.updateKeyInRelationship(relation, updateProperty=True, relationshipName=None,properties={TID: tids, self.RELATIONWEIGHT: weight})
                        else:
                            tids = [tid]
                            weight = 1
                            rel = neoDb.createRelationship(self.SYMPTOM2MEDICINETAG, syptomeNode, medicineNode, propertyDic={TID: tids, self.RELATIONWEIGHT: weight})
                            neoDb.constructSubGraphInDB(rel)
        mysqlDb.close()

    def mysql2neo4j(self):
        print ("开始创建图谱...")
        # self.store_medicine()
        # self.store_symptom()
        # self.store_desease()
        # self.store_desease2symptom()
        # self.store_desease2medicine()
        # self.store_symptom2medicine()

if __name__ == '__main__':
    print("directly excute mysql2neo4j...")
    mysql2Neo = Mysql2Neo4j()
    mysql2Neo.mysql2neo4j()

















