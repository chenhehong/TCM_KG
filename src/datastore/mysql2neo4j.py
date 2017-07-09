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
    PULSETAG = "pulse"
    TONGUETAITAG = "tongueTai"
    TONGUEZHITAG = "tongueZhi"
    DISEASE2MEDICINETAG = "disease2medicine"
    SYMPTOM2MEDICINETAG = "symptom2medicine"
    PULSE2MEDICINETAG = "pulse2medicine"
    TONGUETAI2MEDICINETAG = "tongueTai2medicine"
    TONGUEZHI2MEDICINETAG = "tongueZhi2medicine"
    RELATIONWEIGHT = 'weight'
    RELATIONTID = 'tid'

    def store_medicine(self):
        mysqlDb = TcmMysql()
        neoDb = NeoDataGraphOpt()
        # 先删除数据库中存在的medicine节点以及和其有关联的关系
        medicineNodes = neoDb.selectNodeElementsFromDB(self.MEDICINETAG)
        neoDb.deleteNodesFromDB(medicineNodes)
        medicineMysql = mysqlDb.select('medicine')
        for medicineUnit in medicineMysql:
            print("输出id为%d的药物:%s"%(medicineUnit[0],medicineUnit[1].encode('utf8')))
            node = neoDb.createNode([self.MEDICINETAG], {'id':medicineUnit[0],'name':medicineUnit[1]})
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
            node = neoDb.createNode([self.SYMPTOMTAG], {'id':symptomUnit[0],'name':symptomUnit[1]})
            neoDb.constructSubGraphInDB(node)
        mysqlDb.close()

    def store_desease(self):
        mysqlDb = TcmMysql()
        neoDb = NeoDataGraphOpt()
        # 先删除数据库中存在的disease节点以及和其有关联的关系
        diseaseNodes = neoDb.selectNodeElementsFromDB(self.DISEASETAG)
        neoDb.deleteNodesFromDB(diseaseNodes)
        diseaseMysql = mysqlDb.select('disease')
        for diseaseUnit in diseaseMysql:
            print("输出id为%d的疾病:%s"%(diseaseUnit[0],diseaseUnit[1].encode('utf8')))
            node = neoDb.createNode([self.DISEASETAG], {'id':diseaseUnit[0],'name':diseaseUnit[1]})
            neoDb.constructSubGraphInDB(node)
        mysqlDb.close()

    def store_pulse(self):
        mysqlDb = TcmMysql()
        neoDb = NeoDataGraphOpt()
        # 先删除数据库中存在的pulse节点以及和其有关联的关系
        pulseNodes = neoDb.selectNodeElementsFromDB(self.PULSETAG)
        neoDb.deleteNodesFromDB(pulseNodes)
        pulseMysql = mysqlDb.select('pulse')
        for pulseUnit in pulseMysql:
            print("输出id为%d的脉搏:%s"%(pulseUnit[0],pulseUnit[1].encode('utf8')))
            node = neoDb.createNode([self.PULSETAG], {'id':pulseUnit[0],'name':pulseUnit[1]})
            neoDb.constructSubGraphInDB(node)
        mysqlDb.close()

    def store_tongue_tai(self):
        mysqlDb = TcmMysql()
        neoDb = NeoDataGraphOpt()
        # 先删除数据库中存在的tongueTai节点以及和其有关联的关系
        taongueTaiNodes = neoDb.selectNodeElementsFromDB(self.TONGUETAITAG)
        neoDb.deleteNodesFromDB(taongueTaiNodes)
        tongueTaiMysql = mysqlDb.select('tongue_tai')
        for tongueTaiUnit in tongueTaiMysql:
            print("输出id为%d的舌苔:%s"%(tongueTaiUnit[0],tongueTaiUnit[1].encode('utf8')))
            node = neoDb.createNode([self.TONGUETAITAG], {'id':tongueTaiUnit[0],'name':tongueTaiUnit[1]})
            neoDb.constructSubGraphInDB(node)
        mysqlDb.close()

    def store_tongue_zhi(self):
        mysqlDb = TcmMysql()
        neoDb = NeoDataGraphOpt()
        # 先删除数据库中存在的tongueTai节点以及和其有关联的关系
        taongueZhiNodes = neoDb.selectNodeElementsFromDB(self.TONGUEZHITAG)
        neoDb.deleteNodesFromDB(taongueZhiNodes)
        tongueZhiMysql = mysqlDb.select('tongue_zhi')
        for tongueZhiUnit in tongueZhiMysql:
            print("输出id为%d的舌质:%s"%(tongueZhiUnit[0],tongueZhiUnit[1].encode('utf8')))
            node = neoDb.createNode([self.TONGUEZHITAG], {'id':tongueZhiUnit[0],'name':tongueZhiUnit[1]})
            neoDb.constructSubGraphInDB(node)
        mysqlDb.close()

    def store_desease2medicine(self):
        mysqlDb = TcmMysql()
        neoDb = NeoDataGraphOpt()
        treamentMysql = mysqlDb.select('treatment','id,diseaseId,prescriptionId')
        for treamentUnit in treamentMysql:
            priscriptionMysql = mysqlDb.select('prescription', 'medicineIds', 'id = ' + str(treamentUnit[2]))
            if (len(priscriptionMysql) > 0):
                medicineIds = priscriptionMysql[0][0].split(",")
            else:
                continue
            diseaseId = treamentUnit[1]
            tid = int(treamentUnit[0])
            for medicineId in medicineIds:
                medicineNodes = neoDb.selectNodeElementsFromDB(self.MEDICINETAG, condition=[],properties={'id': int(medicineId)})
                diseaseNodes = neoDb.selectNodeElementsFromDB(self.DISEASETAG, condition=[],properties={'id': int(diseaseId)})
                if(len(medicineNodes)>0 and len(diseaseNodes)>0):
                    medicineNode = medicineNodes[0]
                    diseaseNode = diseaseNodes[0]
                    print(medicineNode['name'])
                    print(diseaseNode['name'])
                    print("输出疾病%s和药物%s的关系:%d"%(diseaseNode['name'].encode('utf8'),medicineNode['name'].encode('utf8'),tid))
                    # 如果节点之间已经存在关系了,则权重加1,否则创建关系
                    relations = neoDb.selectRelationshipsFromDB(diseaseNode, self.DISEASE2MEDICINETAG, medicineNode)
                    if (len(relations) > 0):
                        print("更新疾病%s和药物%s的关系" % (diseaseNode['name'].encode('utf8'), medicineNode['name'].encode('utf8')))
                        relation = relations[0]
                        tids = relation[self.RELATIONTID]
                        weight = relation[self.RELATIONWEIGHT]
                        tids.append(tid)
                        weight += 1
                        neoDb.updateKeyInRelationship(relation, updateProperty=True, relationshipName=None,properties={self.RELATIONTID: tids, self.RELATIONWEIGHT: weight})
                    else:
                        tids = [tid]
                        weight = 1
                        rel = neoDb.createRelationship(self.DISEASE2MEDICINETAG, diseaseNode, medicineNode, propertyDic={self.RELATIONTID: tids, self.RELATIONWEIGHT: weight})
                        neoDb.constructSubGraphInDB(rel)
        mysqlDb.close()

    def store_symptom2medicine(self):
        mysqlDb = TcmMysql()
        neoDb = NeoDataGraphOpt()
        treamentMysql = mysqlDb.select('treatment','id,symptomIds,prescriptionId')
        for treamentUnit in treamentMysql:
            priscriptionMysql = mysqlDb.select('prescription', 'medicineIds', 'id = ' + str(treamentUnit[2]))
            if (len(priscriptionMysql) > 0):
                medicineIds = priscriptionMysql[0][0].split(",")
            else:
                continue
            syptomeIds = treamentUnit[1].split(",")
            tid = int(treamentUnit[0])
            for medicineId in medicineIds:
                for syptomeId in syptomeIds:
                    medicineNodes = neoDb.selectNodeElementsFromDB(self.MEDICINETAG, condition=[],properties={'id': int(medicineId)})
                    syptomeNodes = neoDb.selectNodeElementsFromDB(self.SYMPTOMTAG, condition=[],properties={'id': int(syptomeId)})
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
                            tids = relation[self.RELATIONTID]
                            weight = relation[self.RELATIONWEIGHT]
                            tids.append(tid)
                            weight += 1
                            neoDb.updateKeyInRelationship(relation, updateProperty=True, relationshipName=None,properties={self.RELATIONTID: tids, self.RELATIONWEIGHT: weight})
                        else:
                            tids = [tid]
                            weight = 1
                            rel = neoDb.createRelationship(self.SYMPTOM2MEDICINETAG, syptomeNode, medicineNode, propertyDic={self.RELATIONTID: tids, self.RELATIONWEIGHT: weight})
                            neoDb.constructSubGraphInDB(rel)
        mysqlDb.close()

    def store_pulse2medicine(self):
        mysqlDb = TcmMysql()
        neoDb = NeoDataGraphOpt()
        treamentMysql = mysqlDb.select('treatment','id,pulseId,prescriptionId')
        for treamentUnit in treamentMysql:
            priscriptionMysql = mysqlDb.select('prescription', 'medicineIds', 'id = ' + str(treamentUnit[2]))
            if (len(priscriptionMysql) > 0):
                medicineIds = priscriptionMysql[0][0].split(",")
            else:
                continue
            pulseId = treamentUnit[1]
            tid = int(treamentUnit[0])
            for medicineId in medicineIds:
                medicineNodes = neoDb.selectNodeElementsFromDB(self.MEDICINETAG, condition=[],properties={'id': int(medicineId)})
                pulseNodes = neoDb.selectNodeElementsFromDB(self.PULSETAG, condition=[],properties={'id': int(pulseId)})
                if(len(medicineNodes)>0 and len(pulseNodes)>0):
                    medicineNode = medicineNodes[0]
                    pulseNode = pulseNodes[0]
                    print(medicineNode['name'])
                    print(pulseNode['name'])
                    print("输出脉搏%s和药物%s的关系:%d"%(pulseNode['name'].encode('utf8'),medicineNode['name'].encode('utf8'),tid))
                    # 如果节点之间已经存在关系了,则权重加1,否则创建关系
                    relations = neoDb.selectRelationshipsFromDB(pulseNode, self.PULSE2MEDICINETAG, medicineNode)
                    if (len(relations) > 0):
                        print("更新脉搏%s和药物%s的关系" % (pulseNode['name'].encode('utf8'), medicineNode['name'].encode('utf8')))
                        relation = relations[0]
                        tids = relation[self.RELATIONTID]
                        weight = relation[self.RELATIONWEIGHT]
                        tids.append(tid)
                        weight += 1
                        neoDb.updateKeyInRelationship(relation, updateProperty=True, relationshipName=None,properties={self.RELATIONTID: tids, self.RELATIONWEIGHT: weight})
                    else:
                        tids = [tid]
                        weight = 1
                        rel = neoDb.createRelationship(self.PULSE2MEDICINETAG, pulseNode, medicineNode, propertyDic={self.RELATIONTID: tids, self.RELATIONWEIGHT: weight})
                        neoDb.constructSubGraphInDB(rel)
        mysqlDb.close()

    def store_tonguetai2medicine(self):
        mysqlDb = TcmMysql()
        neoDb = NeoDataGraphOpt()
        treamentMysql = mysqlDb.select('treatment','id,tongueTaiId,prescriptionId')
        for treamentUnit in treamentMysql:
            priscriptionMysql = mysqlDb.select('prescription', 'medicineIds', 'id = ' + str(treamentUnit[2]))
            if (len(priscriptionMysql) > 0):
                medicineIds = priscriptionMysql[0][0].split(",")
            else:
                continue
            tongueTaiId = treamentUnit[1]
            tid = int(treamentUnit[0])
            for medicineId in medicineIds:
                medicineNodes = neoDb.selectNodeElementsFromDB(self.MEDICINETAG, condition=[],properties={'id': int(medicineId)})
                tongueTaiNodes = neoDb.selectNodeElementsFromDB(self.TONGUETAITAG, condition=[],properties={'id': int(tongueTaiId)})
                if(len(medicineNodes)>0 and len(tongueTaiNodes)>0):
                    medicineNode = medicineNodes[0]
                    tongueTaiNode = tongueTaiNodes[0]
                    print(medicineNode['name'])
                    print(tongueTaiNode['name'])
                    print("输出舌苔%s和药物%s的关系:%d"%(tongueTaiNode['name'].encode('utf8'),medicineNode['name'].encode('utf8'),tid))
                    # 如果节点之间已经存在关系了,则权重加1,否则创建关系
                    relations = neoDb.selectRelationshipsFromDB(tongueTaiNode, self.TONGUETAI2MEDICINETAG, medicineNode)
                    if (len(relations) > 0):
                        print("更新舌苔%s和药物%s的关系" % (tongueTaiNode['name'].encode('utf8'), medicineNode['name'].encode('utf8')))
                        relation = relations[0]
                        tids = relation[self.RELATIONTID]
                        weight = relation[self.RELATIONWEIGHT]
                        tids.append(tid)
                        weight += 1
                        neoDb.updateKeyInRelationship(relation, updateProperty=True, relationshipName=None,properties={self.RELATIONTID: tids, self.RELATIONWEIGHT: weight})
                    else:
                        tids = [tid]
                        weight = 1
                        rel = neoDb.createRelationship(self.TONGUETAI2MEDICINETAG, tongueTaiNode, medicineNode, propertyDic={self.RELATIONTID: tids, self.RELATIONWEIGHT: weight})
                        neoDb.constructSubGraphInDB(rel)
        mysqlDb.close()

    def store_tonguezhi2medicine(self):
        mysqlDb = TcmMysql()
        neoDb = NeoDataGraphOpt()
        treamentMysql = mysqlDb.select('treatment','id,tongueZhiId,prescriptionId')
        for treamentUnit in treamentMysql:
            priscriptionMysql = mysqlDb.select('prescription', 'medicineIds', 'id = ' + str(treamentUnit[2]))
            if (len(priscriptionMysql) > 0):
                medicineIds = priscriptionMysql[0][0].split(",")
            else:
                continue
            tongueZhiId = treamentUnit[1]
            tid = int(treamentUnit[0])
            for medicineId in medicineIds:
                medicineNodes = neoDb.selectNodeElementsFromDB(self.MEDICINETAG, condition=[],properties={'id': int(medicineId)})
                tongueZhiNodes = neoDb.selectNodeElementsFromDB(self.TONGUEZHITAG, condition=[],properties={'id': int(tongueZhiId)})
                if(len(medicineNodes)>0 and len(tongueZhiNodes)>0):
                    medicineNode = medicineNodes[0]
                    tongueZhiNode = tongueZhiNodes[0]
                    print(medicineNode['name'])
                    print(tongueZhiNode['name'])
                    print("输出舌质%s和药物%s的关系:%d"%(tongueZhiNode['name'].encode('utf8'),medicineNode['name'].encode('utf8'),tid))
                    # 如果节点之间已经存在关系了,则权重加1,否则创建关系
                    relations = neoDb.selectRelationshipsFromDB(tongueZhiNode, self.TONGUEZHI2MEDICINETAG, medicineNode)
                    if (len(relations) > 0):
                        print("更新舌质%s和药物%s的关系" % (tongueZhiNode['name'].encode('utf8'), medicineNode['name'].encode('utf8')))
                        relation = relations[0]
                        tids = relation[self.RELATIONTID]
                        weight = relation[self.RELATIONWEIGHT]
                        tids.append(tid)
                        weight += 1
                        neoDb.updateKeyInRelationship(relation, updateProperty=True, relationshipName=None,properties={self.RELATIONTID: tids, self.RELATIONWEIGHT: weight})
                    else:
                        tids = [tid]
                        weight = 1
                        rel = neoDb.createRelationship(self.TONGUEZHI2MEDICINETAG, tongueZhiNode, medicineNode, propertyDic={self.RELATIONTID: tids, self.RELATIONWEIGHT: weight})
                        neoDb.constructSubGraphInDB(rel)
        mysqlDb.close()

    def mysql2neo4j(self):
        print ("开始创建图谱...")
        # self.store_medicine()
        # self.store_symptom()
        # self.store_desease()
        # self.store_pulse()
        # self.store_tongue_tai()
        # self.store_tongue_zhi()
        # self.store_symptom2medicine()
        # self.store_desease2medicine()
        # self.store_pulse2medicine()
        # self.store_tonguetai2medicine()
        # self.store_tonguezhi2medicine()

if __name__ == '__main__':
    print("directly excute mysql2neo4j...")
    mysql2Neo = Mysql2Neo4j()
    mysql2Neo.mysql2neo4j()

















