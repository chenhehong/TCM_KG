# -*- coding:utf-8 -*-
'''
将mysql中的中医结构化数据存入neo4j数据库
'''
from src.datastore.mysql_opt import MysqlOpt
from src.datastore.neo4j_opt import Neo4jOpt
from src.util.file_util import FileUtil


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
    SYMPTOM2TONGUEZHITAG = "symptom2tongueZhi"
    SYMPTOM2TONGUETAITAG = "symptom2tongueTai"
    SYMPTOM2PULSETAG = "symptom2pulse"
    NODEID = "id"
    NODENAME="name"
    NODEWEIGHT = "weight"
    NODETID='tid'
    RELATIONWEIGHT = 'weight'
    RELATIONTID = 'tid'

    def store_medicine(self,filter = False,filterSet = set([])):
        mysqlDb = MysqlOpt()
        neoDb = Neo4jOpt()
        # 先删除数据库中存在的medicine节点以及和其有关联的关系
        cql = "MATCH (n:"+self.MEDICINETAG+") DETACH DELETE n"
        neoDb.graph.data(cql)
        FileUtil.print_string(u"开始存储中药节点...",True)
        mysqlDatas = mysqlDb.select('medicine','id,name')
        for unit in mysqlDatas:
            id = unit[0]
            name = unit[1]
            if(filter==True and (id not in filterSet)):
                continue
            # print("输出id为%d的药物:%s"%(id,name.encode('utf8')))
            defaultTid = []
            defaultWeight = 0
            neoDb.createNode([self.MEDICINETAG], {self.NODEID:id,self.NODENAME:name,self.NODEWEIGHT:defaultWeight,self.NODETID:defaultTid})
        mysqlDb.close()

    def store_symptom(self,filter = False,filterSet = set([])):
        mysqlDb = MysqlOpt()
        neoDb = Neo4jOpt()
        cql = "MATCH (n:"+self.SYMPTOMTAG+") DETACH DELETE n"
        neoDb.graph.data(cql)
        FileUtil.print_string(u"开始存储症状节点...",True)
        mysqlDatas = mysqlDb.select('symptom','id,name')
        for unit in mysqlDatas:
            id = unit[0]
            name = unit[1]
            if(filter==True and id not in filterSet):
                continue
            neoDb.createNode([self.SYMPTOMTAG], {self.NODEID:id,self.NODENAME:name})
        mysqlDb.close()

    def store_desease(self,filter = False,filterSet = set([])):
        mysqlDb = MysqlOpt()
        neoDb = Neo4jOpt()
        cql = "MATCH (n:"+self.DISEASETAG+") DETACH DELETE n"
        neoDb.graph.data(cql)
        FileUtil.print_string(u"开始存储疾病节点...",True)
        mysqlDatas = mysqlDb.select('disease','id,name')
        for unit in mysqlDatas:
            id = unit[0]
            name = unit[1]
            if(filter==True and id not in filterSet):
                continue
            neoDb.createNode([self.DISEASETAG], {self.NODEID:id,self.NODENAME:name})
        mysqlDb.close()

    def store_pulse(self,filter = False,filterSet = set([])):
        mysqlDb = MysqlOpt()
        neoDb = Neo4jOpt()
        cql = "MATCH (n:"+self.PULSETAG+") DETACH DELETE n"
        neoDb.graph.data(cql)
        FileUtil.print_string(u"开始存储脉搏节点...",True)
        mysqlDatas = mysqlDb.select('pulse','id,name')
        for unit in mysqlDatas:
            id = unit[0]
            name = unit[1]
            if(filter==True and id not in filterSet):
                continue
            neoDb.createNode([self.PULSETAG], {self.NODEID:id,self.NODENAME:name})
        mysqlDb.close()

    def store_tongue_tai(self,filter = False,filterSet = set([])):
        mysqlDb = MysqlOpt()
        neoDb = Neo4jOpt()
        cql = "MATCH (n:"+self.TONGUETAITAG+") DETACH DELETE n"
        neoDb.graph.data(cql)
        FileUtil.print_string(u"开始存储舌苔节点...",True)
        mysqlDatas = mysqlDb.select('tongueTai','id,name')
        for unit in mysqlDatas:
            id = unit[0]
            name = unit[1]
            if(filter==True and id not in filterSet):
                continue
            neoDb.createNode([self.TONGUETAITAG], {self.NODEID:id,self.NODENAME:name})
        mysqlDb.close()

    def store_tongue_zhi(self,filter = False,filterSet = set([])):
        mysqlDb = MysqlOpt()
        neoDb = Neo4jOpt()
        cql = "MATCH (n:"+self.TONGUEZHITAG+") DETACH DELETE n"
        neoDb.graph.data(cql)
        FileUtil.print_string(u"开始存储舌质节点...",True)
        mysqlDatas = mysqlDb.select('tongueZhi','id,name')
        for unit in mysqlDatas:
            id = unit[0]
            name = unit[1]
            if(filter==True and id not in filterSet):
                continue
            neoDb.createNode([self.TONGUEZHITAG], {self.NODEID:id,self.NODENAME:name})
        mysqlDb.close()

    # 存储每个中药节点的出现的次数
    def store_medicine_weight(self,filter = False,filterSet = set([])):
        FileUtil.print_string(u"开始存储每个中药节点的出现的次数...",True)
        mysqlDb = MysqlOpt()
        neoDb = Neo4jOpt()
        treamentMysql = mysqlDb.select('treatment','id,prescriptionId')
        for treamentUnit in treamentMysql:
            tid = int(treamentUnit[0])
            # print tid
            pid = int(treamentUnit[1])
            if(filter==True and tid not in filterSet):
                continue
            priscriptionMysql = mysqlDb.select('prescription', 'name', 'id = ' + str(pid))
            if (len(priscriptionMysql) > 0):
                medicineIds = priscriptionMysql[0][0].split(",")
            else:
                continue
            # 针对name末尾有逗号的情况进行处理
            lenth = len(medicineIds)
            if(medicineIds[lenth-1]==''):
                medicineIds.pop()
            for medicineId in medicineIds:
                # print "mid"+str(medicineId)
                medicineNodes = neoDb.selectNodeElementsFromDB(self.MEDICINETAG, condition=[],properties={self.NODEID: int(medicineId)})
                if (len(medicineNodes)>0):
                    medicineNode = medicineNodes[0]
                    tids = []
                    for i in medicineNode[self.NODETID]:
                        tids.append(int(i))
                    tids.append(tid)
                    ntids = list(set(tids))
                    nweight = len(ntids)
                    neoDb.updateKeyInNode(medicineNode,properties={self.NODETID: ntids, self.NODEWEIGHT: nweight})
        mysqlDb.close()

    def store_desease2medicine(self,filter = False,filterSet = set([])):
        FileUtil.print_string (u"开始存储疾病和中药的关系...",True)
        mysqlDb = MysqlOpt()
        neoDb = Neo4jOpt()
        treamentMysql = mysqlDb.select('treatment','id,diseaseId,prescriptionId')
        for treamentUnit in treamentMysql:
            diseaseId = treamentUnit[1]
            tid = int(treamentUnit[0])
            if(filter==True and tid not in filterSet):
                continue
            priscriptionMysql = mysqlDb.select('prescription', 'name', 'id = ' + str(treamentUnit[2]))
            if (len(priscriptionMysql) > 0):
                medicineIds = priscriptionMysql[0][0].split(",")
            else:
                continue
            # 针对name末尾有逗号的情况进行处理
            lenth = len(medicineIds)
            if(medicineIds[lenth-1]==''):
                medicineIds.pop()
            for medicineId in medicineIds:
                medicineNodes = neoDb.selectNodeElementsFromDB(self.MEDICINETAG, condition=[],properties={self.NODEID: int(medicineId)})
                diseaseNodes = neoDb.selectNodeElementsFromDB(self.DISEASETAG, condition=[],properties={self.NODEID: int(diseaseId)})
                if(len(medicineNodes)>0 and len(diseaseNodes)>0):
                    medicineNode = medicineNodes[0]
                    diseaseNode = diseaseNodes[0]
                    # print(medicineNode['name'])
                    # print(diseaseNode['name'])
                    # print("输出疾病%s和药物%s的关系:%d"%(diseaseNode['name'].encode('utf8'),medicineNode['name'].encode('utf8'),tid))
                    # 如果节点之间已经存在关系了,则权重加1,否则创建关系
                    relations = neoDb.selectRelationshipsFromDB(diseaseNode, self.DISEASE2MEDICINETAG, medicineNode)
                    if (len(relations) > 0):
                        # print("更新疾病%s和药物%s的关系" % (diseaseNode['name'].encode('utf8'), medicineNode['name'].encode('utf8')))
                        relation = relations[0]
                        tids = relation[self.RELATIONTID]
                        tids.append(tid)
                        ntids = list(set(tids))
                        nweight = len(ntids)
                        neoDb.updateKeyInRelationship(relation,properties={self.RELATIONTID: ntids, self.RELATIONWEIGHT: nweight})
                    else:
                        tids = [tid]
                        weight = 1
                        neoDb.createRelationship(self.DISEASE2MEDICINETAG, diseaseNode, medicineNode, propertyDic={self.RELATIONTID: tids, self.RELATIONWEIGHT: weight})
        mysqlDb.close()

    def store_symptom2medicine(self,filter = False,filterSet = set([])):
        FileUtil.print_string(u"开始存储症状和中药的关系...",True)
        mysqlDb = MysqlOpt()
        neoDb = Neo4jOpt()
        treamentMysql = mysqlDb.select('treatment','id,symptomIds,prescriptionId')
        for treamentUnit in treamentMysql:
            syptomeIds = treamentUnit[1].split(",")
            tid = int(treamentUnit[0])
            if(filter==True and tid not in filterSet):
                continue
            priscriptionMysql = mysqlDb.select('prescription', 'name', 'id = ' + str(treamentUnit[2]))
            if (len(priscriptionMysql) > 0):
                medicineIds = priscriptionMysql[0][0].split(",")
            else:
                continue
            # 针对name末尾有逗号的情况进行处理
            lenth = len(medicineIds)
            if(medicineIds[lenth-1]==''):
                medicineIds.pop()
            for medicineId in medicineIds:
                for syptomeId in syptomeIds:
                    medicineNodes = neoDb.selectNodeElementsFromDB(self.MEDICINETAG, condition=[],properties={self.NODEID: int(medicineId)})
                    syptomeNodes = neoDb.selectNodeElementsFromDB(self.SYMPTOMTAG, condition=[],properties={self.NODEID: int(syptomeId)})
                    if(len(medicineNodes)>0 and len(syptomeNodes)>0):
                        medicineNode = medicineNodes[0]
                        syptomeNode = syptomeNodes[0]
                        # print(medicineNode['name'])
                        # print(syptomeNode['name'])
                        # print("输出症状%s和药物%s的关系:%d"%(syptomeNode['name'].encode('utf8'),medicineNode['name'].encode('utf8'),tid))
                        # 如果节点之间已经存在关系了,则权重加1,否则创建关系
                        relations = neoDb.selectRelationshipsFromDB(syptomeNode, self.SYMPTOM2MEDICINETAG, medicineNode)
                        if (len(relations) > 0):
                            # print("更新症状%s和药物%s的关系" % (syptomeNode['name'].encode('utf8'), medicineNode['name'].encode('utf8')))
                            relation = relations[0]
                            tids = relation[self.RELATIONTID]
                            tids.append(tid)
                            ntids = list(set(tids))
                            nweight = len(ntids)
                            neoDb.updateKeyInRelationship(relation,properties={self.RELATIONTID: ntids, self.RELATIONWEIGHT: nweight})
                        else:
                            tids = [tid]
                            weight = 1
                            neoDb.createRelationship(self.SYMPTOM2MEDICINETAG, syptomeNode, medicineNode, propertyDic={self.RELATIONTID: tids, self.RELATIONWEIGHT: weight})
        mysqlDb.close()

    def store_pulse2medicine(self,filter = False,filterSet = set([])):
        FileUtil.print_string (u"开始存储脉搏和中药的关系...",True)
        mysqlDb = MysqlOpt()
        neoDb = Neo4jOpt()
        treamentMysql = mysqlDb.select('treatment','id,pulseId,prescriptionId')
        for treamentUnit in treamentMysql:
            pulseId = treamentUnit[1]
            tid = int(treamentUnit[0])
            if(filter==True and tid not in filterSet):
                continue
            priscriptionMysql = mysqlDb.select('prescription', 'name', 'id = ' + str(treamentUnit[2]))
            if (len(priscriptionMysql) > 0):
                medicineIds = priscriptionMysql[0][0].split(",")
            else:
                continue
            # 针对name末尾有逗号的情况进行处理
            lenth = len(medicineIds)
            if(medicineIds[lenth-1]==''):
                medicineIds.pop()
            for medicineId in medicineIds:
                medicineNodes = neoDb.selectNodeElementsFromDB(self.MEDICINETAG, condition=[],properties={self.NODEID: int(medicineId)})
                pulseNodes = neoDb.selectNodeElementsFromDB(self.PULSETAG, condition=[],properties={self.NODEID: int(pulseId)})
                if(len(medicineNodes)>0 and len(pulseNodes)>0):
                    medicineNode = medicineNodes[0]
                    pulseNode = pulseNodes[0]
                    # print(medicineNode['name'])
                    # print(pulseNode['name'])
                    # print("输出脉搏%s和药物%s的关系:%d"%(pulseNode['name'].encode('utf8'),medicineNode['name'].encode('utf8'),tid))
                    # 如果节点之间已经存在关系了,则权重加1,否则创建关系
                    relations = neoDb.selectRelationshipsFromDB(pulseNode, self.PULSE2MEDICINETAG, medicineNode)
                    if (len(relations) > 0):
                        # print("更新脉搏%s和药物%s的关系" % (pulseNode['name'].encode('utf8'), medicineNode['name'].encode('utf8')))
                        relation = relations[0]
                        tids = relation[self.RELATIONTID]
                        tids.append(tid)
                        ntids = list(set(tids))
                        nweight = len(ntids)
                        neoDb.updateKeyInRelationship(relation, properties={self.RELATIONTID: ntids, self.RELATIONWEIGHT: nweight})
                    else:
                        tids = [tid]
                        weight = 1
                        neoDb.createRelationship(self.PULSE2MEDICINETAG, pulseNode, medicineNode, propertyDic={self.RELATIONTID: tids, self.RELATIONWEIGHT: weight})
        mysqlDb.close()

    def store_tonguetai2medicine(self,filter = False,filterSet = set([])):
        FileUtil.print_string(u"开始存储舌苔和中药的关系...",True)
        mysqlDb = MysqlOpt()
        neoDb = Neo4jOpt()
        treamentMysql = mysqlDb.select('treatment','id,tongueTaiId,prescriptionId')
        for treamentUnit in treamentMysql:
            tongueTaiId = treamentUnit[1]
            tid = int(treamentUnit[0])
            if(filter==True and tid not in filterSet):
                continue
            priscriptionMysql = mysqlDb.select('prescription', 'name', 'id = ' + str(treamentUnit[2]))
            if (len(priscriptionMysql) > 0):
                medicineIds = priscriptionMysql[0][0].split(",")
            else:
                continue
            # 针对name末尾有逗号的情况进行处理
            lenth = len(medicineIds)
            if(medicineIds[lenth-1]==''):
                medicineIds.pop()
            for medicineId in medicineIds:
                medicineNodes = neoDb.selectNodeElementsFromDB(self.MEDICINETAG, condition=[],properties={self.NODEID: int(medicineId)})
                tongueTaiNodes = neoDb.selectNodeElementsFromDB(self.TONGUETAITAG, condition=[],properties={self.NODEID: int(tongueTaiId)})
                if(len(medicineNodes)>0 and len(tongueTaiNodes)>0):
                    medicineNode = medicineNodes[0]
                    tongueTaiNode = tongueTaiNodes[0]
                    # print(medicineNode['name'])
                    # print(tongueTaiNode['name'])
                    # print("输出舌苔%s和药物%s的关系:%d"%(tongueTaiNode['name'].encode('utf8'),medicineNode['name'].encode('utf8'),tid))
                    # 如果节点之间已经存在关系了,则权重加1,否则创建关系
                    relations = neoDb.selectRelationshipsFromDB(tongueTaiNode, self.TONGUETAI2MEDICINETAG, medicineNode)
                    if (len(relations) > 0):
                        # print("更新舌苔%s和药物%s的关系" % (tongueTaiNode['name'].encode('utf8'), medicineNode['name'].encode('utf8')))
                        relation = relations[0]
                        tids = relation[self.RELATIONTID]
                        tids.append(tid)
                        ntids = list(set(tids))
                        nweight = len(ntids)
                        neoDb.updateKeyInRelationship(relation,properties={self.RELATIONTID: ntids, self.RELATIONWEIGHT: nweight})
                    else:
                        tids = [tid]
                        weight = 1
                        neoDb.createRelationship(self.TONGUETAI2MEDICINETAG, tongueTaiNode, medicineNode, propertyDic={self.RELATIONTID: tids, self.RELATIONWEIGHT: weight})
        mysqlDb.close()

    def store_tonguezhi2medicine(self,filter = False,filterSet = set([])):
        FileUtil.print_string(u"开始存储舌质和中药的关系...",True)
        mysqlDb = MysqlOpt()
        neoDb = Neo4jOpt()
        treamentMysql = mysqlDb.select('treatment','id,tongueZhiId,prescriptionId')
        for treamentUnit in treamentMysql:
            tongueZhiId = treamentUnit[1]
            tid = int(treamentUnit[0])
            if(filter==True and tid not in filterSet):
                continue
            priscriptionMysql = mysqlDb.select('prescription', 'name', 'id = ' + str(treamentUnit[2]))
            if (len(priscriptionMysql) > 0):
                medicineIds = priscriptionMysql[0][0].split(",")
            else:
                continue
            # 针对name末尾有逗号的情况进行处理
            lenth = len(medicineIds)
            if(medicineIds[lenth-1]==''):
                medicineIds.pop()
            for medicineId in medicineIds:
                medicineNodes = neoDb.selectNodeElementsFromDB(self.MEDICINETAG, condition=[],properties={self.NODEID: int(medicineId)})
                tongueZhiNodes = neoDb.selectNodeElementsFromDB(self.TONGUEZHITAG, condition=[],properties={self.NODEID: int(tongueZhiId)})
                if(len(medicineNodes)>0 and len(tongueZhiNodes)>0):
                    medicineNode = medicineNodes[0]
                    tongueZhiNode = tongueZhiNodes[0]
                    # print(medicineNode['name'])
                    # print(tongueZhiNode['name'])
                    # print("输出舌质%s和药物%s的关系:%d"%(tongueZhiNode['name'].encode('utf8'),medicineNode['name'].encode('utf8'),tid))
                    # 如果节点之间已经存在关系了,则权重加1,否则创建关系
                    relations = neoDb.selectRelationshipsFromDB(tongueZhiNode, self.TONGUEZHI2MEDICINETAG, medicineNode)
                    if (len(relations) > 0):
                        # print("更新舌质%s和药物%s的关系" % (tongueZhiNode['name'].encode('utf8'), medicineNode['name'].encode('utf8')))
                        relation = relations[0]
                        tids = relation[self.RELATIONTID]
                        tids.append(tid)
                        ntids = list(set(tids))
                        nweight = len(ntids)
                        neoDb.updateKeyInRelationship(relation,properties={self.RELATIONTID: ntids, self.RELATIONWEIGHT: nweight})
                    else:
                        tids = [tid]
                        weight = 1
                        neoDb.createRelationship(self.TONGUEZHI2MEDICINETAG, tongueZhiNode, medicineNode, propertyDic={self.RELATIONTID: tids, self.RELATIONWEIGHT: weight})
        mysqlDb.close()

    def store_symptom2tonguezhi(self,filter = False,filterSet = set([])):
        FileUtil.print_string(u"开始存储症状和舌质的关系...",True)
        mysqlDb = MysqlOpt()
        neoDb = Neo4jOpt()
        treamentMysql = mysqlDb.select('treatment', 'id,symptomIds,tongueZhiId')
        for treamentUnit in treamentMysql:
            syptomeIds = treamentUnit[1].split(",")
            tid = int(treamentUnit[0])
            tongueZhiId = treamentUnit[2]
            if(filter==True and tid not in filterSet):
                continue
            for syptomeId in syptomeIds:
                tongueZhiNodes = neoDb.selectNodeElementsFromDB(self.TONGUEZHITAG, condition=[],
                                                               properties={self.NODEID: int(tongueZhiId)})
                syptomeNodes = neoDb.selectNodeElementsFromDB(self.SYMPTOMTAG, condition=[],
                                                              properties={self.NODEID: int(syptomeId)})
                if (len(tongueZhiNodes) > 0 and len(syptomeNodes) > 0):
                    tongueZhiNode = tongueZhiNodes[0]
                    syptomeNode = syptomeNodes[0]
                    # print(tongueZhiNode['name'])
                    # print(syptomeNode['name'])
                    # print("输出症状%s和舌质%s的关系:%d" % (
                    # syptomeNode['name'].encode('utf8'), tongueZhiNode['name'].encode('utf8'), tid))
                    # 如果节点之间已经存在关系了,则权重加1,否则创建关系
                    relations = neoDb.selectRelationshipsFromDB(syptomeNode, self.SYMPTOM2TONGUEZHITAG,
                                                                tongueZhiNode)
                    if (len(relations) > 0):
                        # print("更新症状%s和舌质%s的关系" % (syptomeNode['name'].encode('utf8'), tongueZhiNode['name'].encode('utf8')))
                        relation = relations[0]
                        tids = relation[self.RELATIONTID]
                        tids.append(tid)
                        ntids = list(set(tids))
                        nweight = len(ntids)
                        neoDb.updateKeyInRelationship(relation,properties={self.RELATIONTID: ntids, self.RELATIONWEIGHT: nweight})
                    else:
                        tids = [tid]
                        weight = 1
                        neoDb.createRelationship(self.SYMPTOM2TONGUEZHITAG, syptomeNode, tongueZhiNode,
                                                       propertyDic={self.RELATIONTID: tids,
                                                                    self.RELATIONWEIGHT: weight})
        mysqlDb.close()

    def store_symptom2tonguetai(self,filter = False,filterSet = set([])):
        FileUtil.print_string(u"开始存储症状和舌苔的关系...",True)
        mysqlDb = MysqlOpt()
        neoDb = Neo4jOpt()
        treamentMysql = mysqlDb.select('treatment', 'id,symptomIds,tongueTaiId')
        for treamentUnit in treamentMysql:
            syptomeIds = treamentUnit[1].split(",")
            tid = int(treamentUnit[0])
            tongueTaiId = treamentUnit[2]
            if(filter==True and tid not in filterSet):
                continue
            for syptomeId in syptomeIds:
                tongueTaiNodes = neoDb.selectNodeElementsFromDB(self.TONGUETAITAG, condition=[],
                                                               properties={self.NODEID: int(tongueTaiId)})
                syptomeNodes = neoDb.selectNodeElementsFromDB(self.SYMPTOMTAG, condition=[],
                                                              properties={self.NODEID: int(syptomeId)})
                if (len(tongueTaiNodes) > 0 and len(syptomeNodes) > 0):
                    tongueTaiNode = tongueTaiNodes[0]
                    syptomeNode = syptomeNodes[0]
                    # print(tongueTaiNode['name'])
                    # print(syptomeNode['name'])
                    # print("输出症状%s和舌苔%s的关系:%d" % (syptomeNode['name'].encode('utf8'), tongueTaiNode['name'].encode('utf8'), tid))
                    # 如果节点之间已经存在关系了,则权重加1,否则创建关系
                    relations = neoDb.selectRelationshipsFromDB(syptomeNode, self.SYMPTOM2TONGUETAITAG,
                                                                tongueTaiNode)
                    if (len(relations) > 0):
                        # print("更新症状%s和舌苔%s的关系" % (syptomeNode['name'].encode('utf8'), tongueTaiNode['name'].encode('utf8')))
                        relation = relations[0]
                        tids = relation[self.RELATIONTID]
                        tids.append(tid)
                        ntids = list(set(tids))
                        nweight = len(ntids)
                        neoDb.updateKeyInRelationship(relation,properties={self.RELATIONTID: ntids, self.RELATIONWEIGHT: nweight})
                    else:
                        tids = [tid]
                        weight = 1
                        neoDb.createRelationship(self.SYMPTOM2TONGUETAITAG, syptomeNode, tongueTaiNode,
                                                       propertyDic={self.RELATIONTID: tids,
                                                                    self.RELATIONWEIGHT: weight})
        mysqlDb.close()


    def store_symptom2pulse(self,filter = False,filterSet = set([])):
        FileUtil.print_string(u"开始存储症状和脉搏的关系...",True)
        mysqlDb = MysqlOpt()
        neoDb = Neo4jOpt()
        treamentMysql = mysqlDb.select('treatment', 'id,symptomIds,pulseId')
        for treamentUnit in treamentMysql:
            syptomeIds = treamentUnit[1].split(",")
            tid = int(treamentUnit[0])
            pulseId = treamentUnit[2]
            if(filter==True and tid not in filterSet):
                continue
            for syptomeId in syptomeIds:
                pulseNodes = neoDb.selectNodeElementsFromDB(self.PULSETAG, condition=[],
                                                               properties={self.NODEID: int(pulseId)})
                syptomeNodes = neoDb.selectNodeElementsFromDB(self.SYMPTOMTAG, condition=[],
                                                              properties={self.NODEID: int(syptomeId)})
                if (len(pulseNodes) > 0 and len(syptomeNodes) > 0):
                    pulseNode = pulseNodes[0]
                    syptomeNode = syptomeNodes[0]
                    # print(pulseNode['name'])
                    # print(syptomeNode['name'])
                    # print("输出症状%s和脉搏%s的关系:%d" % (syptomeNode['name'].encode('utf8'), pulseNode['name'].encode('utf8'), tid))
                    # 如果节点之间已经存在关系了,则权重加1,否则创建关系
                    relations = neoDb.selectRelationshipsFromDB(syptomeNode, self.SYMPTOM2PULSETAG,
                                                                pulseNode)
                    if (len(relations) > 0):
                        # print("更新症状%s和脉搏%s的关系" % (syptomeNode['name'].encode('utf8'), pulseNode['name'].encode('utf8')))
                        relation = relations[0]
                        tids = relation[self.RELATIONTID]
                        tids.append(tid)
                        ntids = list(set(tids))
                        nweight = len(ntids)
                        neoDb.updateKeyInRelationship(relation, properties={self.RELATIONTID: ntids, self.RELATIONWEIGHT: nweight})
                    else:
                        tids = [tid]
                        weight = 1
                        neoDb.createRelationship(self.SYMPTOM2PULSETAG, syptomeNode, pulseNode,
                                                       propertyDic={self.RELATIONTID: tids,
                                                                    self.RELATIONWEIGHT: weight})
        mysqlDb.close()

    def get_medicine_class_num(self):
        neoDb = Neo4jOpt()
        nodes = neoDb.selectNodeElementsFromDB(self.MEDICINETAG)
        return len(nodes)

    def get_syptome_class_num(self):
        neoDb = Neo4jOpt()
        nodes = neoDb.selectNodeElementsFromDB(self.SYMPTOMTAG)
        return len(nodes)

    def get_tonguezhi_class_num(self):
        neoDb = Neo4jOpt()
        nodes = neoDb.selectNodeElementsFromDB(self.TONGUEZHITAG)
        return len(nodes)

    def get_tonguetai_class_num(self):
        neoDb = Neo4jOpt()
        nodes = neoDb.selectNodeElementsFromDB(self.TONGUETAITAG)
        return len(nodes)

    def get_pulse_class_num(self):
        neoDb = Neo4jOpt()
        nodes = neoDb.selectNodeElementsFromDB(self.PULSETAG)
        return len(nodes)

    def get_medicine_totle_weight(self):
        weight = 0
        neoDb = Neo4jOpt()
        nodes = neoDb.selectNodeElementsFromDB(self.MEDICINETAG)
        for i in range(len(nodes)):
            weight +=nodes[i][self.NODEWEIGHT]
        return weight

    def get_medicine_id_set(self, treatmentIdSet):
        mysqlDb = MysqlOpt()
        treamentMysql = mysqlDb.select('treatment','id,prescriptionId')
        idSet = set([])
        for treamentUnit in treamentMysql:
            tid = int(treamentUnit[0])
            pid = treamentUnit[1]
            if(tid not in treatmentIdSet):
                continue
            priscriptionMysql = mysqlDb.select('prescription', 'name', 'id = ' + str(pid))
            if (len(priscriptionMysql) == 0):
                continue
            medicineIds = priscriptionMysql[0][0].split(",")
            # 针对name末尾有逗号的情况进行处理
            lenth = len(medicineIds)
            if(medicineIds[lenth-1]==''):
                medicineIds.pop()
            medicineSet = set([int(x) for x in medicineIds])
            idSet = idSet|medicineSet
        return idSet

    def get_symptom_id_set(self, treatmentIdSet):
        mysqlDb = MysqlOpt()
        treamentMysql = mysqlDb.select('treatment','id,symptomIds')
        idSet = set([])
        for treamentUnit in treamentMysql:
            tid = int(treamentUnit[0])
            sids = treamentUnit[1]
            if(tid not in treatmentIdSet):
                continue
            symptomIds = sids.split(",")
            symptomSet = set([int(x) for x in symptomIds])
            idSet = idSet|symptomSet
        return idSet

    def get_tonguezhi_id_set(self, treatmentIdSet):
        mysqlDb = MysqlOpt()
        treamentMysql = mysqlDb.select('treatment','id,tongueZhiId')
        idSet = set([])
        for treamentUnit in treamentMysql:
            tid = int(treamentUnit[0])
            tzid = treamentUnit[1]
            if(tid not in treatmentIdSet):
                continue
            idSet.add(tzid)
        return idSet

    def get_tonguetai_id_set(self, treatmentIdSet):
        mysqlDb = MysqlOpt()
        treamentMysql = mysqlDb.select('treatment','id,tongueTaiId')
        idSet = set([])
        for treamentUnit in treamentMysql:
            tid = int(treamentUnit[0])
            ttid = treamentUnit[1]
            if(tid not in treatmentIdSet):
                continue
            idSet.add(ttid)
        return idSet

    def get_pulse_id_set(self, treatmentIdSet):
        mysqlDb = MysqlOpt()
        treamentMysql = mysqlDb.select('treatment','id,pulseId')
        idSet = set([])
        for treamentUnit in treamentMysql:
            tid = int(treamentUnit[0])
            pid = treamentUnit[1]
            if(tid not in treatmentIdSet):
                continue
            idSet.add(pid)
        return idSet

    # 把处方表那些没有药方的记录删除
    def cleanup_data(self):
        mysqlDb = MysqlOpt()
        treamentMysql = mysqlDb.select('treatment', 'id,prescriptionId')
        for treamentUnit in treamentMysql:
            tid = int(treamentUnit[0])
            pid = treamentUnit[1]
            priscriptionMysql = mysqlDb.select('prescription', 'name', 'id = ' + str(pid))
            if (len(priscriptionMysql) == 0):
                mysqlDb.delete('treatment','id='+str(tid))

    def mysql2neo4j(self):
        print ''
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
        # self.store_symptom2tonguezhi()
        # self.store_symptom2tonguetai()
        # self.store_symptom2pulse()

if __name__ == '__main__':
    print("directly excute mysql2neo4j...")
    mysql2Neo = Mysql2Neo4j()
    mysql2Neo.mysql2neo4j()

















