# -*- coding:utf-8 -*-
from sklearn.cross_validation import KFold

from src.datastore.mysql2neo4j import Mysql2Neo4j
from src.datastore.neo4j_opt import Neo4jOpt
from src.datastore.mysql_opt import MysqlOpt
import math
from sklearn import metrics
from list_util import ListUtil
from src.file_util import FileUtil


class NaiveBayesDM(object):

    TOPK = 300
    NAME = "name"
    PROBABILITY = "probability"
    LOGBASE = 10
    POINTACCURACY = 5
    CRVNUMFOLDER = 5

    def cross_validation(self):
        preFile = 'pre.txt'
        randomState = 51
        mysqlOpt = MysqlOpt()
        treamentIds = mysqlOpt.select('treatment','id')
        tidList = []
        for tid in treamentIds:
            tidList.append(tid[0])
        kf = KFold(len(tidList), n_folds=self.CRVNUMFOLDER, shuffle=True, random_state=randomState)
        numFold = 0
        for trainIndex,testIndex in kf:
            numFold+=1
            FileUtil.print_string(str(numFold)+'th validation:',True)
            trainList = []
            for i in trainIndex:
                trainList.append(tidList[i])
            self.createGraph(filter=True,treamentIdfilterSet=set(trainList))
            yTrue = []
            yPre = []
            FileUtil.add_string(preFile,str(numFold)+'th validation:')
            for i in testIndex:
                perTreament = mysqlOpt.select('treatment','prescriptionId,symptomIds,tongueZhiId,tongueTaiId,pulseId','id='+str(tidList[i]))
                pids = perTreament[0][0]
                symptomIds = perTreament[0][1]
                tZhiId = perTreament[0][2]
                tTaiId = perTreament[0][3]
                pulseId = perTreament[0][4]
                perPrecription = mysqlOpt.select('prescription','name','id = '+str(pids))
                mids = perPrecription[0][0]
                medicines = mysqlOpt.select('medicine','name','id IN ('+mids+')')
                trueMedicineList = []
                for m in medicines:
                    trueMedicineList.append(m[0])
                yTrue.append(trueMedicineList)
                symptoms = mysqlOpt.select('symptom','name','id IN ('+symptomIds+')')
                symptomList = []
                for s in symptoms:
                    symptomList.append(s[0])
                tongueZhiSql = mysqlOpt.select('tongueZhi','name','id = '+str(tZhiId))
                if(len(tongueZhiSql)>0):
                    tongueZhi = tongueZhiSql[0][0]
                else:
                    tongueZhi = ''
                tongueTaiSql = mysqlOpt.select('tongueTai','name','id = '+str(tTaiId))
                if(len(tongueTaiSql)>0):
                    tongueTai = tongueTaiSql[0][0]
                else:
                    tongueTai = ''
                pulseSql = mysqlOpt.select('pulse','name','id = '+str(pulseId))
                if(len(pulseSql)):
                    pulse = pulseSql[0][0]
                else:
                    pulse=''
                preMedicineList = self.input_multiple_syptomes(symptomList,tongueZhi,tongueTai,pulse)
                preString = str(tidList[i])+' '
                for m in preMedicineList:
                    preString=preString+m+','
                FileUtil.add_string(preFile,preString)
                yPre.append(preMedicineList)
            precision,recall = self.classification_evaluate(yTrue,yPre)
            FileUtil.print_string('precision:'+str(precision),True)
            FileUtil.print_string('recall:'+str(recall),True)
            F1 = 2*precision*recall/(precision+recall)
            FileUtil.print_string('F1:'+str(F1),True)

    def classification_evaluate(self,yTrue,yPre):
        testNum = len(yTrue)
        totalPrecision = 0
        totalRecall = 0
        for i in range(testNum):
            l = []
            l.append(yTrue[i])
            l.append(yPre[i])
            interList = ListUtil.list_intersect(l)
            perPrecision = round(len(interList)*1.0/len(yPre[i]),5)
            perRecall = round(len(interList)*1.0/len(yTrue[i]),5)
            FileUtil.print_string(str(i)+'th precision:'+str(perPrecision)+',recall:'+str(perRecall),True)
            totalPrecision+=perPrecision
            totalRecall+=perRecall
        precison = round(totalPrecision/testNum,5)
        recall = round(totalRecall/testNum,5)
        return precison,recall

    def createGraph(self, filter = False, treamentIdfilterSet = set([])):
        FileUtil.print_string(u"开始创建图谱...",True)
        mysql2Neo = Mysql2Neo4j()
        mysql2Neo.store_medicine(filter=filter, filterSet=mysql2Neo.get_medicine_id_set(treamentIdfilterSet))
        mysql2Neo.store_symptom(filter=filter, filterSet=mysql2Neo.get_symptom_id_set(treamentIdfilterSet))
        mysql2Neo.store_tongue_tai(filter=filter, filterSet=mysql2Neo.get_tonguetai_id_set(treamentIdfilterSet))
        mysql2Neo.store_tongue_zhi(filter=filter, filterSet=mysql2Neo.get_tonguezhi_id_set(treamentIdfilterSet))
        mysql2Neo.store_pulse(filter=filter, filterSet=mysql2Neo.get_pulse_id_set(treamentIdfilterSet))
        mysql2Neo.store_medicine_weight(filter=filter, filterSet=treamentIdfilterSet)
        mysql2Neo.store_symptom2medicine(filter=filter, filterSet=treamentIdfilterSet)
        mysql2Neo.store_tonguetai2medicine(filter=filter, filterSet=treamentIdfilterSet)
        mysql2Neo.store_tonguezhi2medicine(filter=filter, filterSet=treamentIdfilterSet)
        mysql2Neo.store_pulse2medicine(filter=filter, filterSet=treamentIdfilterSet)

    # 输入组合中医症状、舌脉，模型输出最相关的中药组合名称
    def input_multiple_syptomes(self, symptomList, tongueZhi, tongueTai, pulse):
        mysql2Neo = Mysql2Neo4j()
        medicineClassNum = mysql2Neo.get_medicine_class_num()
        symptomClassNum = mysql2Neo.get_syptome_class_num()
        tongueZhiClassNum = mysql2Neo.get_tonguezhi_class_num()
        tongueTaiClassNum = mysql2Neo.get_tonguetai_class_num()
        pulseClassNum = mysql2Neo.get_pulse_class_num()
        medicineTotalWeight = mysql2Neo.get_medicine_totle_weight()
        medicineDicList = []
        neoDb = Neo4jOpt()
        medicinNodes = neoDb.selectNodeElementsFromDB(Mysql2Neo4j.MEDICINETAG)
        for i in range(len(medicinNodes)):
            medicineNode = medicinNodes[i]
            medicineDic = {}
            medicineDic[self.NAME] = medicineNode[Mysql2Neo4j.NODENAME]
            medicineWeight = medicineNode[Mysql2Neo4j.NODEWEIGHT]
            medicnePro = self.myLog((medicineWeight+1.0)/(medicineTotalWeight+medicineClassNum))
            symptomOfMedicinePro = 0
            for symptom in symptomList:
                symtpomWeight = self.getSymptome2MedicineWeight(symptom,medicineNode)
                symptomOfMedicinePro = symptomOfMedicinePro+self.myLog((symtpomWeight+1.0)/ (medicineWeight + symptomClassNum))
            tongueZhiWeight = self.getTongueZhi2MedicineWeight(tongueZhi,medicineNode)
            tongueZhiOfMedicinePro = self.myLog((tongueZhiWeight+1.0)/(medicineWeight+tongueZhiClassNum))
            tongueTaiWeight = self.getTongueTai2MedicineWeight(tongueTai,medicineNode)
            tongueTaiOfMedicinePro = self.myLog((tongueTaiWeight+1.0)/(medicineWeight+tongueTaiClassNum))
            pulseWeight = self.getPulse2MedicineWeight(pulse,medicineNode)
            pulseOfMedicinePro = self.myLog((pulseWeight+1.0)/(medicineWeight+pulseClassNum))
            totalPro = medicnePro+symptomOfMedicinePro+tongueZhiOfMedicinePro+tongueTaiOfMedicinePro+pulseOfMedicinePro
            # print(medicineNode[Mysql2Neo4j.NODENAME])
            # print("symptomOfMedicinePro"+str(symptomOfMedicinePro))
            # print("tongueZhiOfMedicinePro"+str(tongueZhiOfMedicinePro))
            # print("tongueTaiOfMedicinePro"+str(tongueTaiOfMedicinePro))
            # print("pulseOfMedicinePro"+str(pulseOfMedicinePro))
            # print("medicnePro"+str(medicnePro))
            # print("totalPro"+str(totalPro))
            medicineDic[self.PROBABILITY] = totalPro
            medicineDicList.append(medicineDic)
        # 按照后验概率降序排列
        rankList = sorted(medicineDicList,key=lambda k:k[self.PROBABILITY],reverse=True)
        topMedicineList = []
        topk = 0
        if(len(rankList)<self.TOPK):
            topk=len(rankList)
        else:
            topk = self.TOPK
        for i in range(topk):
            topMedicineList.append(rankList[i])
        lableList = self.chooseLablesNumber(topMedicineList)
        return lableList

    # 确定多标签的标签个数
    def chooseLablesNumber(self,topMedicineList):
        totalInv = 0
        listLen = len(topMedicineList)
        for i in range(listLen-1):
            inv = topMedicineList[i][self.PROBABILITY]-topMedicineList[i+1][self.PROBABILITY]
            totalInv+=inv
        avgInv = round(totalInv/(listLen-1),self.POINTACCURACY)
        lableList = []
        lableList.append(topMedicineList[0][self.NAME])
        for i in range(listLen-1):
            perInv = topMedicineList[i][self.PROBABILITY]-topMedicineList[i+1][self.PROBABILITY]
            if perInv>=avgInv:
                lableList.append(topMedicineList[i+1][self.NAME])
            else:
                break
        return lableList

    # 防止连乘下溢问题，采用对数相加
    def myLog(self,n):
        power = math.log(n,self.LOGBASE)
        return round(power,self.POINTACCURACY)

    def getSymptome2MedicineWeight(self,symptom,medicineNode):
        neoDb = Neo4jOpt()
        nodes = neoDb.selectNodeElementsFromDB(Mysql2Neo4j.SYMPTOMTAG, properties={Mysql2Neo4j.NODENAME: symptom})
        if(len(nodes)==0):
            return 0
        node = nodes[0]
        relations = neoDb.selectRelationshipsFromDB(node, Mysql2Neo4j.SYMPTOM2MEDICINETAG,medicineNode)
        if(len(relations)==0):
            return 0
        relation = relations[0]
        weight = relation[Mysql2Neo4j.RELATIONWEIGHT]
        return weight

    def getTongueZhi2MedicineWeight(self,tongueZhi,medicineNode):
        neoDb = Neo4jOpt()
        nodes = neoDb.selectNodeElementsFromDB(Mysql2Neo4j.TONGUEZHITAG, properties={Mysql2Neo4j.NODENAME: tongueZhi})
        if(len(nodes)==0):
            return 0
        node = nodes[0]
        relations = neoDb.selectRelationshipsFromDB(node, Mysql2Neo4j.TONGUEZHI2MEDICINETAG,medicineNode)
        if(len(relations)==0):
            return 0
        relation = relations[0]
        weight = relation[Mysql2Neo4j.RELATIONWEIGHT]
        return weight

    def getTongueTai2MedicineWeight(self, tongueTai, medicineNode):
        neoDb = Neo4jOpt()
        nodes = neoDb.selectNodeElementsFromDB(Mysql2Neo4j.TONGUETAITAG, properties={Mysql2Neo4j.NODENAME: tongueTai})
        if(len(nodes)==0):
            return 0
        node = nodes[0]
        relations = neoDb.selectRelationshipsFromDB(node, Mysql2Neo4j.TONGUETAI2MEDICINETAG,medicineNode)
        if(len(relations)==0):
            return 0
        relation = relations[0]
        weight = relation[Mysql2Neo4j.RELATIONWEIGHT]
        return weight

    def getPulse2MedicineWeight(self, pulse, medicineNode):
        neoDb = Neo4jOpt()
        nodes = neoDb.selectNodeElementsFromDB(Mysql2Neo4j.PULSETAG, properties={Mysql2Neo4j.NODENAME: pulse})
        if(len(nodes)==0):
            return 0
        node = nodes[0]
        relations = neoDb.selectRelationshipsFromDB(node, Mysql2Neo4j.PULSE2MEDICINETAG,medicineNode)
        if(len(relations)==0):
            return 0
        relation = relations[0]
        weight = relation[Mysql2Neo4j.RELATIONWEIGHT]
        return weight

if __name__=='__main__':
    naiveBayesDM=NaiveBayesDM()
    # naiveBayesDM.createGraph()
    naiveBayesDM.input_multiple_syptomes(['头痛','发热','鼻塞'],'红','白','浮')
    naiveBayesDM.cross_validation()