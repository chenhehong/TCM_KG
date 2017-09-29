# -*- coding:utf-8 -*-
import codecs

from sklearn.cross_validation import KFold

from src.datastore.neo4j_opt import Neo4jOpt
from src.util.file_util import FileUtil
from src.util.list_util import ListUtil


class WeightPropagationDM(object):

    MEDICINETAG = 'medicine'
    SYMPTOMTAG = "symptom"
    SYMPTOM2MEDICINETAG = "symptom2medicine"
    SYMPTOM2SYMPTOMTAG = "symptom2symptom"
    NODENAME = "name"
    NODEWEIGHT = "weight"
    NODETID = 'tid'
    RELATIONWEIGHT = 'weight'
    RELATIONTID = 'tid'

    LOGFILE = '../data/log.txt'

    TOPK = 100
    CRVNUMFOLDER = 90

    POINTACCURACY = 4

    BFSLAYER = 2
    SYM2SYMTHRESHOLD = 8
    SYM2MEDTHRESHOLD = 6

    def cross_validation(self,in_file):
        preFile = '../data/pre.txt'
        randomState = 43
        idList = []
        symptomList = []
        medicineList = []
        with codecs.open(in_file, 'r', encoding='utf-8') as f:
            for line in f:
                splits = line.split("||")
                symptoms = splits[1].split(',')
                medicines = splits[2].split(',')
                idList.append(splits[0])
                symptomList.append(symptoms)
                medicineList.append(medicines)
        # 临时用全部的数据构建图数据库
        # trainIndex = [x for x in range(len(idList))]
        # self.createGraph([idList[i] for i in trainIndex],[symptomList[i] for i in trainIndex],[medicineList[i] for i in trainIndex])
        kf = KFold(len(idList), n_folds=self.CRVNUMFOLDER, shuffle=True, random_state=randomState)
        numFold = 0
        for trainIndex,testIndex in kf:
            numFold+=1
            FileUtil.print_string(str(numFold)+'th validation:',True)
            # self.createGraph(idList[trainIndex],symptomList[trainIndex],medicineList[trainIndex])
            yTrue = []
            yPre = []
            FileUtil.print_string(str(numFold)+'th validation:',self.LOGFILE,True)
            for i in testIndex:
                testId = idList[i]
                print "**********************************************"
                print str(testId)
                preMedicineList = self.predict_medicine(symptomList[i])
                if(len(preMedicineList)==0):
                    continue
                preString = str(testId) + ' '
                for m in preMedicineList:
                    preString = preString + m + ','
                FileUtil.add_string(preFile, preString)
                yTrue.append(medicineList[i])
                yPre.append(preMedicineList)
            precision,recall = self.classification_evaluate(yTrue,yPre)
            FileUtil.print_string('precision:'+str(precision),self.LOGFILE,True)
            FileUtil.print_string('recall:'+str(recall),self.LOGFILE,True)
            F1 = 2*precision*recall/(precision+recall)
            FileUtil.print_string('F1:'+str(F1),self.LOGFILE,True)

    def predict_medicine(self,syptomList):
        neoDb = Neo4jOpt()
        # 生成以给定症状为中心形成的广度子图：中心症状的权值各自传播给子节点后，再把各个子图叠加起来
        symptomeWeightDic = {}
        for s in syptomList:
            initWeight = 1
            symptomNode = neoDb.selectFirstNodeElementsFromDB(self.SYMPTOMTAG, properties={self.NODENAME: s})
            if(symptomNode==None):
                continue
            # 如果只有一层
            if(self.BFSLAYER<2):
                if (symptomeWeightDic.get(s) == None):
                    symptomeWeightDic[s] = initWeight
                else:
                    symptomeWeightDic[s] += initWeight
                continue
            bfsLayerDic = {}
            bfsLayerDic[1] = {s:initWeight}
            symptomVisitDic = {}
            symptomVisitDic[s]=True
            #获取特定层次的广度图
            for currentLayerNum in range(1,self.BFSLAYER):
                nextLayerNum = currentLayerNum+1
                bfsLayerDic[nextLayerNum] = {}
                # 获取每一层节点的子节点，并赋权重
                for (parentName,parentWeight) in bfsLayerDic[currentLayerNum].items():
                    symptomNode = neoDb.selectFirstNodeElementsFromDB(self.SYMPTOMTAG, properties={self.NODENAME: parentName})
                    rels = neoDb.selectRelationshipsFromDB(symptomNode,self.SYMPTOM2SYMPTOMTAG,bidirectional=True)
                    for rel in rels:
                        childName = rel.start_node()[self.NODENAME] if rel.end_node()[self.NODENAME]==parentName else rel.end_node()[self.NODENAME]
                        if(symptomVisitDic.get(childName)!=None):
                            continue
                        childWeight = round(rel[self.RELATIONWEIGHT]*1.0*parentWeight/symptomNode[self.NODEWEIGHT],self.POINTACCURACY)
                        # 考虑到同一层的不同父节点可以赋权值给同一个子节点
                        if(bfsLayerDic[nextLayerNum].get(childName)==None):
                            bfsLayerDic[nextLayerNum][childName] = childWeight
                        else:
                            bfsLayerDic[nextLayerNum][childName]+=childWeight
                for nextLayerElementName in bfsLayerDic[nextLayerNum]:
                    symptomVisitDic[nextLayerElementName] = True
            for i in bfsLayerDic:
                for j in bfsLayerDic[i]:
                    if (symptomeWeightDic.get(j) == None):
                        symptomeWeightDic[j] = bfsLayerDic[i][j]
                    else:
                        symptomeWeightDic[j] += bfsLayerDic[i][j]
        for i in symptomeWeightDic:
            print u"症状:"+i+str(symptomeWeightDic[i])
    #   计算每一种和子图症状相连的中药的权值
        medicineWeightDic = {}
        for i in symptomeWeightDic:
            syptomeName = i
            syptomeWeight = symptomeWeightDic[i]
            symptomNode = neoDb.selectFirstNodeElementsFromDB(self.SYMPTOMTAG, properties={self.NODENAME: syptomeName})
            rels = neoDb.selectRelationshipsFromDB(symptomNode, self.SYMPTOM2MEDICINETAG)
            for rel in rels:
                medicineName = rel.end_node()[self.NODENAME].strip()
                medicineWeight = round(rel[self.RELATIONWEIGHT]*syptomeWeight*1.0/symptomNode[self.NODEWEIGHT],self.POINTACCURACY)
                if(medicineWeightDic.get(medicineName)==None):
                    medicineWeightDic[medicineName]=medicineWeight
                else:
                    medicineWeightDic[medicineName] += medicineWeight
        # 转成排序好的列表，[0]是中药名，[1]是权值
        medicineWeightSortedList = sorted(medicineWeightDic.iteritems(),key=lambda d:d[1],reverse=True)
        topMedicineList = []
        if(len(medicineWeightSortedList)>self.TOPK):
            topMedicineList = [medicineWeightSortedList[i] for i in range(self.TOPK)]
        else:
            topMedicineList=[medicineWeightSortedList[i] for i in range(len(medicineWeightSortedList))]
        for i in topMedicineList:
            print u"中药"+i[0]+str(i[1])
        if(len(topMedicineList)==0):
            return []
        preMedicineList = self.chooseTopLables(topMedicineList)
        for i in preMedicineList:
            print i
        return preMedicineList

    # 确定多标签的标签个数
    def chooseTopLables(self, topMedicineList):
        totalInv = 0
        listLen = len(topMedicineList)
        if(listLen<3):
            return [topMedicineList[i][0] for i in range(len(topMedicineList))]
        for i in range(listLen - 1):
            inv = topMedicineList[i][1] - topMedicineList[i + 1][1]
            totalInv += inv
        avgInv = round(totalInv / (listLen - 1), self.POINTACCURACY)
        lableList = []
        lableList.append(topMedicineList[0][0])
        for i in range(listLen - 1):
            perInv = topMedicineList[i][1] - topMedicineList[i + 1][1]
            if perInv >= avgInv:
                lableList.append(topMedicineList[i + 1][0])
            else:
                break
        return lableList

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
            FileUtil.print_string(str(i)+'th precision:'+str(perPrecision)+',recall:'+str(perRecall),self.LOGFILE,True)
            totalPrecision+=perPrecision
            totalRecall+=perRecall
        precison = round(totalPrecision/testNum,5)
        recall = round(totalRecall/testNum,5)
        return precison,recall

    def createGraph(self, idTrainList,symptomTrainList,medicineTrainList):
        FileUtil.print_string(u"开始创建图谱...",self.LOGFILE,True)
        neoDb = Neo4jOpt()
        neoDb.graph.data('MATCH (n) DETACH DELETE n')
        for i in range(len(idTrainList)):
            print("读取id:"+str(idTrainList[i]))
            tid = idTrainList[i]
            for s in symptomTrainList[i]:
                symptomNode = neoDb.selectFirstNodeElementsFromDB(self.SYMPTOMTAG, properties={self.NODENAME: s})
                if(symptomNode==None):
                    tids = [tid]
                    weight = 1
                    neoDb.createNode([self.SYMPTOMTAG], {self.NODENAME:s,self.NODETID:tids,self.NODEWEIGHT:weight})
                else:
                    tids = symptomNode[self.NODETID]
                    tids.append(tid)
                    ntids = list(set(tids))
                    nweight = len(ntids)
                    neoDb.updateKeyInNode(symptomNode,{self.NODETID:ntids,self.NODEWEIGHT:nweight})
            for m in medicineTrainList[i]:
                medicineNode = neoDb.selectFirstNodeElementsFromDB(self.MEDICINETAG, properties={self.NODENAME: m})
                if(medicineNode==None):
                    tids = [tid]
                    weight = 1
                    neoDb.createNode([self.MEDICINETAG], {self.NODENAME:m,self.NODETID:tids,self.NODEWEIGHT:weight})
                else:
                    tids = medicineNode[self.NODETID]
                    tids.append(tid)
                    ntids = list(set(tids))
                    nweight = len(ntids)
                    neoDb.updateKeyInNode(medicineNode,{self.NODETID:ntids,self.NODEWEIGHT:nweight})
            symptoms = symptomTrainList[i]
            for j in range(len(symptoms)):
                s = symptoms[j]
                symptomNode = neoDb.selectFirstNodeElementsFromDB(self.SYMPTOMTAG, properties={self.NODENAME: s})
                # 症状->中药
                for m in medicineTrainList[i]:
                    medicineNode = neoDb.selectFirstNodeElementsFromDB(self.MEDICINETAG, properties={self.NODENAME: m})
                    s2mrel = neoDb.selectFirstRelationshipsFromDB(symptomNode,self.SYMPTOM2MEDICINETAG,medicineNode)
                    if(s2mrel==None):
                        tids = [tid]
                        weight = 1
                        neoDb.createRelationship(self.SYMPTOM2MEDICINETAG, symptomNode, medicineNode,propertyDic={self.RELATIONTID: tids, self.RELATIONWEIGHT: weight})
                    else:
                        tids = s2mrel[self.RELATIONTID]
                        tids.append(tid)
                        ntids = list(set(tids))
                        nweight = len(ntids)
                        neoDb.updateKeyInRelationship(s2mrel, properties={self.RELATIONTID: ntids,self.RELATIONWEIGHT: nweight})
                if(j==len(symptoms)-1):
                    continue
                # 症状-症状
                for k in range(j+1,len(symptoms)):
                    s1 = symptoms[k]
                    if(s==s1):
                        continue
                    symptomNode1 = neoDb.selectFirstNodeElementsFromDB(self.SYMPTOMTAG, properties={self.NODENAME: s1})
                    s2srel = neoDb.selectFirstRelationshipsFromDB(symptomNode,self.SYMPTOM2SYMPTOMTAG,symptomNode1,bidirectional=True)
                    if(s2srel==None):
                        tids = [tid]
                        weight = 1
                        neoDb.createRelationship(self.SYMPTOM2SYMPTOMTAG, symptomNode, symptomNode1,propertyDic={self.RELATIONTID: tids, self.RELATIONWEIGHT: weight})
                    else:
                        tids = s2srel[self.RELATIONTID]
                        tids.append(tid)
                        ntids = list(set(tids))
                        nweight = len(ntids)
                        neoDb.updateKeyInRelationship(s2srel, properties={self.RELATIONTID: ntids,self.RELATIONWEIGHT: nweight})
        self.relationFilter()

    def relationFilter(self):
        neoDb = Neo4jOpt()
        cql = 'MATCH ()-[r:'+self.SYMPTOM2SYMPTOMTAG+']-() where r.'+self.RELATIONWEIGHT+'<'+str(self.SYM2SYMTHRESHOLD)+' delete r'
        neoDb.graph.data(cql)
        cql = 'MATCH ()-[r:'+self.SYMPTOM2MEDICINETAG+']-() where r.'+self.RELATIONWEIGHT+'<'+str(self.SYM2MEDTHRESHOLD)+' delete r'
        neoDb.graph.data(cql)


if __name__ == '__main__':
    weightPropagationDM = WeightPropagationDM()
    weightPropagationDM.cross_validation('../data/symptom2medicine.txt')
    # weightPropagationDM.predict_medicine([u'化痰',u'惊痫'])
