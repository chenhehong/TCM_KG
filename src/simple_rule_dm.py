# -*- coding:utf-8 -*-
import sys

current_working_directory = "D:\Python\TCM_KG"
sys.path.append(current_working_directory)
from src.datastore.neo4j_opt import Neo4jOpt
from src.datastore.mysql2neo4j import Mysql2Neo4j
from src.util.list_util import ListUtil
from operator import itemgetter
import copy
from src.base_data_query import BaseDataQuery

class SimpleRuleDM(object):

    TOPK = 5
    TIDS = 'tids'
    NAME = 'name'
    CONFIDENCE = "confidence"
    ID = 'id'

    def input_single_medicine(self,medicine):
        baseDataQuery = BaseDataQuery()
        list = []
        list.append(medicine)
        symptoms = baseDataQuery.get_symptom_from_medicine(list)
        i = 1
        for symptom in symptoms:
            if i>self.TOPK:
                break
            print '症状：' + symptom[BaseDataQuery.SEARCHVALUEKEY]['name'].encode('utf8') + ' 支持度：' + str(symptom[BaseDataQuery.SEARCHWEIGHTKEY])
            i+=1
        tongueZhis = baseDataQuery.get_tonguezhi_from_medicine(list)
        i = 1
        for tongueZhi in tongueZhis:
            if i>self.TOPK:
                break
            print '舌质：'+tongueZhi[BaseDataQuery.SEARCHVALUEKEY]['name'].encode('utf8') + ' 支持度：' + str(tongueZhi[BaseDataQuery.SEARCHWEIGHTKEY])
            i+=1
        tongueTais = baseDataQuery.get_tonguetai_from_medicine(list)
        i = 1
        for tongueTai in tongueTais:
            if i>self.TOPK:
                break
            print '舌苔：'+tongueTai[BaseDataQuery.SEARCHVALUEKEY]['name'].encode('utf8') + ' 支持度：' + str(tongueTai[BaseDataQuery.SEARCHWEIGHTKEY])
            i+=1
        pulses = baseDataQuery.get_pulse_from_medicine(list)
        i = 1
        for pulse in pulses:
            if i>self.TOPK:
                break
            print '脉搏：'+pulse[BaseDataQuery.SEARCHVALUEKEY]['name'].encode('utf8') + ' 支持度：' + str(pulse[BaseDataQuery.SEARCHWEIGHTKEY])
            i+=1

    def input_single_syptom(self, symptom):
        baseDataQuery = BaseDataQuery()
        list = []
        list.append(symptom)
        medicines = baseDataQuery.get_medicine_from_symptom(list)
        i = 1
        for element in medicines:
            if i > self.TOPK:
                break
            print '中药：' + element[BaseDataQuery.SEARCHVALUEKEY]['name'].encode('utf8') + ' 支持度：' + str(element[BaseDataQuery.SEARCHWEIGHTKEY])
            i += 1

    def input_single_tonguezhi(self,tonguezhi):
        baseDataQuery = BaseDataQuery()
        list = []
        list.append(tonguezhi)
        medicines = baseDataQuery.get_medicine_from_tonguezhi(list)
        i = 1
        for medicine in medicines:
            if i > self.TOPK:
                break
            print '中药：' + medicine[BaseDataQuery.SEARCHVALUEKEY]['name'].encode('utf8') + ' 支持度：' + str(medicine[BaseDataQuery.SEARCHWEIGHTKEY])
            i += 1

    def input_single_tonguetai(self, tonguetai):
        baseDataQuery = BaseDataQuery()
        list = []
        list.append(tonguetai)
        medicines = baseDataQuery.get_medicine_from_tonguetai(list)
        i = 1
        for element in medicines:
            if i > self.TOPK:
                break
            print '中药：' + element[BaseDataQuery.SEARCHVALUEKEY]['name'].encode('utf8') + ' 支持度：' + str(element[BaseDataQuery.SEARCHWEIGHTKEY])
            i += 1

    def input_single_pulse(self, pulse):
        baseDataQuery = BaseDataQuery()
        list = []
        list.append(pulse)
        medicines = baseDataQuery.get_medicine_from_pulse(list)
        i = 1
        for element in medicines:
            if i > self.TOPK:
                break
            print '中药：' + element[BaseDataQuery.SEARCHVALUEKEY]['name'].encode('utf8') + ' 支持度：' + str(element[BaseDataQuery.SEARCHWEIGHTKEY])
            i += 1

    def input_multiple_medicine(self,medicine):
        baseDataQuery = BaseDataQuery()
        IDS = 'ids'
        NAME = 'name'
        medicineLenth = len(medicine)
        symptoms = baseDataQuery.get_symptom_from_medicine(medicine)
        symptomsFilter = []
        for e in symptoms:
            tidlists = []
            for i in range(medicineLenth):
                tidlists.append(e[BaseDataQuery.SEARCHIDSKEY+str(i+1)])
            intersectList = ListUtil.list_intersect(tidlists)
            if len(intersectList)>0:
                new = {}
                new[IDS] = intersectList
                new[NAME] = e[BaseDataQuery.SEARCHVALUEKEY]['name']
                symptomsFilter.append(new)
#       获取中药组合为前提时的事件数,计算置信度要用
        totleListId = []
        for e in symptomsFilter:
            totleListId.append(e[IDS])
        mergeList = ListUtil.list_merge(totleListId)
        totleCount = len(mergeList)
        print(mergeList)

        tongueZhis = baseDataQuery.get_tonguezhi_from_medicine(medicine)
        tongueZhisFilter = []
        for e in tongueZhis:
            tidlists = []
            for i in range(medicineLenth):
                tidlists.append(e[BaseDataQuery.SEARCHIDSKEY+str(i+1)])
            intersectList = ListUtil.list_intersect(tidlists)
            if len(intersectList)>0:
                new = {}
                new[IDS] = intersectList
                new[NAME] = e[BaseDataQuery.SEARCHVALUEKEY]['name']
                tongueZhisFilter.append(new)

        tongueTais = baseDataQuery.get_tonguetai_from_medicine(medicine)
        tongueTaisFilter = []
        for e in tongueTais:
            tidlists = []
            for i in range(medicineLenth):
                tidlists.append(e[BaseDataQuery.SEARCHIDSKEY+str(i+1)])
            intersectList = ListUtil.list_intersect(tidlists)
            if len(intersectList)>0:
                new = {}
                new[IDS] = intersectList
                new[NAME] = e[BaseDataQuery.SEARCHVALUEKEY]['name']
                tongueTaisFilter.append(new)

        pulses = baseDataQuery.get_pulse_from_medicine(medicine)
        pulsesFilter = []
        for e in pulses:
            tidlists = []
            for i in range(medicineLenth):
                tidlists.append(e[BaseDataQuery.SEARCHIDSKEY+str(i+1)])
            intersectList = ListUtil.list_intersect(tidlists)
            if len(intersectList)>0:
                new = {}
                new[IDS] = intersectList
                new[NAME] = e[BaseDataQuery.SEARCHVALUEKEY]['name']
                pulsesFilter.append(new)
        combineList = []
        for sy in symptomsFilter:
            for tz in tongueZhisFilter:
                tzLists = []
                tzLists.append(sy[IDS])
                tzLists.append(tz[IDS])
                tzCombineList = ListUtil.list_intersect(tzLists)
                if len(tzCombineList)>0:
                    for tt in tongueTaisFilter:
                        ttLists = []
                        ttLists.append(tt[IDS])
                        ttLists.append(tzCombineList)
                        ttCombineList = ListUtil.list_intersect(ttLists)
                        if len(ttCombineList)>0:
                            for pu in pulsesFilter:
                                combineName = sy[NAME]+','+tz[NAME]+','+tt[NAME]+','+pu[NAME]
                                lists = []
                                lists.append(ttCombineList)
                                lists.append(pu[IDS])
                                combineIdList = ListUtil.list_intersect(lists)
                                if(len(combineIdList)>0):
                                    combineElement = {}
                                    combineElement['name'] = combineName
                                    combineElement['weight'] = len(combineIdList)
                                    combineElement['confidence'] = float(len(combineIdList))/totleCount
                                    combineList.append(combineElement)
        combineSortList = sorted(combineList,key=itemgetter('weight'),reverse=True)
        i = 1
        for element in combineSortList:
            if i > self.TOPK:
                break
            print '症状和舌脉组合：' + element['name'].encode('utf8') + ' 支持度：' + str(element['weight'])+' 置信度:'+str(element['confidence'])
            i += 1


    # 输入组合中医症状、舌脉，模型输出最相关的中药组合名称
    def input_multiple_syptomes(self,symptomList,tongueZhi,tongueTai,pulse,support=1):
        baseDataQuery = BaseDataQuery()
        tongueZhiList = []
        tongueZhiList.append(tongueZhi)
        tongueTaiList = []
        tongueTaiList.append(tongueTai)
        pulseList = []
        pulseList.append(pulse)
        symptomLenth = len(symptomList)
        medicines = baseDataQuery.get_medicine_from_symptom(symptomList)
        symptomMedicines = []
        for e in medicines:
            tidlists = []
            if symptomLenth>1:
                for i in range(symptomLenth):
                    tidlists.append(e[BaseDataQuery.SEARCHIDSKEY + str(i + 1)])
                intersectList = ListUtil.list_intersect(tidlists)
            else:
                intersectList = e[BaseDataQuery.SEARCHIDSKEY]
            if len(intersectList) > 0:
                new = {}
                new[self.TIDS] = intersectList
                new[self.NAME] = e[BaseDataQuery.SEARCHVALUEKEY]['name']
                new[self.ID]=e[BaseDataQuery.SEARCHVALUEKEY]['id']
                symptomMedicines.append(new)
        medicines = baseDataQuery.get_medicine_from_tonguezhi(tongueZhiList)
        tongueZhiMedicines = []
        for e in medicines:
            new = {}
            new[self.TIDS] = e[BaseDataQuery.SEARCHIDSKEY]
            new[self.NAME] = e[BaseDataQuery.SEARCHVALUEKEY]['name']
            new[self.ID]=e[BaseDataQuery.SEARCHVALUEKEY]['id']
            tongueZhiMedicines.append(new)
        medicines = baseDataQuery.get_medicine_from_tonguetai(tongueTaiList)
        tongueTaiMedicines = []
        for e in medicines:
            new = {}
            new[self.TIDS] = e[BaseDataQuery.SEARCHIDSKEY]
            new[self.NAME] = e[BaseDataQuery.SEARCHVALUEKEY]['name']
            new[self.ID]=e[BaseDataQuery.SEARCHVALUEKEY]['id']
            tongueTaiMedicines.append(new)
        medicines = baseDataQuery.get_medicine_from_pulse(pulseList)
        pulseMedicines = []
        for e in medicines:
            new = {}
            new[self.TIDS] = e[BaseDataQuery.SEARCHIDSKEY]
            new[self.NAME] = e[BaseDataQuery.SEARCHVALUEKEY]['name']
            new[self.ID]=e[BaseDataQuery.SEARCHVALUEKEY]['id']
            pulseMedicines.append(new)
        freItemList = []
        for sm in symptomMedicines:
            # 指示是否找到id和ids有交集的中药
            find = False
            for tzm in tongueZhiMedicines:
                if(int(tzm[self.ID])==int(sm[self.ID])):
                    lists = []
                    lists.append(sm[self.TIDS])
                    lists.append(tzm[self.TIDS])
                    interList = ListUtil.list_intersect(lists)
                    if len(interList)>0:
                        sm[self.TIDS] = interList
                        find = True
                        break
            if find==False:
                continue
            find = False
            for ttm in tongueTaiMedicines:
                if(int(ttm[self.ID])==int(sm[self.ID])):
                    lists = []
                    lists.append(sm[self.TIDS])
                    lists.append(tzm[self.TIDS])
                    interList = ListUtil.list_intersect(lists)
                    if len(interList)>0:
                        sm[self.TIDS] = interList
                        find = True
                        break
            if find==False:
                continue
            find = False
            for pm in pulseMedicines:
                if(int(pm[self.ID])==int(sm[self.ID])):
                    lists = []
                    lists.append(sm[self.TIDS])
                    lists.append(tzm[self.TIDS])
                    interList = ListUtil.list_intersect(lists)
                    if len(interList)>0:
                        sm[self.TIDS] = interList
                        find = True
                        break
            # 满足支持度的一次项添加进来
            if find==True and len(sm[self.TIDS])>=support:
                freItemList.append(sm)
        # 计算事件总数
        lists = []
        for e in freItemList:
            lists.append(e[self.TIDS])
        totleList = ListUtil.list_intersect(lists)
        totleCount = len(totleList)

    #   利用apriori算法生成所有支持度大于support的中药组合
        k = 1
        doc = {}
        doc[k] = []
        for e in freItemList:
            item = {}
            item[self.TIDS] = e[self.TIDS]
            list = []
            list.append(e[self.NAME])
            item[self.NAME]=list
            list = []
            list.append(e[self.ID])
            item[self.ID]=list
            doc[k].append(item)
        while True:
            nextFre = self.apriori_gen_fre(doc[k],doc[1],support)
            if len(nextFre)==0:
                break
            k+=1
            doc[k] = nextFre
        #   计算置信度并排序
        allItem = []
        # 只有一种中药的排除掉
        for i in range(2,k+1):
            for j in range(len(doc[i])):
                item = doc[i][j]
                newItem = {}
                newItem[self.NAME] = item[self.NAME]
                newItem[self.TIDS] = item[self.TIDS]
                newItem[self.CONFIDENCE] = float(len(item[self.TIDS]))/totleCount
                allItem.append(newItem)
        newAllItem = sorted(allItem,key=itemgetter(self.CONFIDENCE),reverse=True)
        if len(newAllItem)==0:
            print "暂无符合要求的中药组合！"
            return
        i=1
        for e in newAllItem:
            if(i>self.TOPK):
                break
            name = ''
            for n in e[self.NAME]:
               name=name+n+" "
            print '中药组合：'+name.encode('utf8')+' 支持度:'+str(len(e[self.TIDS]))+' 置信度：'+str(e[self.CONFIDENCE])
            i+=1


    # 由前一项数产生下一项数的频繁集：产生候选集>>剪枝
    def apriori_gen_fre(self,preList,F1List,support):
        list = []
        for e in preList:
            list.append(e[self.ID])
        newList = ListUtil.list_merge(list)
        C1List = []
        for e in F1List:
            if(e[self.ID][0] in newList):
                C1List.append(e)
        nextList = []
        # i=0
        for c in C1List:
            for p in preList:
                lists = []
                lists.append(c[self.TIDS])
                lists.append(p[self.TIDS])
                intersectList = ListUtil.list_intersect(lists)
                # 避免产生重复
                if(c[self.ID][0]>max(p[self.ID]) and len(intersectList)>=support):
                    item = copy.deepcopy(p)
                    item[self.ID].append(c[self.ID][0])
                    item[self.TIDS] = intersectList
                    item[self.NAME].append(c[self.NAME][0])
                    # if i>65536:
                    #     print item
                    # i+=1
                    nextList.append(item)
        return nextList


    def is_symptom_with_tonguezhi(self,symptom,tonguezhi):
        cql = 'MATCH (s:' + Mysql2Neo4j.SYMPTOMTAG + ')-[r]->(e:' + Mysql2Neo4j.TONGUEZHITAG + ") WHERE s.name='"+symptom+"' and e.name='"+tonguezhi+"' return r"
        neoDb = Neo4jOpt()
        result = neoDb.graph.data(cql)
        if len(result)>0:
            print "存在"
        else:
            print "矛盾"


    def is_symptom_with_tonguetai(self, symptom, tonguetai):
        cql = 'MATCH (s:' + Mysql2Neo4j.SYMPTOMTAG + ')-[r]->(e:' + Mysql2Neo4j.TONGUETAITAG + ") WHERE s.name='"+symptom+"' and e.name='" + tonguetai + "' return r"
        neoDb = Neo4jOpt()
        result = neoDb.graph.data(cql)
        if len(result)>0:
            print "存在"
        else:
            print "矛盾"


    def is_symptom_with_pulse(self, symptom, pulse):
        cql = 'MATCH (s:' + Mysql2Neo4j.SYMPTOMTAG + ')-[r]->(e:' + Mysql2Neo4j.PULSETAG + ") WHERE s.name='"+symptom+"' and e.name='" + pulse + "' return r"
        neoDb = Neo4jOpt()
        result = neoDb.graph.data(cql)
        if len(result)>0:
            print "存在"
        else:
            print "矛盾"


if __name__ =='__main__':
    simpleRuleDM = SimpleRuleDM()
    # tagId = int(sys.argv[1])
    # print tagId
    # print sys.argv[2]
    # if tagId==1:
    #     dataSerach.input_single_medicine(sys.argv[2])
    # if tagId==2:
    #     list = []
    #     for i in range(2,len(sys.argv)):
    #         list.append(sys.argv[i])
    #     dataSerach.input_multiple_medicine(list)
    # if tagId==3:
    #     dataSerach.input_single_syptom(sys.argv[2])
    # if tagId==4:
    #     dataSerach.input_single_tonguezhi(sys.argv[2])
    # if tagId==5:
    #     dataSerach.input_single_tonguetai(sys.argv[2])
    # if tagId==6:
    #     dataSerach.input_single_pulse(sys.argv[2])
    simpleRuleDM.input_single_medicine('当归')
    simpleRuleDM.input_single_tonguezhi('舌质淡')
    simpleRuleDM.input_single_tonguetai('苔黄')
    simpleRuleDM.input_single_syptom('头痛')
    simpleRuleDM.input_single_pulse('弦滑')
    simpleRuleDM.input_multiple_medicine(['当归', '麻黄'])
    simpleRuleDM.is_symptom_with_tonguezhi("不思饮食", "淡")
    simpleRuleDM.is_symptom_with_tonguetai("胃脘疼痛", "厚腻")
    simpleRuleDM.is_symptom_with_pulse("脘腹满闷", "滑")
    simpleRuleDM.input_multiple_syptomes(['头痛'], '红', '苔黄', '弦滑')