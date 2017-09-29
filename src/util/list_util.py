# -*- coding: utf-8 -*-

class ListUtil(object):

    @staticmethod
    def list_merge(lists):
        newList = []
        for list in lists:
            for element in list:
                if element not in newList:
                    newList.append(element)
        return newList

    @staticmethod
    def list_intersect(lists):
        if len(lists)==0:
            return lists
        if len(lists)==1:
            return lists[0]
        for i in range(1,len(lists)):
            newList = list(set(lists[i-1]).intersection(set(lists[i])))
            lists[i] = newList
        return lists[i]