# -*- coding: utf-8 -*-
from gensim.models import word2vec
import codecs
from sklearn.cluster import AffinityPropagation

def get_total_syndrome():
    syndromeList = []
    with codecs.open('../../data/syndrome_dic.txt','r',encoding='utf-8') as f:
        for line in f:
            syndromeList.append(line.strip())
    return syndromeList

def syndrome_cluster():
    totalSyndromeList = get_total_syndrome()
    model = word2vec.Word2Vec.load('../../model/word_vector.vec')
    syndromeNameList = []
    syndromeVectorList = []
    for s in totalSyndromeList:
        if s in model:
            syndromeNameList.append(s)
            syndromeVectorList.append(model[s])
    af = AffinityPropagation(preference=-10).fit(syndromeVectorList)
    cluster_centers_indices = af.cluster_centers_indices_
    n_clusters_ = len(cluster_centers_indices)
    print "聚类个数："+str(n_clusters_)
    print "聚类结果："
    labels = af.labels_
    for k in range(n_clusters_):
        cluster_center_index = cluster_centers_indices[k]
        print '\n'+syndromeNameList[cluster_center_index]+":",
        class_members = labels == k
        for i in range(len(labels)):
            if class_members[i]==True:
                print syndromeNameList[i]+u"、",


if __name__ == '__main__':
    syndrome_cluster()
