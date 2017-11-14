#!/usr/bin/env python
# -*- coding: utf-8 -*-
import jieba
import codecs
import re
from gensim.models import word2vec


def cut_word(inputFile,outputFile):
    with codecs.open(inputFile,'r',encoding='utf-8') as inputf:
        inputf.readline()
        for line in inputf:
            content = line.split('\t')[2]
            # 所有标点符号转成空格
            content = re.sub(ur"[-~!@#$%^&*()_+`=\[\]\\\{\}\"|;':,./<>?·！@#￥%……&*（）——+【】、；‘：“”，。、《》？「『」』]",' ',content)
            cutContentIter = jieba.cut(content)
            with codecs.open(outputFile,'a') as outputf:
                for w in cutContentIter:
                    outputf.write(w.strip().encode('utf-8')+' ')
                outputf.write('\n')

def train_word2vec(inputFile,modelFile):
    sentences = word2vec.LineSentence(inputFile)
    model = word2vec.Word2Vec(sentences,size=300,min_count=1,sg=1)
    model.save(modelFile)



jieba.load_userdict('../../data/syndrome_dic.txt')
if __name__=='__main__':
    ''
    # train_word2vec('../../data/tcm_literature_cutword.txt','../../model/word_vector.vec')




