# -*- coding:utf-8 -*-

from __future__ import absolute_import
from __future__ import print_function

from py2neo import Node, Relationship, NodeSelector
from py2neo.database import Graph
from py2neo.database.auth import authenticate

_user = 'neo4j'
_password = '123456'
_service_ip = '127.0.0.1:7474'

class Neo4jOpt(object):

    def __init__(self, user=_user, password=_password, service_ip = _service_ip):
        self.user = user;
        self.password = password
        self.service_ip = service_ip
        self.graph = self.connectGraph()
        self.selector = NodeSelector(self.graph)
                
    def connectGraph(self):
        authenticate(self.service_ip, self.user, self.password)
        graph = Graph('http://%s/db/data/' % self.service_ip)
        return graph

    '''
    #2016年8月9号修改
    #把创建节点和创建关系给抽象化
    #属性添加用自带的创建函数
    '''
    def createNode(self, nodeType=[], properties={}):
        node = Node(*nodeType, **properties)
        self.constructSubGraphInDB(node)

    def createRelationship(self, relationshipName, node1, node2,propertyDic={}):
        rel = Relationship(node1, relationshipName, node2,**propertyDic)
        self.constructSubGraphInDB(rel)
    
    def unionSubGraphs(self, subGraphs):
        if len(subGraphs) <= 1:
            return subGraphs[0]
        unionGraph = subGraphs[0] | subGraphs[1]
        for i in range(2, len(subGraphs)):  # range is [...)
            unionGraph = (unionGraph | subGraphs[i])
            
        return unionGraph
    
    def constructSubGraphInDB(self, subGraph, primary_label = None, primary_key = []):
        '''
        get a graph's new transactions
        '''
        '''
        #2016年8月3日 16:05:34：修改了创建方式，由merge()函数实现，避免重复创建节点、关系
        #参数说明：subgraph – a Node, Relationship or other Subgraph object
                primary_label – label on which to match any existing nodes
                primary_key – property key(s) on which to match any existing nodes
        #存在疑问：单个节点创建后，会仿照相同类型的结点，建立关系，怎么回事？
        #2016.8.8 修改了参数primary_key，改为列表，以便当作不定参数输入（原函数 merge要求的是不定参数）
        '''
        '''
        trs = self.graph.begin()  # autocommit = false
        trs.create(subGraph)
        trs.commit()
        '''
        self.graph.merge(subGraph, primary_label, *primary_key)
    
    '''
    query records from graph
    '''

    def selectFirstNodeElementsFromDB(self, labels = None, condition = [], properties = {}):
        if labels == None:
            selected = self.selector.select().first()
        else :
            if properties == None:
                selected = self.selector.select(labels).first()
            else :
                selected = self.selector.select(labels).where(*condition,**properties).first()
        return selected

    def selectNodeElementsFromDB(self, labels = None, condition = [], properties = {}):
        '''
        select elements from graph
        #通过selector查询的暂时只能是结点类型的，关系类型的查询暂时使用match()函数匹配
        #参数说明：labels – node labels to match
                properties – set of property keys and values to match
                                        注：关于labels参数：如果输入的参数元组是空的，默认是查询所有节点
        #返回数据：对查询到的结点数据以列表的形式返回
        ''' 
        nodes = []
        if labels == None:
            selected = self.selector.select()
        else :
            if properties == None:
                selected = self.selector.select(labels)
            else :
                selected = self.selector.select(labels).where(*condition,**properties)
        for node in selected:
            nodes.append(node)
        return nodes   

    
    def selectRelationshipsFromDB(self, start_node = None, rel_type = None, end_node = None, bidirectional = False, limit = None):
        '''
        select relationships from graph
        #函数功能：实现从图数据库中查找与输入参数相对应的关系
        #函数说明：通过调用graph.match()函数来实现关系的查找操作
        #参数说明：start_node – start node of relationships to match (None means any node)
                rel_type – type of relationships to match (None means any type)
                end_node – end node of relationships to match (None means any node)
                bidirectional – True if reversed relationships should also be included
                limit – maximum number of relationships to match (None means unlimited)
        #返回数据：return all relationships with specific criteria
        '''
        relationships = []
        rels = self.graph.match(start_node, rel_type, end_node, bidirectional, limit)
        for rel in rels:
            relationships.append(rel)
        return relationships

    def selectFirstRelationshipsFromDB(self, start_node = None, rel_type = None, end_node = None, bidirectional = False, limit = None):
        relationships = []
        rels = self.graph.match(start_node, rel_type, end_node, bidirectional, limit)
        for rel in rels:
            relationships.append(rel)
        if(len(relationships)==0):
            return None
        else:
            return relationships[0]
        
    '''
    update or delete elements from graph
    '''
        
    '''
    截至2016年8月4日21:06:25，完成至此
    '''
    def deleteNodeFromDB(self, node):
        '''
        delete node from graph
        #函数功能：实现对单结点node的删除操作
        #函数说明：首先判断该结点存在与否：
                    1、否：直接返回True;
                    2、是：继续下一步判断；
                                        然后再判断与其它结点是否有关系存在：
                    1、否：直接删除该结点；
                    2、是：先删除与其它结点的关系，然后再删除该结点
        #参数说明：node – 待删除的结点
        #返回数据：如果完成删除操作，则返回True,否则返回异常提示
        #2016年8月9号修改
        #原先删除的关系只有指向其他节点的关系，没有删掉指向自己的关系，改为删掉两个方向的关系
        '''
        if self.graph.exists(node):
            innerRelationships = self.selectRelationshipsFromDB(node, None, None, True, None)

            if len(innerRelationships) < 1:
                self.graph.delete(node)
            else:
                for rel in innerRelationships:
                    self.graph.separate(rel)

                self.graph.delete(node)

    def deleteNodesFromDB(self, nodes):
        '''
        delete nodes(tuple、set or frozenset) from graph
        #函数功能：实现对多结点的删除操作
        #函数说明：首先判断传入的结点参数nodes是单个节点还是集合：
                    1、单个结点：直接调用deleteNodeFromDB(self.node);
                    2、集合：依次提取每个结点，并调用deleteNodeFromDB(self.node)。
        #参数说明：nodes - 由执行selectNodesFromDB()函数返回的结果
        #返回结果：无
        '''
        for node in nodes:
            self.deleteNodeFromDB(node)

        
    def deleteRelationshipsFromDB(self, relationships):        
        '''
        select relationships from graph
        #函数功能：实现对结点之间关系的删除操作
        #函数说明：首先判断传入的关系参数relationships是单个关系还是集合：
                    1、单个关系：直接调用graph.separate();
                    2、集合：依次提取每个关系，并调用graph.separate()来完成删除操作。
        #参数说明：relationships – 由执行selectRelaionshipsFromDB()函数返回的结果
        #返回数据：如果完成删除操作，则返回True,否则返回异常提示
        #2016年8月9号修改
        len(relationship)使用错误，返回的不是关系的长度，而是关系里属性的多少
        separate(subgraph)可以接受一个子图最为参数，用for循环反而会报错
        '''
        if self.graph.exists(relationships):       
            self.graph.separate(relationships)               

            
    def updateKeyInNode(self, node, properties = {}):
        if self.graph.exists(node):
            for key in properties:
                node[key] = properties[key]
            self.graph.push(node)
            return node
        else :
            return None
    
    def updateKeyInRelationship(self, relation, relationshipName=None,properties = {}):
        if self.graph.exists(relation):
            for key in properties:
                relation[key] = properties[key]
            self.graph.push(relation)
            return relation
        else :
            return None
        
    def testElementsInDB(self,subGraph):
        '''
        For test
        '''
        print('evaluate')
        
        n = self.graph.evaluate("MATCH (a:teacher) RETURN a")
        print(n)
        print('run')
        n = self.graph.run("MATCH (a:teacher) where a.age = {x} RETURN a",x = 37)
        for node in n:
            print(node)

if __name__ == '__main__':
    
    print("directly excute neoDataGraphOpt...")
    #初始化,输入数据库帐号密码
    neoObj = Neo4jOpt("neo4j", "123456")
    medicineNodes = neoObj.selectNodeElementsFromDB('medicine', condition=[],
                                                   properties={"id": int(248)})
    medicineNode = medicineNodes[0]
    ntids = [2,1,3]
    neoObj.updateKeyInNode(medicineNode, properties={'tid': ntids, "weight": 3})
    #创建节点,返回节点
    #node1=neoObj.createNode([u"Person"], {u'name':u'Jerr'})
    #把节点、关系、子图加入数据库，自动过滤掉已存在的，无返回值
    #neoObj.constructSubGraphInDB(node1)

    # #根据标签，条件，属性查找节点，返回节点列表
    # node2=neoObj.selectNodeElementsFromDB('disease',condition=[],properties={'name':'胃痛'})
    # print(node2[0]['name'])
    # node3=neoObj.selectNodeElementsFromDB("Person", condition=[], properties={u'name':u'Ian'})
    #
    # #创建关系，指定标签，起始、终止节点，以及属性列表，返回关系
    # rel=neoObj.createRelationship("KNOWS", node1, node2[0], propertyDic={})
    # neoObj.constructSubGraphInDB(rel)
    #
    # #查找起始节点与终止节点之间的关系
    # print("找到的关系：",neoObj.selectRelationshipsFromDB(node2[0], None, node3[0], True, None))
    #
    # #修改关系属性，返回修改完的关系
    # neoObj.updateKeyInRelationship(rel, True,relationshipName="friend" ,properties={})
    #
    # #修改节点属性，返回修改完的节点
    # neoObj.updateKeyInNode(node1, True, properties={u'name':u'Jrre'})
    #
    # #添加节点标签，返回修改完的节点
    # neoObj.addLabelsInNode(node1,*['star'])

    #删除关系，无返回
    #neoObj.deleteRelationshipsFroNeoDataGraphOpt
    #删除节点，无返回
    #neoObj.deleteNodeFromDB(node1)
    #输入文件地址，开始节点名，终止节点名，两者之间的关系名
    #neoObj.addLink(u'G:\\Xi\\学习\\大数据饮食推荐\\词典\\shicai2bingzheng_links.txt', '食材','病症','关联')