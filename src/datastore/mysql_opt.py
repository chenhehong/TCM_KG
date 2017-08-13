#!/usr/bin/env python
# -*- coding: utf-8 -*-

import MySQLdb
import mysql_config

class MysqlOpt(object):
    """docstring for mysql"""
    def __init__(self, dbconfig=mysql_config._dbconfig):
        self.host = dbconfig['host']
        self.port = dbconfig['port']
        self.user = dbconfig['user']
        self.passwd = dbconfig['passwd']
        self.dbname = dbconfig['dbname']
        self.charset = dbconfig['charset']
        self._conn = None
        self._connect()
        self._cursor = self._conn.cursor()

    def _connect(self):
        try:
            self._conn = MySQLdb.connect(host=self.host,
                port = self.port,
                user=self.user,
                passwd=self.passwd,
                db=self.dbname,
                charset=self.charset)
        except MySQLdb.Error,e:
            print e

    def query(self, sql):
        try:
            result = self._cursor.execute(sql)
        except MySQLdb.Error, e:
            print e
            result = False
        return result

    def select(self, table, column='*', condition=''):
        condition = ' where ' + condition if condition else None
        if condition:
            sql = "select %s from %s %s" % (column,table,condition)
        else:
            sql = "select %s from %s" % (column,table)
        self.query(sql)
        return self._cursor.fetchall()

    def insert(self, table, tdict):
        column = ''
        value = ''
        for key in tdict:
            column += ',' + key
            value += "','" + tdict[key]
        column = column[1:]
        value = value[2:] + "'"
        sql = "insert into %s(%s) values(%s)" % (table,column,value)
        self._cursor.execute(sql)
        self._conn.commit()
        return self._cursor.lastrowid #返回最后的id

    def update(self, table, tdict, condition=''):
        if not condition:
            print "must have id"
            exit()
        else:
            condition = 'where ' + condition
        value = ''
        for key in tdict:
            value += ",%s='%s'" % (key,tdict[key])
        value = value[1:]
        sql = "update %s set %s %s" % (table,value,condition)
        self._cursor.execute(sql)
        return self.affected_num() #返回受影响行数

    def delete(self, table, condition=''):
        condition = 'where ' + condition if condition else None
        sql = "delete from %s %s" % (table,condition)
        # print sql
        self._cursor.execute(sql)
        self._conn.commit()
        return self.affected_num() #返回受影响行数

    def rollback(self):
        self._conn.rollback()

    def affected_num(self):
        return self._cursor.rowcount

    def __del__(self):
        try:
            self._cursor.close()
            self._conn.close()
        except:
            pass

    def close(self):
        self.__del__()

if __name__ == '__main__':

    db = MysqlOpt()
    print(db.select('medicine'))
    # print db.select('msg','id,ip,domain','id>2')
    # print db.affected_num()

    # tdict = {
    #     'ip':'111.13.100.91',
    #     'domain':'baidu.com'
    # }
    # print db.insert('msg', tdict)

    # tdict = {
    #     'ip':'111.13.100.91',
    #     'domain':'aaaaa.com'
    # }
    # print db.update('msg', tdict, 'id=5')

    # print db.delete('msg', 'id>3')

    db.close()