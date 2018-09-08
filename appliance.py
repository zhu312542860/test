#!/usr/bin/python
#coding:utf-8


#相关联的数据库用户和表结构
'''CREATE USER 'nbu'@'ZJHZ-CMREAD-BACKUP02-MGT-SD' IDENTIFIED BY 'nbu';
grant all privileges on nbu.* to 'nbu'@'ZJHZ-CMREAD-BACKUP02-MGT-SD' identified by 'nbu';

CREATE TABLE IF NOT EXISTS `appliance`(
   `md_time` datetime NOT NULL,    
   `disk_volume` varchar(30) NOT NULL,
   `disk_type` VARCHAR(20) NOT NULL,
   `disk_Capacity` VARCHAR(40) NOT NULL,
   `disk_free` VARCHAR(40) NOT NULL,
   `disk_used_percent` VARCHAR(10) NOT NULL
)'''


import time
import os
from datetime import datetime, timedelta
import MySQLdb

class mysql(object):
    """docstring for mysql"""
    def __init__(self, dbconfig):
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
        #print sql
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
        #print sql
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
        print sql
        #self._cursor.execute(sql)
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

dbconfig = {
    'host':'ZJHZ-CMREAD-BACKUP02-MGT-SD',
    'port':3306,
    'user':'nbu',
    'passwd':'nbu',
    'dbname':'nbu',
    'charset':'utf8'
}
db = mysql(dbconfig)


cmd1 = 'nbdevquery -listdv -L  -stype PureDisk'
puredisks = os.popen(cmd1).readlines()

cmd2 = 'nbdevquery -listdv -L  -stype AdvancedDisk'
advanceddisks = os.popen(cmd2).readlines()

disks = puredisks + advanceddisks
md_time = time.strftime('%Y-%m-%d %H:%M:%S',time.localtime(time.time()))


for disk in disks:
    tmp_dict = {}
    tmp_dict['md_time'] = md_time
    info = disk.strip().split()
    tmp_dict['disk_volume'] = info[1]
    tmp_dict['disk_type'] = info[2]
    tmp_dict['disk_Capacity'] = info[5]
    tmp_dict['disk_free'] = info[6]
    tmp_dict['disk_used_percent'] = info[7]
    


    db.insert("appliance",tmp_dict)

db.close()