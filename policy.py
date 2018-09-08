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


md_time = time.strftime('%Y-%m-%d %H:%M:%S',time.localtime(time.time()))
policys = os.popen('bppllist').read().strip().split()
for policy in policys:
    tmp_dict = {}
    tmp_dict['md_time'] = md_time
    tmp_dict['policy'] = policy
    policy_clients = os.popen("bpplclients %s -L | awk '{print $2}' | xargs" %policy).read().strip('\n').split()
    policy_info = os.popen("bpplinfo %s  -U | egrep 'Residence:|Volume Pool:|Policy Type:' | awk -F ':' '{print $2}'" %policy).read().strip(' ').split('\n')
    tmp_dict['policy_type'] = policy_info[0].strip()
    tmp_dict['policy_stu'] = policy_info[1].strip()
    tmp_dict['policy_pool'] = policy_info[2].strip()


    policy_sched_label = os.popen("bpplsched %s -U | egrep 'Schedule' | grep -v Default-Application-Backup | awk '{print $2}'" %policy).readlines()

    for label in policy_sched_label:
        policy_sched = os.popen("bpplsched %s -U -label %s | egrep 'Type:|Frequency:|Retention Level:' | awk -F ':' '{print $2}'" %(policy,label.strip())).readlines()
        tmp_dict['policy_sched_type'] = policy_sched[0].strip()
        tmp_dict['policy_windows'] = os.popen("bpplsched %s -U -label %s | tail -1 | awk '{print $2,$3,$5}'" %(policy,label.strip())).read().strip()
        tmp_dict['policy_include'] = os.popen("bpplinclude %s -U | awk -F ':' '{print $2}' | xargs" %policy).read().strip('\n')
        if tmp_dict['policy_sched_type'] == "User Backup" or tmp_dict['policy_sched_type'] == "User Archive":
            tmp_dict['policy_sched_frequency'] = "None"
            tmp_dict['policy_sched_retention'] = policy_sched[1].strip()[3:].strip(')')
        else:
            tmp_dict['policy_sched_frequency'] = policy_sched[1].strip()
            tmp_dict['policy_sched_retention'] = policy_sched[2].strip()[3:].strip(')')       
    
        if len(policy_clients) > 1:
            for clients in policy_clients:
                tmp_dict['policy_clients'] = clients
                db.insert('policy',tmp_dict)        
                '''print '%s,%s,%s,%s,%s,%s,%s,%s,%s,%s' \
                    %(policy,policy_clients,policy_stu,\
                    policy_pool,policy_type,policy_sched_type,\
                    policy_sched_retention,policy_sched_frequency,\
                    policy_include,policy_windows)'''
        else:
            tmp_dict['policy_clients'] = policy_clients[0]
            db.insert('policy',tmp_dict) 
            '''print '%s,%s,%s,%s,%s,%s,%s,%s,%s,%s' \
                %(policy,policy_clients[0],policy_stu,\
                policy_pool,policy_type,policy_sched_type,\
                policy_sched_retention,policy_sched_frequency,\
                policy_include,policy_windows)'''