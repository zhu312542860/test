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

'''从NBU侧获取相关一体机的数据信息'''


import time
import os
from datetime import datetime, timedelta
from mysql import mysql



def get_appliance(db):

	#获取pure disk的信息
	cmd1 = 'nbdevquery -listdv -L  -stype PureDisk'
	puredisks = os.popen(cmd1).readlines()
	
	#获取advance disk的信息
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


if __name__ == '__main__':

	dbconfig = {
		'host':'ZJHZ-CMREAD-BACKUP02-MGT-SD',
		'port':3306,
		'user':'nbu',
		'passwd':'nbu',
		'dbname':'nbu',
		'charset':'utf8'
	}
	db = mysql(dbconfig)
	get_appliance(db)