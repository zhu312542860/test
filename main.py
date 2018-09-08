#!/usr/bin/python
#coding:utf-8


import os
import time
import logging
from mysql import mysql
from appliance import get_appliance
from job import get_jobinfo
from policy import get_policyinfo


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
	
	try:	
		get_appliance(db)
	except Exception,e:
		pass
		
		
	try:	
		get_jobinfo(db)
	except Exception,e:
		pass
	
	try:
		get_policyinfo(db)
	except Exception,e:
		pass