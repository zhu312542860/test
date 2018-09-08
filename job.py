#!/usr/bin/python
#coding:utf-8


#相关联的数据库用户和表结构
'''CREATE USER 'nbu'@'ZJHZ-CMREAD-BACKUP02-MGT-SD' IDENTIFIED BY 'nbu';
grant all privileges on nbu.* to 'nbu'@'ZJHZ-CMREAD-BACKUP02-MGT-SD' identified by 'nbu';

CREATE TABLE IF NOT EXISTS `job`(
   `md_time` datetime NOT NULL,    
   `Job_ID` varchar(10) NOT NULL,
   `Job_type` VARCHAR(100) NOT NULL,
   `Job_State` VARCHAR(40) NOT NULL,
   `Job_Policy` VARCHAR(40) NOT NULL,
   `Job_Status` VARCHAR(100) NOT NULL,
   `Job_Schedule` VARCHAR(40) NOT NULL,
   `Job_Client` VARCHAR(40) NOT NULL,
   `Policy_type` VARCHAR(40) NOT NULL,
   `Job_Started` datetime,
   `Job_Elapsed` VARCHAR(40) NOT NULL,
   `Job_End` datetime 
)'''

'''从NBU侧获取相关的备份作业信息'''

import time
import os
from datetime import datetime, timedelta



def get_jobinfo(db):

	d = datetime.now()
	start_time = (d + timedelta(days=-0.5)).strftime("%m/%d/%Y %H:%M:%S") 
	
	#获取作业的信息输入一个启始时间，防止入库的时候重复作业条数太多
	cmd1 = 'bpdbjobs -most_columns -t %s ' %start_time
	bpdbjobs = os.popen(cmd1).readlines()
	md_time = time.strftime('%Y-%m-%d %H:%M:%S',time.localtime(time.time()))
	
	for job in bpdbjobs:
		tmp_dict = {}
		one_job_info = job.strip().split(',')
		tmp_dict['md_time'] = md_time
		tmp_dict['Job_ID'] = one_job_info[0]
		tmp_dict['Job_type'] = one_job_info[1]
		tmp_dict['Job_State'] = one_job_info[2]
		tmp_dict['Job_Status'] = one_job_info[3]
		tmp_dict['Job_Policy'] = one_job_info[4]
		tmp_dict['Job_Schedule'] = one_job_info[5]
		tmp_dict['Job_Client'] = one_job_info[6]
		tmp_dict['Policy_type'] = one_job_info[21]
		
		#时间需要做格式化转换，默认采集到的时间是1535476330这种格式
		tmp_dict['Job_Started'] = time.strftime('%Y-%m-%d %H:%M:%S',time.localtime(float(one_job_info[8])))
		
		#持续的时间使用秒为单位
		tmp_dict['Job_Elapsed'] = one_job_info[9]
		
		tmp_dict['Job_End'] = time.strftime('%Y-%m-%d %H:%M:%S',time.localtime(float(one_job_info[10])))
		
		result = db.select("job",'Job_ID','Job_ID = %s' %tmp_dict['Job_ID'])
	
		if result:
			db.update("job",tmp_dict,'Job_ID = %s' %tmp_dict['Job_ID'])
		else:
			db.insert("job",tmp_dict)
	
	db.close()