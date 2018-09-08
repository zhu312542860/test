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


def get_policyinfo(db):


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