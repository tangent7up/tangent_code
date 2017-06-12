#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
版本：
（1）2017-05-22 处理searches数据库
（2）2017-05-23 整合所有数据库的入库
功能：ASO拓词
数据源：searches数据
数据周期：一周kw
数据库样式：
A. app2kw/: (AppID对应关键词ID)【样式: appid_1 --> (kw_1|kw_2|...|kw_N)】
B. kw2app/: (关键词ID对应APPID)【样式: kwid --> {appid_1:int(count),appid_2:count,...,appid_N:count}】
C. kw2hint/: (关键词ID对应搜索指数)【样式: kwid-->int(prioity)】
D. kw2kwid/: (关键词对应其ID)【样式: kw --> kwid】
E. kw2norm/: (关键词对应其范数)【样式: kwid --> float(所有的count的平方和开方)】
F. kwid2kw（关键词ID对应其实际中文)【样式: kwid --> kw】
"""

import os
import sys
import time
import leveldb
import csv
import traceback
import math
import codecs


reload(sys)
sys.setdefaultencoding('utf8')

# #################
# （1）levelD相关操作
# #################

def initDB(db_name):
    """初始化数据库"""
    db_path = 'level_db/'
    db = leveldb.LevelDB(db_path+db_name)
    return db

def display(db, max_num=10):
    """展示数据"""
    # count = 0
    if max_num > len(list(db.RangeIter())):
    	print '展示记录数超上限！'
    else:
    	for each_item in list(db.RangeIter())[:max_num]:
    		print each_item

def insert(db, key, value):
    """插入数据"""
    db.Put(key, value)

def update(db, key, value):
    """更新数据"""
    db.Put(key, value)

def delete(db, key):
    """删除数据"""
    db.Delete(key)

def search(db, key):
    """查询数据"""
    value = db.Get(key)
    return value

def multiInsert(db, info_dict, each_num=100000): 
    """批量插入
    each_num：每次插入数量"""
    count = 0
    batch = leveldb.WriteBatch() 
    for each_tu in info_dict.iteritems():
    	count += 1
        batch.Put(each_tu[0], str(each_tu[1]))
        if count == each_num:
        	db.Write(batch, sync = True)
        	batch = leveldb.WriteBatch()
        	count = 0
    if count != 0:
    	db.Write(batch, sync = True)

# #################
# （2）文件处理与入库
# #################
def dealK2aStr(top_num=200, mod=0):
	"""
	基于keyword对searches文件横向遍历归并操作
	==>
	kw-->all_app: {kw1:'appid1|appid2|...', kw2:'', ...}
	top_num：默认取搜索后排名前200的APP；mod：分批处理，防止内存溢出
	"""
	k2astr_dict = dict()
	count = 0
	for each_line in csv.reader(file('../data_source/searches.csv','rb')):
		assert(len(each_line) == 2)
		kw = each_line[0]
		if int(kw) % 5 != mod:
			continue
		applist = each_line[1].split(',')
		applist[0] = applist[0][1:]
		applist[-1] = applist[-1][:-1]
		applen = len(applist)
		if k2astr_dict.get(each_line[0]) is None:
			if applen > top_num:
				k2astr_dict[each_line[0]] = '|'.join(applist[:top_num])
			else:
				k2astr_dict[each_line[0]] = '|'.join(applist)
		else:
		    if applen > top_num:
				k2astr_dict[each_line[0]] += '|' + '|'.join(applist[:top_num])
		    else:
				k2astr_dict[each_line[0]] += '|' + '|'.join(applist)
		count += 1
		if count % 100000 == 0:
			print count, time.strftime('%Y-%m-%d %H:%M:%S',time.localtime(time.time()))
	return k2astr_dict

def dealKw2an(k2astr_dict):
	"""
	k2astr_dict：dealKw2app()返回的结果{kw1:'appid1|appid2|...', kw2:'', ...}
	==>
	1,kwid-->appid: {kwid_1:str({appid_1:count,appid_2:count,...,appid_N:count}), kw_2:str({}), ...}
	2,kwid-->||*||: {kwid_1:所有的count的平方和开方, kw_2:, ...}
	"""
	kw2app_dict = dict()
	kw2norm_dict = dict()
	for k,v in k2astr_dict.iteritems():	
		app_dict = dict()
		for each_app in v.split('|'):
			old_value = app_dict.setdefault(each_app, 0)
			app_dict[each_app] = old_value + 1
		kw2app_dict[k] = str(app_dict)
		kw2norm_dict[k] = str(math.sqrt(sum([i*i for i in app_dict.values()])))
	return (kw2app_dict, kw2norm_dict)

def dealApp2kw(top_num=200, mod=0):
	"""
	纵向遍历searches表
	==>
	appid-->kw_list: {appid_1:str(kw1|kw2|...),appid_2:str}
	top_num：默认取搜索后排名前200的APP；mod：分批处理，防止内存溢出
	"""
	app2kw_dict = dict()
	count = 0
	for each_line in csv.reader(file('../data_source/searches.csv','rb')):
		assert(len(each_line) == 2)
		kw = each_line[0]
		applist = each_line[1].split(',')
		applist[0] = applist[0][1:]
		applist[-1] = applist[-1][:-1]
		applen = len(applist)
		num = min(top_num, applen)
		for i in xrange(num):
			app = applist[i]
			if app != '' and int(app) % 2 == mod:
				if app in app2kw_dict:
					ov = app2kw_dict[app]
					app2kw_dict[app] = ov + '|' + kw
				else:
					app2kw_dict[app] = kw
		count += 1
		if count % 100000 == 0:
			print count, time.strftime('%Y-%m-%d %H:%M:%S',time.localtime(time.time()))
	for eachapp in app2kw_dict:
		ov = app2kw_dict[eachapp]
		nv = '|'.join(set(ov.split('|')))
		app2kw_dict[eachapp] = nv
	return app2kw_dict

def dealkwid2kw():
	"""
	转换本地keyword文件
	==>
	kwid-->kwname: {kwid1:kwname1,kwid2:kwname2}
	"""
	kw_dict = dict()
	for each_line in csv.reader(file('../data_source/keywords.csv', 'rb')):
		kw_dict[each_line[0]] = each_line[1]
	multiInsert(db_kwid2kw, kw_dict, each_num=100000)
	# display(db_kw, max_num=1)

def dealkw2kwid():
	"""
	转换本地keyword文件
	==>
	kwname-->kwid: {kwname1:kwid1,kwname2:kwid2}
	"""
	kw_dict = dict()
	for each_line in csv.reader(file('../data_source/keywords.csv', 'rb')):
		kw_dict[each_line[1]] = each_line[0]
	multiInsert(db_kw2kwid, kw_dict, each_num=100000)

def dealKw2Hint():
	"""
	处理关键词搜索热度
	"""
	k2hstr_dict = dict()
	kw2hint_dict = dict()
	count = 0
	for each_line in csv.reader(file('/../data_source/hints.csv','rb')):
		assert(len(each_line) == 2)
		kw = each_line[0]
		hint = each_line[1]
		if k2hstr_dict.get(kw) is None:
			k2hstr_dict[kw] = hint
		else:
			k2hstr_dict[kw] = k2hstr_dict[kw] + ('|'+hint)
		count += 1
		if count % 100000 == 0:
			print count, time.strftime('%Y-%m-%d %H:%M:%S',time.localtime(time.time()))
	for each_item in k2hstr_dict.iteritems():
		hint_list = map(eval, each_item[1].split('|'))
		kw2hint_dict[each_item[0]] = sum(hint_list)/len(hint_list)
	return kw2hint_dict	

def db_main():
    """
    处理数据分批入库
    注意：数据量较大，慎用！！
    """
    st = time.time()
    print '[Start]: 正在处理[keyword-->app]和[keyword-->norm]...'
    for k in range(0, 5):
        print '--正在遍历第 %s 次[searches.csv]--'%str(i+1)
        k2astr_dict = dealK2aStr(200, k)
        print 'Message: 已完成reduce by keyword! '
        kw2app_dict, kw2norm_dict = dealKw2an(k2astr_dict)
        print 'Message: 已完成reduce by appid! '
        multiInsert(db_kw2app, kw2app_dict, each_num=100000)
        kw2app_dict = None
        multiInsert(db_kw2norm, kw2norm_dict, each_num=100000)
        print 'Message: 已完成数据导入leveldb! '
        kw2norm_dict = None
        k2astr_dict = None
    et1 = time.time()
    print '[End]: 处理[kw-->app]和[kw-->norm]，共计 %s 分钟'%((et1-st)/60)

    print '[Start]: 正在处理[app-->keyword]...'
    for i in range(0, 2):
        print '--正在遍历第 %s 次[searches.csv]--'%str(i+1)
        app2kw_dict = dealApp2kw(top_num=200, mod=i)
        print 'Message: 已完成reduce by appid! '
        multiInsert(db_app2kw, app2kw_dict, each_num=100000)
        print 'Message: 已完成数据导入leveldb! '
        app2kw_dict = None
    et2 = time.time()
    print '[End]: 处理[app-->keyword]，共计 %s 分钟'%((et2-et1)/60)

    print '[Start]: 正在处理[keyword_id-->keyword]和[keyword-->keyword_id]...'
    dealkwid2kw()
    print 'Message: 已完成[keyword_id-->keyword]处理! '
    dealkw2kwid()
    print 'Message: 已完成[keyword-->keyword_id]处理!'
    et3 = time.time()
    print '[End]: 处理[keyword_id-->keyword]和[keyword-->keyword_id]，共计 %s 分钟'%((et3-et2)/60)

    print '[Start]: 正在处理[keyword_id-->hint]...'
    kw2hint_dict = dealKw2Hint()
    multiInsert(db_kw2hint, kw2hint_dict, each_num=100000)
    print 'Message: 已完成[keyword_id-->hint]处理!'
    et4 = time.time()
    print '[End]: 处理[keyword_id-->hint]，共计 %s 分钟'%((et4-et3)/60)


if __name__ == '__main__':
	pass
	# db_main() #耗时较长，慎用

