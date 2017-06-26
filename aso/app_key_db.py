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