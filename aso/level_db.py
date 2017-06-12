#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
功能：处理数据库交互
版本：
（1）2017-05-25 初版，处理keyerror问题并提供排序问题
（2）2017-05-25 归一合并相似度与搜索指数结果，并且按0-100来进行打分
（3）2017-05-26 作为的flask后台数据交互文件，响应前端需求

"""

import sys
import leveldb
# import logging
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

def search(db, key):
    """查询数据"""
    value = db.Get(key)
    return value


# #################
# （2）相似度计算
# #################
def cos(input_app, keyword_app ,input_app_norm, keyword_app_norm):
    """
    计算向量的余弦相似度
    """
    dot_product = 0.0 
    # 对两个关键词的APP剔重并编号
    for key in input_app.keys():
        if key in keyword_app:
            dot_product += input_app[key]*keyword_app[key]
    if input_app_norm == 0.0 or keyword_app_norm==0.0:  
        return 0.00  
    else:
        return dot_product / (input_app_norm*keyword_app_norm)

def getKw2hint(db):
    """一次性加载搜索热度词数据，减少与leveldb的交互"""
    kw2hint_dict = dict()
    for each_item in db.RangeIter():
        kw2hint_dict[each_item[0]] = int(each_item[1])    
    return kw2hint_dict

# #################
# （3）查询接口——语义拓展
# #################
def getSimWord(input_word, weight=0.5, top_num=200, max_can_num=2000, min_prity=4605):
    """
    语义拓词
    top_num：打印相似度TOP数目
    max_can_num：候选关键词TOP数目
    min_prity：搜索指数筛选值
    """
    db_kw2kwid = initDB('kw2kwid')
    db_kwid2kw = initDB('kwid2kw')
    db_kw2app = initDB('kw2app')
    db_kw2norm = initDB('kw2norm')
    db_app2kw = initDB('app2kw')
    db_kw2hint = initDB('kw2hint')
    try:
        # keyword-->keyword_id
        input_kw_id = search(db_kw2kwid, str(input_word.strip()))
        # keyword_id-->app_dict
        input_app_dict = eval(search(db_kw2app, input_kw_id))
        input_app_norm = float(search(db_kw2norm, input_kw_id))
    except Exception as er:
        return None
    # app_dict-->keyword_id_set
    kwid_dict = dict()
    prioity_dict = dict()
    kw2hint_dict = getKw2hint(db_kw2hint)
    for appid in input_app_dict.keys():
        tmp_list = search(db_app2kw, appid).split('|')
        for each_kw in tmp_list:
            if each_kw in kwid_dict:
                kwid_dict[each_kw] += 1
            else:
                try:
                    prioity = kw2hint_dict[each_kw]
                    if prioity >= min_prity:
                        prioity_dict[each_kw] = str(prioity)
                        kwid_dict[each_kw] = 1
                except Exception as e:
                    continue
    # 对搜索指数进行归一化操作
    if len(kwid_dict) > max_can_num:
        sorted_kwid = sorted(kwid_dict.iteritems(), key = lambda item: item[1], reverse=True)
        kwid_dict = [sorted_kwid[i][0] for i in xrange(max_can_num)]
        prity_can_dict = dict([(i, int(prioity_dict[i])) for i in kwid_dict])
        # keyword_id_set-->app_dict
        sim_dict = dict()
        max_v = max(prity_can_dict.values())
        min_v = min_prity
        for key in prity_can_dict:
            nor_v = (float(prity_can_dict[key]-min_v)/float(max_v-min_v))*(1-weight)
            prity_can_dict[key] = str(prity_can_dict[key]) + '|' + str(nor_v)
        for each_kw_id in kwid_dict:
            if each_kw_id == input_kw_id:
                continue 
            else:
                try:
                    sim_app_dict = eval(search(db_kw2app, each_kw_id))
                    sim_app_norm = float(search(db_kw2norm, each_kw_id))
                    sim_word = search(db_kwid2kw, each_kw_id) 
                    # 归一化cos值与搜索指数，并加权得到最终得分
                    cos_v = cos(input_app_dict, sim_app_dict ,input_app_norm, sim_app_norm)
                    nor_cos_v = cos_v*weight
                    prity = prity_can_dict[each_kw_id].split('|')[0]
                    nor_prity = float(prity_can_dict[each_kw_id].split('|')[1])
                    score = (nor_prity + nor_cos_v)*100
                    sim_dict[sim_word] = str(cos_v) + '|' + prity + '|' + str(round(score,1))
                except Exception as er:
                    continue    
    sim_dict = sorted(sim_dict.iteritems(), key = lambda item: float(item[1].split('|')[2]), reverse=True)
    count = 0
    num = min(top_num, len(sim_dict))
    info_list = list()
    for i in range(num):
        each_sim_word = sim_dict[i]
        count += 1
        v = each_sim_word[1].split('|')
        info_list.append([str(count), each_sim_word[0], str(round(float(v[0]),2)), str(v[1]), v[2]])
    return info_list

# #################
# （4）查询接口——词汇拓展
# #################

def getReWord(input_word, weight=0.5, top_num=200, is_cut=False ,min_prity=4605):
    """
    文字拓词
    """
    db_kw2kwid = initDB('kw2kwid')
    db_kwid2kw = initDB('kwid2kw')
    db_kw2app = initDB('kw2app')
    db_kw2norm = initDB('kw2norm')
    db_app2kw = initDB('app2kw')
    db_kw2hint = initDB('kw2hint')
    kw_set = set()
    if is_cut:
        for each_cut_word in jieba.cut(input_word):
            for each_word in open('level_db/keyword.txt', 'r'):
                if each_cut_word in each_word:
                    kw_set.add(each_word.strip('\r\n'))
    else:
        for each_word in open('level_db/keyword.txt', 'r'):
            if input_word in each_word:
                kw_set.add(each_word.strip('\r\n'))
    # 搜索结果为空
    if len(kw_set) == 0:
        # 在关键词库中找不到关键词，搜索无意义
        return None
    else:
        try: 
            # keyword-->keyword_id
            input_kw_id = search(db_kw2kwid, str(input_word.strip()))
            input_app_dict = eval(search(db_kw2app, input_kw_id))
            input_app_norm = float(search(db_kw2norm, input_kw_id))
            # keyword_id-->app_dict
        except Exception as er:
            return None
        kwid_set = set()
        prity_can_dict = dict()
        kw2hint_dict = getKw2hint(db_kw2hint)
        for each_kw in kw_set:
            try:
                kw_id = search(db_kw2kwid, str(each_kw.strip()))
                prioity = kw2hint_dict[kw_id]
                if prioity >= min_prity:
                    prity_can_dict[kw_id] = prioity
                    kwid_set.add(kw_id)
            except Exception as e:
                continue

        sim_dict = dict()
        max_v = max(prity_can_dict.values())
        min_v = min_prity
        for key in prity_can_dict:
            nor_v = (float(prity_can_dict[key]-min_v)/float(max_v-min_v))*(1-weight)
            prity_can_dict[key] = str(prity_can_dict[key]) + '|' + str(nor_v)
        for each_kw_id in kwid_set:
            if each_kw_id == input_kw_id:
                continue 
            else:
                try:
                    sim_app_dict = eval(search(db_kw2app, each_kw_id))
                    sim_app_norm = float(search(db_kw2norm, each_kw_id))
                    sim_word = search(db_kwid2kw, each_kw_id) 
                    cos_v = cos(input_app_dict, sim_app_dict ,input_app_norm, sim_app_norm)
                    nor_cos_v = cos_v*weight
                    prity = prity_can_dict[each_kw_id].split('|')[0]
                    nor_prity = float(prity_can_dict[each_kw_id].split('|')[1])
                    score = (nor_prity + nor_cos_v)*100
                    sim_dict[sim_word] = str(cos_v) + '|' + prity + '|' + str(round(score,1))
                except Exception as er:
                    continue        
        sim_dict = sorted(sim_dict.iteritems(), key = lambda item: float(item[1].split('|')[2]), reverse=True)
        count = 0
        num = min(top_num, len(sim_dict))
        info_list = list()
        for i in range(num):
            each_sim_word = sim_dict[i]
            count += 1
            v = each_sim_word[1].split('|')
            info_list.append([str(count), each_sim_word[0], str(round(float(v[0]),2)), str(v[1]), v[2]])
    return info_list

if __name__ == '__main__':
    pass
    # input_word = '京东'
    # print getSimWord(input_word)
    
    # print getReWord(input_word)
