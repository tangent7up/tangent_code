#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os
import leveldb
import gzip

def getWeight(time):
    if time<86400:
        weight=10
    elif time<132800:
        weight=6
    elif time<259200:
        weight=4
    elif time<43200:
        weight=3
    elif time<604800:
        weight=2
    else: weight=1
    return weight

def readClickFiles(clickdirs,header=None):
    """
    return a set contain clicks
    """
    clicks={}
    for dirpath in clickdirs:
        for filepath in getFiles(dirpath,startwith="cov"):
            try:
                #file=open(filepath).readlines()   ##
                file = gzip.open(filepath).readlines()
                file=[line.strip().split() for line in file]
                click_dict=dict([(line[0],getWeight(int(line[1]))) for line in file if len(line)==2])
                click_add=dict([(line[0],1) for line in file if len(line)==1])
                clicks.update(click_add)
                clicks.update(click_dict)
            except Exception as e:
                continue
    return clicks

def getFiles(path,contain="",startwith=""):
    filepaths=[]
    files = os.listdir(path)
    for f in files:
        if(os.path.isdir(os.path.join(path,f))):
            filepaths+=getFiles(os.path.join(path,f),contain,startwith)
        elif(f.startswith(".") or contain not in f or not f.startswith(startwith)):
            continue
        else:
            filepaths.append(os.path.join(path,f))
    return filepaths

def getConf(path="conf.txt"):
    conf=open("conf.txt").readlines()
    conf=map(lambda line: line.strip(),conf)
    conf=filter(lambda line: not line.startswith("#") and not line.startswith("/") and "=" in line,conf)
    conf=map(lambda line: line.split("="),conf)
    for c in conf:
        if c[0].strip()=="mode":
            mode=eval("'" + c[1].strip() + "'")
        elif c[0].strip()=="train_featuredata_path":
            train_featuredata_path=eval("['" + "','".join(map(lambda x: x.strip(), c[1].split(","))) + "']")
        elif c[0].strip()=="train_clickdata_path":
            train_clickdata_path=eval("['" + "','".join(map(lambda x: x.strip(), c[1].split(","))) + "']")
        elif c[0].strip()=="test_featuredata_path":
            test_featuredata_path=eval("['" + "','".join(map(lambda x: x.strip(), c[1].split(","))) + "']")
        elif c[0].strip()=="test_clickdata_path":
            test_clickdata_path=eval("['" + "','".join(map(lambda x: x.strip(), c[1].split(","))) + "']")
        elif c[0].strip()=="key":
            key=eval("'" + c[1].strip() + "'")
        elif c[0].strip()=="header":
            header=eval("'" + c[1].strip() + "'")
        elif c[0].strip()=="schema":
            schema=eval("'" + c[1].strip() + "'")
        elif c[0].strip() == "output":
            output = eval("'" + c[1].strip() + "'")
    return mode,train_featuredata_path,train_clickdata_path,test_featuredata_path,test_clickdata_path,key,header,schema,output

def loadLevelDB(db):
    info_dict = dict()
    for each_item in db.RangeIter():
        tuple=each_item[1].split(",")
        info_dict[each_item[0]] = [int(tuple[0]),int(tuple[1])]
    return info_dict

def customString(line,head,custom_f):
    string = line[head.index(custom_f[0])]
    for c in custom_f[1:]:
        string = string + "," + line[head.index(c)]
    return string

def predict(line):
    sample = 0
    value_sum = 0
    for cf in custom_f:
        for af in advert_f:
            for cv in line[head.index(cf)].split("|"):
                for av in line[head.index(af)].split("|"):
                    old_value = info_dict.get(cf + "&&" + af + "&&" + cv + "&&" + av, None)
                    if old_value != None:
                        sample += 1
                        value_sum += float(old_value[0]) / old_value[1]
    return value_sum / sample

def test1(files):
    count = 0
    collect_part = set()
    for filepath in files:
        read = gzip.open(filepath) ##
        #read = open(filepath)
        if header.lower() == "yes":
            head = read.readline().strip().split(",")
        else:
            head = schema_str.split(",")
        supposed_len = len(head)
        for line in read.readlines():
            try:
                line = line.strip().split(",")
                count += 1
                if count % 100000 == 0:
                    print("testing: " + str(count) + " lines finished")
                if len(line) != supposed_len:
                    # error_log.append([filepath]+line)
                    continue
                if line[head.index(key)] not in clicks_values:
                    continue
                print line[head.index(key)]
                collect_part.add(customString(line,head,custom_f))
            except Exception as e:
                print(e)
        read.close()
    return collect_part

def test2(files):
    count=0
    collect_dict_part={}
    for filepath in files:
        read = gzip.open(filepath)  ##
        #read = open(filepath)
        if header.lower() == "yes":
            head = read.readline().strip().split(",")
        else:
            head = schema_str.split(",")
        supposed_len = len(head)
        for line in read.readlines():
            try:
                line = line.strip().split(",")
                count += 1
                if count % 100000 == 0:
                    print("testing: " + str(count) + " lines finished")
                if len(line) != supposed_len:
                    # error_log.append([filepath]+line)
                    continue
                cus_str=customString(line,head,custom_f)
                if cus_str not in collect:
                    continue
                oldv=collect_dict_part.get(cus_str,[])
                oldv.append([clicks.get(line[head.index(key)],0), predict(line),customString(line,head,advert_f)])
                collect_dict_part[cus_str]=oldv
            except Exception as e:
                print(e)
        read.close()
    return collect_dict_part


if __name__ == '__main__':
    import multiprocessing
    import json
    custom_f="adx,ssp,sid,hod,dow,pub_cat1,pub_cat2,s_bundle,yob,gender,conn_type,make,model,os,osv".split(",")
    advert_f="ad_cat1,ad_cat2,ad_bundle,marketing_sdk".split(",")
    mode, train_featuredata_path, train_clickdata_path, test_featuredata_path, test_clickdata_path, key, header, schema_str, output=getConf()
    clicks = readClickFiles(train_clickdata_path)
    #db = leveldb.LevelDB(os.path.join(output,"query_database"))

    clicks_values=set(clicks.keys())



    #info_dict = loadLevelDB(db)
    fp = open(os.path.join(output, 'json.txt'), 'r')
    info_dict = eval(fp.read())
    fp.close()





    allfiles = []
    for filedir in train_featuredata_path:
        for filepath in getFiles(filedir, startwith="sum"):
            allfiles.append(filepath)
    f_num = len(allfiles)
    process_num = 15 ##
    it=[]
    for i in xrange(process_num):
        it.append(allfiles[int(round(f_num*i/process_num)):int(round(f_num*(i+1)/process_num))])

    print("begin test1: searching transformed custums")
    collect = set()
    pool=multiprocessing.Pool(process_num)
    rl=pool.map(test1,it)
    pool.close()
    pool.join()
    for collect_part in rl:
        collect=collect.union(collect_part)


    print("begin test2: ranking")
    collect_dict={}
    pool=multiprocessing.Pool(process_num)
    rl2=pool.map(test2,it)
    pool.close()
    pool.join()
    for collect_dict_part in rl2:
        for k in collect_dict_part.keys():
            oldv=collect_dict.get(k,[])
            oldv=oldv+collect_dict_part[k]
            collect_dict[k]=oldv

    rank=[]
    for it in collect_dict.iteritems():
        it_contain=sorted(it[1],key=lambda x:x[1],reverse=True)
        it_click=[i[0] for i in it_contain]
        for i in range(len(it_click)):
            if it_click[i]>0:
                rank.append([i,len(it_contain)])

    part=[0]*20
    for i in rank:
        part[int(float(i[0])/float(i[1])*20)]+=1
    s=float(sum(part))

    for i in range(len(part)):
        print(5+i*5, "%: ",part[i]/s)


    with open("rank.txt","w") as t:
        t.write(str(rank))
    with open("part.txt","w") as t:
        t.write(str(part))
    # with open("test.txt") as t:
    #     rank=eval(t.read())
