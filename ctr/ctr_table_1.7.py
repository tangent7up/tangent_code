#!/usr/bin/env python
# -*- coding: utf-8 -*-
# version 1.7
import os
import leveldb
import gzip


def getFiles(path,contain="",startwith=""):
    filepaths=[]
    if (not os.path.isdir(path)):
        filepaths.append(path)
        return filepaths
    files = os.listdir(path)
    for f in files:
        if(os.path.isdir(os.path.join(path,f))):
            filepaths+=getFiles(os.path.join(path,f),contain,startwith)
        elif(f.startswith(".") or contain not in f or not f.startswith(startwith)):
            continue
        else:
            filepaths.append(os.path.join(path,f))
    return filepaths

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
                #file=gzip.open(filepath).readlines() ##
                file = open(filepath).readlines()
                file=[line.strip().split() for line in file]
                click_dict=dict([(line[0],getWeight(int(line[1]))) for line in file if len(line)==2])
                click_add=dict([(line[0],1) for line in file if len(line)==1])
                clicks.update(click_add)
                clicks.update(click_dict)
            except Exception as e:
                print(e)
    return clicks

def loadLevelDB(db):
    info_dict = dict()
    for each_item in db.RangeIter():
        tuple=each_item[1].split(",")
        info_dict[each_item[0]] = [int(tuple[0]),int(tuple[1])]
    return info_dict

def multiUpdate(db, info_dict_add, each_num=100000):
    """批量插入
    each_num：每次插入数量"""
    info_dict=loadLevelDB(db)
    for each_tu in info_dict_add.keys():
        value=info_dict.get(each_tu, [0, 0])
        add=info_dict_add[each_tu]
        value=[value[0]+add[0],value[1]+add[1]]
        info_dict[each_tu]=value
    count = 0
    batch = leveldb.WriteBatch()
    info_dict = dict(map(lambda x: (x[0], str(x[1][0]) + "," + str(x[1][1])), info_dict.iteritems()))
    for each_tu in info_dict.iteritems():
    	count += 1
        batch.Put(each_tu[0], str(each_tu[1]))
        if count == each_num:
        	db.Write(batch, sync = True)
        	batch = leveldb.WriteBatch()
        	count = 0
    if count != 0:
    	db.Write(batch, sync = True)



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



def moreTrain(filepaths):
    count = 0
    info_dict_add={}
    for filepath in filepaths:
        #read = gzip.open(filepath)  ##
        read = open(filepath)
        try:
            if header.lower() == "yes":
                head = read.readline().strip().split(",")
            else:
                head = schema_str.split(",")
            supposed_len = len(head)
        except Exception as e:
            print("some error happen to schema of: "+str(filepath))
            return {}
        for line in read.readlines():
            try:
                line=line.strip().split(",")
                if count%100000==0:
                    print("training: "+ str(count)+" lines finished")
                count+=1
                if len(line)!=supposed_len:
                    #error_log.append([filepath]+line)
                    continue
                for cf in custom_f:
                    for af in advert_f:
                        for cv in line[head.index(cf)].split("|"):
                            for av in line[head.index(af)].split("|"):
                                old_value = info_dict_add.get(cf+"&&"+af+"&&"+cv+"&&"+av, [0, 0])
                                info_dict_add[cf+"&&"+af+"&&"+cv+"&&"+av]=[old_value[0]+clicks.get(line[head.index(key)],0),old_value[1]+clicks.get(line[head.index(key)],1)]
            except Exception as e:
                print(e)
        read.close()
    return info_dict_add





if __name__ == '__main__':
    import multiprocessing
    import json
    custom_f="adx,ssp,sid,hod,dow,pub_cat1,pub_cat2,s_bundle,yob,gender,conn_type,make,model,os,osv".split(",")
    advert_f="ad_cat1,ad_cat2,ad_bundle,marketing_sdk".split(",")
    mode, train_featuredata_path, train_clickdata_path, test_featuredata_path, test_clickdata_path, key, header, schema_str, output=getConf()
    try:
        os.mkdir(output)
    except Exception as e:
        info_dict={}

    #error_log = []
        pass
    clicks = readClickFiles(train_clickdata_path)
    #db = leveldb.LevelDB(os.path.join(output,"query_database"))
    try:
        fp=open(os.path.join(output, 'json_full.txt'), 'r')
        info_dict=eval(fp.read())
        fp.close()
    except Exception as e:
        info_dict={}

    #error_log = []



    allfiles = []
    for filedir in train_featuredata_path:
        for filepath in getFiles(filedir, startwith="sum"):
            allfiles.append(filepath)
    f_num = len(allfiles)
    process_num = 15
    it=[]
    for i in xrange(process_num):
        it.append(allfiles[int(round(f_num*i/process_num)):int(round(f_num*(i+1)/process_num))])

    print("begin deal logs")
    pool=multiprocessing.Pool(process_num)
    rl=pool.map(moreTrain,it)
    pool.close()
    pool.join()


    print("combining dictionary")
    for info_dict_add in rl:
        for k in info_dict_add.keys():
            v=info_dict.get(k, [0,0])
            info_dict[k] = [v[0] + info_dict_add[k][0],v[1] + info_dict_add[k][1]]
    #multiUpdate(db, info_dict_addsum)

    print("begin writing json")
    #info_dict=loadLevelDB(info_dict_addsum)
    fp=open(os.path.join(output,'json_full.txt'),'w')
    info_json=json.dump(info_dict,fp)
    fp.close()

    new_info_dict = dict([i for i in info_dict.iteritems() if i[1][0] != 0])
    new_info_dict = dict([(i[0],float(i[1][0])/float(i[1][1])) for i in new_info_dict.iteritems()])

    fp=open(os.path.join(output,'json.txt'),'w')
    info_json=json.dump(new_info_dict,fp)
    fp.close()

    # if mode.lower().strip() == "train_test":
    #     test(test_featuredata_path,test_clickdata_path)
    # elif mode.lower().strip()=="train_only":
    #     test(train_featuredata_path, train_clickdata_path)
    # else:
    #     pass
