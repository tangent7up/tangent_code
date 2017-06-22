#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os
import sys
import time
from __future__ import division
from math import exp
import numpy as np
import scipy as sp
from datetime import datetime
#np.random.normal(size=(10, 10))
import leveldb

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


def getWeight(time):
    if time<86400:
        weight=10.0
    elif time<132800:
        weight=6.0
    elif time<259200:
        weight=4.0
    elif time<43200:
        weight=3.0
    elif time<604800:
        weight=2.0
    else: weight=1.0
    return weight


def readClickFiles(clickdirs,header=None):
    """
    return a set contain clicks
    """
    clicks={}
    for dirpath in clickdirs:
        for filepath in getFiles(dirpath):
            try:
                file=open(filepath).readlines()
                file=[line.strip().split(",") for line in file]
                click_dict=dict([(line[0],int(line[1])) for line in file if len(line)==2])
                click_add=dict([(line[0],1) for line in file if len(line)==1])
                clicks.update(click_add)
                clicks.update(click_dict)
            except Exception as e:
                continue
    return clicks


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
        elif c[0].strip() == "special":
            special = eval("'" + c[1].strip() + "'")
    return mode,train_featuredata_path,train_clickdata_path,test_featuredata_path,test_clickdata_path,key,header,schema,output,special



def stocGradAscent(data,v, w, w0,learning_rate):
    dataMatrix=data[:-1]
    classLable=data[-1]
    length = len(dataMatrix)
    inter1=sum(dataMatrix*dataMatrix*(v*v).sum(axis=1))
    inter2=(dataMatrix.reshape((length,1))*dataMatrix*v.dot(v.transpose())).sum()
    interaction = (inter2-inter1)/2.
    p = w0 + (dataMatrix*w).sum() + interaction
    w0+=learning_rate*(-classLable*(1-p)+(1-classLable)*p)
    w+=learning_rate*dataMatrix*(-classLable*(1-p)+(1-classLable)*p)
    for i in xrange(length):
        v[i]+=learning_rate*(-classLable*(1-p)+(1-classLable)*p)*((v - v[i]) * dataMatrix.reshape((length, 1)) * dataMatrix[i]).sum(axis=0)
        #(np.tile(v, (length, 1, 1))-v) Memory,error








#  with open("./output/part-00000") as f: model = eval(f.readlines()[0])
# clicks.get(line[head.index(key)],0)
# sum([len(i[1]) for i in columns_values.items()])
def newTrain(train_featuredata_path,train_clickdata_path,batch=1000,newtrain="no"):
    k=10
    #扫一遍盘获得每个字段包含哪些值，注意好顺序。
    if newtrain == "no":
        with open("columns_values","r") as f: columns_values = eval(f.readlines()[0])
    else:
        columns_values={}
    #vdb = leveldb.LevelDB(os.path.join(output, "v_database"))
    for filedir in train_featuredata_path:
        for filepath in getFiles(filedir):
            read = open(filepath)
            if header.lower() == "yes":
                head = read.readline().strip().split(",")
            else:
                head = schema_str.split(",")
            supposed_len = len(head)
            for line in read.readlines():
                try:
                    line=line.strip().split(",")
                    if len(line)!=supposed_len:
                        #error_log.append([filepath]+line)
                        continue
                    for i in xrange(1,supposed_len):
                        if head[i]==key:
                            continue
                        if head[i] in special:
                            if line[i]=="":
                                continue
                            words=set(line[i].split("|"))
                            oldvalue = columns_values.get(head[i], set())
                            oldvalue.union(words)
                            columns_values[head[i]]=oldvalue

                        else:
                            oldvalue=columns_values.get(head[i],set())
                            oldvalue.add(line[i])
                            columns_values[head[i]]=oldvalue
                except Exception as e:
                    print(e)
    with open("columns_values","w") as f:
        f.write(str(columns_values))
    clicks = readClickFiles(train_clickdata_path)
    # 部分v或全部v初始化
    if newtrain == "no":
        with open("parameter_values","r") as f:
            parameters=eval(f.readlines()[0])
    else:
        parameters={}
        parameters["v"]={}
        parameters["w"] = {}
        parameters["w0"] = 0.01*np.random.normal()
    for k_v in [i[0]+"&&"+j for i in columns_values.iteritems() for j in i[1]]:
        if k_v not in parameters["w"].keys():
            parameters["v"][k_v]=np.random.normal(size=k)*0.01
            parameters["w"][k_v]=np.random.normal()*0.01

    #生成onehot双向词典
    key2num={}
    num2key={}
    for i,k_v in enumerate(parameters["w"].keys()):
        key2num[k_v]=i
        num2key[i]=k_v
    onehot_len=len(parameters["w"].keys())

    learning_rate = 0.0001
    w0=parameters["w0"]
    w=np.array([i[1] for i in sorted(parameters["w"].iteritems(),key= lambda x : key2num[x[0]])])
    v=np.array([i[1] for i in sorted(parameters["v"].iteritems(),key= lambda x : key2num[x[0]])])

    for filedir in train_featuredata_path:
        for filepath in getFiles(filedir):
            read = open(filepath)
            if header.lower() == "yes":
                head = read.readline().strip().split(",")
            else:
                head = schema_str.split(",")
            supposed_len = len(head)
            for line in read.readlines():
                try:
                    line=line.strip().split(",")
                    if count%100000==0:
                        print("training: "+ str(count)+" lines finished")
                    count+=1
                    if len(line)!=supposed_len:
                        continue
                    data = [0] * onehot_len
                    for i in xrange(supposed_len):
                        if head[i]==key:
                            continue
                        if head[i] in special:
                            if line[i]=="":
                                continue
                            for word in line[i].split("|"):
                                data[key2num[head[i]+"&&"+word]]=1
                        else:
                            data[key2num[head[i]+"&&"+line[i]]]=1
                    data.append(clicks.get(line[head.index(key)],0))
                    data=np.array(data)
                    v, w, w0 = stocGradAscent(data, v,w,w0, learning_rate)
                except Exception as e:
                    print(e)



if __name__ == '__main__':
    mode, train_featuredata_path, train_clickdata_path, test_featuredata_path, test_clickdata_path, key, header, schema_str, output,special_str=getConf()
    special=special_str.split(",")
    error_log = []
    moreTrain(train_featuredata_path, train_clickdata_path)