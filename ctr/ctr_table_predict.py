#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os


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


def predict(data_string):
    example=data_string.split(",")
    sample = 0
    value_sum = 0
    for cf in custom_f:
        for af in advert_f:
            for cv in example[head.index(cf)].split("|"):
                for av in example[head.index(af)].split("|"):
                    old_value = info_dict.get(cf + "&&" + af + "&&" + cv + "&&" + av, None)
                    if old_value != None:
                        sample += 1
                        value_sum += float(old_value[0]) / old_value[1]
    return value_sum / sample


if __name__ == '__main__':
    custom_f="adx,ssp,sid,hod,dow,pub_cat1,pub_cat2,s_bundle,yob,gender,conn_type,make,model,os,osv".split(",")
    advert_f="ad_cat1,ad_cat2,ad_bundle,marketing_sdk".split(",")
    mode, train_featuredata_path, train_clickdata_path, test_featuredata_path, test_clickdata_path, key, header, schema_str, output=getConf()
    head = schema_str.split(",")
    info_dict=eval(open(os.path.join(output,'json.txt'),'r').read())
    data_string="8bc3d79b-bc13-4677-bdeb-f1d551f89345,s,xx,130048848,3,5,,,com.droidbender.dramania,0,,0,Samsung,SM-A500F,android,5.0,ID,unknown,,,unknown,"
    print predict(data_string)
    data= ["8bc3d79b-bc13-4677-bdeb-f1d551f89345,s,xx,130048848,3,5,,,com.droidbender.dramania,0,,0,Samsung,SM-A500F,android,5.0,ID,unknown,,,unknown,",
           "8bc3d79b-bc13-4677-bdeb-f1d551f89345,s,xx,130048848,3,5,,,com.droidbender.dramania,0,,0,Samsung,SM-A500F,android,5.0,ID,unknown,,,unknown,",
           "8bc3d79b-bc13-4677-bdeb-f1d551f89345,s,xx,130048848,3,5,,,com.droidbender.dramania,0,,0,Samsung,SM-A500F,android,5.0,ID,unknown,,,unknown,"]
    predictlist=map(predict,data)
    print predictlist
