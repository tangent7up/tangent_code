#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Jun  5 06:26:19 2017

@author: tangent
"""
import math
from pyspark.sql.session import SparkSession
from pyspark.sql import Row
from pyspark.sql.types import StructType,StringType,IntegerType
from pyspark.ml.linalg import Vectors
from pyspark.ml.classification import LogisticRegression
import json
import sys

def toSparse(l):
    d={}
    for i in range(len(l)):
        if l[i]!=0:
            d[i]=l[i]
    return d

def split(string,sign):
    if string is None:
        return []
    else:
        return string.split(sign)

def rowTransform(row):
    new_row=[]
    for column in full_columns+["target"]:
        if column in main_key:
            new_row.append(row[column])
        if column in quantity:
            new_row.append(row[column])
        if column in catagory:
            new_row+=oneHot(columns_dict[column].get(row[column],0),columns_dict_len[column])
        if column in quality:
            words=split(row[column],"|")
            index=[columns_dict[column].get(i,None) for i in words if columns_dict[column].get(i,None) is not None]
            new_row+=oneHot2(index,columns_dict_len[column])
        if column in target:
            new_row.append(row[column])
    return Row(label=new_row[-1], features=Vectors.sparse(len(new_row)-2 , toSparse(new_row[1:-1])))
        
def oneHot(index,length):
    l=[0]*length
    if index==0: pass
    else: l[index-1]=1
    return l
    
def oneHot2(index,length):
    l=[0]*length
    for i in index:
        try:
            l[i]=1
        except:
            pass
    return l

def spark_read(path,schema=None,header=False):
    df=spark.read.csv(path[0],schema=schema, header=header)
    for file in path[1:]:
        df_add=spark.read.csv(file,schema=schema, header=header)
        df=df.union(df_add)
    return df

def parse_feature_data1(onerow):
    result=[]
    for eachcol in full_columns:
        if eachcol in catagory:
            value=onerow[eachcol]
            if value != None:
                result.append(eachcol + '_:_' + value)
            else:
                result.append(eachcol + '_:__null_')
    return result

def parse_feature_data2(onerow):
    result=[]
    for eachcol in full_columns:
        if eachcol in quality:
            value=split(onerow[eachcol])
            if value != []:
                for v in value:
                    result.append(eachcol + '_:_' + v)
    return result

if __name__ == "__main__":
    assert(len(sys.argv)==2)

    spark = SparkSession\
        .builder\
        .appName("OneHotEncoderCtr")\
        .config("spark.some.config.option", "some-value") \
        .getOrCreate()
    sc = spark.sparkContext


    conf = sc.textFile(sys.argv[1]).filter(lambda x: not x.startswith("#")).filter(lambda x: "=" in x).map(
        lambda x: x.split("=")).collect()
    # conf = sc.textFile("./conf.txt").filter(lambda x: not x.startswith("#")).filter(lambda x: "=" in x).map(lambda x: x.split("=")).collect()
    for c in conf:
        if "path" in c[0]:
            exec(c[0].strip() + "=['" + "','".join(map(lambda x: x.strip(),c[1].split(","))) + "']")
        else:
            exec(c[0].strip() + "='" + c[1].strip() + "'")
    schema_str = schema
    del (schema)


    if header=="yes":
        df_feature_train = spark_read(train_featuredata_path, header=True)
        df_click_train = spark_read(train_clickdata_path)
        full_columns=df_feature_train.columns
    else:
        full_columns=schema_str.split(",")
        schema = StructType()
        for column in full_columns:
            schema=schema.add(column,StringType(),True)
        df_feature_train = spark_read(train_featuredata_path, schema=schema,header=False)
        df_click_train = spark_read(train_clickdata_path)

    check_bar=df_feature_train.rdd.map(lambda r: [int("|" in str(i)) for i in list(r)]).reduce(lambda ra,rb: [ra[i]+rb[i] for i in range(len(ra))])


    if mode == "train_test":
        if header == "yes":
            df_feature_test = spark_read(test_featuredata_path,header=True)
            df_click_test = spark_read(test_clickdata_path)
        else:
            df_feature_test = spark_read(test_featuredata_path,schema=schema,header=False)
            df_click_test = spark_read(test_clickdata_path)
        check_bar_test=df_feature_test.rdd.map(lambda r: [int("|" in str(i)) for i in list(r)]).reduce(lambda ra,rb: [ra[i]+rb[i] for i in range(len(ra))])
        check_bar=[check_bar[i] + check_bar_test[i] for i in range(len(check_bar))]


    main_key=[key]
    quality = []
    catagory = []
    quantity=[]
    for i in range(len(full_columns)):
        if check_bar[i]>0 and full_columns[i]!=key:
            quality.append(full_columns[i])
        elif check_bar[i]==0 and full_columns[i]!=key:
            catagory.append(full_columns[i])
    target = ["target"]


    tran_cus=set(df_click_train.rdd.map(lambda r: r["_c0"]).collect())
    tran_cus_b = sc.broadcast(tran_cus)
    schema=df_feature_train.schema
    new_schema=schema.add("target",IntegerType(),True)
    df_feature_train_ = df_feature_train.rdd.map(lambda r: list(r) +[int(r[key] in tran_cus_b.value)])
    df_train=spark.createDataFrame(df_feature_train_,schema=new_schema)
    if mode == "train_test":
        tran_cus = set(df_click_test.rdd.map(lambda r: r["_c0"]).collect())
        tran_cus_b = sc.broadcast(tran_cus)
        df_feature_test_ = df_feature_test.rdd.map(lambda r: list(r) + [int(r[key] in tran_cus_b.value)])
        df_test = spark.createDataFrame(df_feature_test_, schema=new_schema)


    #确定需要做Onehot操作的维度
    print("确定需要做Onehot操作的维度")
    new_columns=[key]
    columns_dict = {}
    distinct_words1=df_train.select(catagory).rdd.flatMap(parse_feature_data1).distinct().collect()
    distinct_words2=df_train.select(quality).rdd.flatMap(parse_feature_data2).distinct().collect()
    for column in catagory+quality:
        columns_dict[column]=[]
    for column in catagory:
        columns_dict[column] += [word.split("_:_")[1] for word in distinct_words1 if word.split("_:_")[0]==column]
    for column in quality:
        columns_dict[column] += [word.split("_:_")[1] for word in distinct_words2 if word.split("_:_")[0]==column]




    #生成onehot后新的列--------------
    print("生成onehot后新的列")
    columns_dict_len={}
    for column in catagory:
        unique=[i for i in list(set(columns_dict[column])) if i !=""]
        columns_dict[column]=dict(zip(unique,range(1,1+len(unique))))
        columns_dict_len[column]=len(unique)
        columns_dict[column][""]=0
        new_columns+=[column+"&&"+str(c) for c in unique]
    for column in quality:
        unique=[i for i in list(set(columns_dict[column])) if i!=""]
        columns_dict[column]=dict(zip(unique,range(len(unique))))
        columns_dict_len[column]=len(unique)
        new_columns+=[column+"&&"+str(c) for c in unique]


    # Logistic Regression
    print("Logistic Regression")
    parsedData = df_train.rdd.map(rowTransform).toDF()
    lr = LogisticRegression(maxIter=10, regParam=0.0, elasticNetParam=0.0)
    lrModel = lr.fit(parsedData)
    if mode != "train_test":
        trainingSummary = lrModel.summary
        print("areaUnderROC: " + str(trainingSummary.areaUnderROC))
        summary = lrModel.transform(parsedData)
        logloss=summary.select("probability","label").rdd.map(lambda r: math.log(r["probability"][1]+0.00000001)*r["label"]+
            math.log(r["probability"][0]+0.00000001)*(1-r["label"])).sum()/(-summary.rdd.count())
        print("logloss: "+ str(logloss))
    else:
        parsedData_test = df_test.rdd.map(rowTransform).toDF()
        trainingSummary =lrModel.evaluate(parsedData_test)
        summary = lrModel.transform(parsedData_test)
        print("areaUnderROC: " + str(trainingSummary.areaUnderROC))
        logloss = summary.select("probability", "label").rdd.map(
            lambda r: math.log(r["probability"][1]+0.00000001) * r["label"] +
                      math.log(r["probability"][0]+0.00000001) * (1 - r["label"])).sum() / (-summary.rdd.count())
        print("logloss: " + str(logloss))


    #生成json
    print("生成json")
    json_data={"evaluation_metrics":{"auc":str(trainingSummary.areaUnderROC),"logloss":logloss}}
    json_data["parameters"]={}
    coef=lrModel.coefficients
    for column_num in range(len(new_columns[1:])):
        json_data["parameters"][new_columns[column_num+1]]=coef[column_num]
    json_data["parameters"]["model_intercept"] = lrModel.intercept
    json_str = json.dumps(json_data)
    joutput = sc.parallelize([json_str],1)
    joutput.saveAsTextFile(output)


    spark.stop()

