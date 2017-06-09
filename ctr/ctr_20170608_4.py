#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Jun  5 06:26:19 2017

@author: tangent
"""
import math

from pyspark.sql.session import SparkSession
from pyspark.sql import Row
from pyspark.ml.linalg import Vectors

from pyspark.ml.classification import LogisticRegression
import utils



def rowTransform(row):
    new_row=[]
    for column in full_columns+["target"]:
        if column in main_key:
            new_row.append(row[column])
        if column in quantity:
            new_row.append(row[column])            
        if column in catagory:
            new_row+=oneHot(columns_dict[column][row[column]],columns_dict_len[column])
        if column in quality:
            words=utils.split(row[column],"|")
            index=[columns_dict[column].get(i,None) for i in words if columns_dict[column].get(i,None) is not None]
            new_row+=oneHot2(index,columns_dict_len[column])
        if column in target:
            new_row.append(row[column])
    return new_row
        
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

def get_data(training_start,training_end,have_target=True):
    print("将数据根据是否转化打上target，并生成一个dataframe")
    if have_target:
        Custom = Row(*full_columns+["target"])
    else:
        Custom = Row(*full_columns)
    all_rows= sc.parallelize([])
    for date in utils.date_range(training_start, training_end):
        tran_dates = utils.tran_dates(date, match_range)
        files = utils.search("data/" + date, "part")
        rows = sc.parallelize([])
        all_cus = sc.parallelize([])
        for file in files:
            lines = sc.textFile(file)
            parts = lines.map(lambda l: l.split(",")).filter(lambda x: x[0] != "" and x[0] != "cid" and len(x) == 22)
            rows = rows.union(parts)
        if have_target:
            search_files = []
            for search_date in tran_dates:
                try:
                    search_files += utils.search("data/" + search_date, "tran")
                except:
                    print("date " + search_date + " is not exist")
            tran_cus = []
            for search_file in search_files:
                with open(search_file) as read:
                    tran_cus += [i.strip() for i in read.readlines() if i.strip() != ""]
            tran_cus_b = sc.broadcast(tran_cus)
            rows = rows.map(lambda r: r + [int(r[0] in tran_cus_b.value)])
        rows = rows.map(lambda r: Custom(*r))
        all_rows = all_rows.union(rows)
    df = spark.createDataFrame(all_rows)
    return df

if __name__ == "__main__":

    #需要根据实际情况处理的参数
    full_columns=['cid','adx','ssp','sid','hod','dow','pub_cat1','pub_cat2','s_bundle', 'yob',
                  'gender', 'conn_type', 'make', 'model', 'os', 'osv', 'country', 'third_ad_partner',
                  'ad_cat1', 'ad_cat2', 'ad_bundle', 'marketing_sdk']
    main_key=["cid"]
    quantity=[]
    catagory=['adx','ssp','hod','dow','pub_cat1','pub_cat2','conn_type','os','country','third_ad_partner','ad_cat1','marketing_sdk']
    quality=[]
    dump=[]
    target=["target"]
    match_range=1   #匹配搜索，如设定为0则只匹配当日的转化。
    bias=99
    training_start="20170102"   #训练集的始末日期
    training_end="20170105"



    spark = SparkSession\
        .builder\
        .appName("OneHotEncoderCtr")\
        .config("spark.some.config.option", "some-value") \
        .getOrCreate()
    sc = spark.sparkContext


    new_columns=[]
    columns_dict={}
    columns_dict_len={}


    df=get_data(training_start,training_end)



    #确定需要做Onehot操作的维度
    print("确定需要做Onehot操作的维度")
    for column in catagory+quality:
        columns_dict[column]=[]
    for column in catagory:
        columns_dict[column]+=[list(x.asDict().values())[0] for x in df.select(column).distinct().collect()]
    for column in quality:
        columns_dict[column]+=[val for sublist in [utils.split(list(x.asDict().values())[0],"|") for x in df.select(column).distinct().collect()] for val in sublist]



    #生成onehot后新的列--------------
    print("生成onehot后新的列")
    for column in full_columns+["target"]:
        if column in main_key:
            new_columns.append(column)
        if column in quantity:
            new_columns.append(column)
    for column in catagory:
        unique=[i for i in list(set(columns_dict[column])) if i !=""]
        columns_dict[column]=dict(zip(unique,range(1,1+len(unique))))
        columns_dict_len[column]=len(unique)
        columns_dict[column][""]=0
        new_columns+=[column+"_"+str(c) for c in unique]
    for column in quality:
        unique=[i for i in list(set(columns_dict[column])) if i!=""]
        columns_dict[column]=dict(zip(unique,range(len(unique))))
        columns_dict_len[column]=len(unique)
        new_columns+=[column+":"+str(c) for c in unique]



    print("生成稀疏矩阵")
    rows_tran=df.rdd.map(lambda x:rowTransform(x))
    Onehot = Row(*new_columns+target)

    rows=rows_tran.map(lambda r: Onehot(*r))
    df2 = spark.createDataFrame(rows)


    # Logistic Regression
    print("Logistic Regression")
    test_parsedData = rows_tran.map(lambda r: Row(label=r[-1], features=Vectors.dense(r[1:-1]))).toDF()
    parsedData = rows_tran.flatMap(lambda r: [Row(label=r[-1], features=Vectors.dense(r[1:-1]))] * (r[-1] * bias + 1)).toDF()




    lr = LogisticRegression(maxIter=10, regParam=0.0, elasticNetParam=0.0)
    lrModel = lr.fit(parsedData)



    trainingSummary = lrModel.summary
    # print(trainingSummary.roc.rdd.collect())
    print("areaUnderROC: " + str(trainingSummary.areaUnderROC))
    summary=lrModel.transform(parsedData.select("features"))


    logloss=summary.select("probability","prediction").rdd.map(lambda r: math.log(r["probability"][1])*r["prediction"]+
        math.log(r["probability"][0])*(1-r["prediction"])).sum()/(-summary.rdd.count())
    print("logloss: "+ str(logloss))

    print(lrModel.coefficients)
    print(lrModel.intercept)
    print(new_columns)

    json_data={"evaluation_metrics":{"auc":str(trainingSummary.areaUnderROC),"logloss":logloss}}
    json_data["parameters"]={}
    for column_num in range(len(new_columns[1:])):
        json_data["parameters"][new_columns[column_num+1]]=lrModel.coefficients[column_num]
    json_data["parameters"]["model_intercept"] = lrModel.intercept





    # #检测区




    spark.stop()

