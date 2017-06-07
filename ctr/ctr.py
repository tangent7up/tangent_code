#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Jun  5 06:26:19 2017

@author: tangent
"""

from pyspark.sql.session import SparkSession
from pyspark.sql import Row
from pyspark.mllib.regression import LabeledPoint, LinearRegressionWithSGD, LinearRegressionModel
import utils



def rowTransform(row):
    new_row=[]
    for column in full_columns:
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


if __name__ == "__main__":

    #需要根据实际情况处理的参数
    full_columns=["cid","age","sex","hobby","target"]
    main_key=["cid"]
    quantity=["age"]
    catagory=["sex"]
    quality=["hobby"]
    target=["target"]
    match_range=1   #匹配搜索，如设定为0则只匹配当日的转化。
    training_start="20170101"   #训练集的始末日期
    training_end="20170103"



    spark = SparkSession\
        .builder\
        .appName("OneHotEncoderCtr")\
        .config("spark.some.config.option", "some-value") \
        .getOrCreate()
    sc = spark.sparkContext


    new_columns=[]
    columns_dict={}
    columns_dict_len={}



    #将数据根据是否转化打上target，并生成一个dataframe
    Custom = Row(*full_columns)
    all_rows=sc.parallelize([])
    for date in utils.date_range(training_start,training_end):
        tran_dates=utils.tran_dates(date,match_range)
        files=utils.search("data/"+date, "summary")
        search_files=[]
        for search_date in tran_dates:
            try:
                search_files+=utils.search("data/"+search_date, "tran")
            except:
                print("date "+search_date+ " is not exist")
        tran_cus=[]
        for search_file in search_files:
            with open(search_file) as read:
                tran_cus+=[i.strip() for i in read.readlines() if i.strip()!=""]
        tran_cus_b=sc.broadcast(tran_cus)
        rows = sc.parallelize([])
        all_cus=sc.parallelize([])
        for file in files:
            lines=sc.textFile(file)
            parts = lines.map(lambda l: l.split(",")).filter(lambda x:x[0]!="" and x[0]!="cid")
            rows=rows.union(parts)
        rows=rows.map(lambda r: r+[int(r[0] in tran_cus_b.value)])
        rows=rows.map(lambda r: Custom(*r))
        all_rows=all_rows.union(rows)
    df = spark.createDataFrame(all_rows)



    #确定需要做Onehot操作的维度
    for column in catagory+quality:
        columns_dict[column]=[]
    for column in catagory:
        columns_dict[column]+=[list(x.asDict().values())[0] for x in df.select(column).distinct().collect()]
    for column in quality:
        columns_dict[column]+=[val for sublist in [utils.split(list(x.asDict().values())[0],"|") for x in df.select(column).distinct().collect()] for val in sublist]



    #生成onehot后新的列--------------
    for column in full_columns:
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
        new_columns+=[column+"_"+str(c) for c in unique]

    rows_tran=df.rdd.map(lambda x:rowTransform(x))
    Onehot = Row(*new_columns+target)
    rows=rows_tran.map(lambda r: Onehot(*r))
    df2 = spark.createDataFrame(rows)



    parsedData = rows_tran.map(lambda r : LabeledPoint(r[-1], r[1:-1]))
    model = LinearRegressionWithSGD.train(parsedData, iterations=1000, step=0.00000001)

    valuesAndPreds = parsedData.map(lambda p: (p.label, model.predict(p.features)))

    MSE = valuesAndPreds \
              .map(lambda tup: (tup[0] - tup[1]) ** 2) \
              .reduce(lambda x, y: x + y) / valuesAndPreds.count()
    print("Mean Squared Error = " + str(MSE))



    #检测区
    df.show()
    df2.show()
    print (columns_dict["sex"])
    print (columns_dict["hobby"])
    print ("new_columns: ",new_columns)
    print ("columns_dict_len: ",columns_dict_len)


    spark.stop()

