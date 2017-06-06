#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Jun  5 06:26:19 2017

@author: tangent
"""
from pyspark.sql import SparkSession,Row
from pyspark.sql.context import SQLContext

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
            index=[columns_dict[column][i] for i in words]
            new_row+=oneHot2(index,columns_dict_len[column])
    return new_row
        
def oneHot(index,length):
    l=[0]*length
    if index==0: pass
    else: l[length-1]=1
    return l
    
def oneHot2(index,length):
    l=[0]*length
    for i in index:
        l[i]=1
    return l


if __name__ == "__main__":
    spark = SparkSession\
        .builder\
        .appName("OneHotEncoderExample")\
        .getOrCreate()
    sc = spark.sparkContext

    full_columns=["cid","age","sex","hobby"]
    main_key=["cid"]
    quantity=["age"]
    catagory=["sex"]
    quality=["hobby"]
    match_range=1
    training_start="20170101"
    training_end="20170103"
    
    #check() #is reasonable?rowTransform(row)
    
    new_columns=[]
    columns_dict={}
    columns_dict_len={}
    
    sqlContext=SQLContext(spark)
    
    # decide newcolumns and setsark on Jupyter notebook. Here is how Sp
    for column in full_columns:
        if column in main_key:
            new_columns.append(column)
        if column in quantity:
            new_columns.append(column)
    
    begin=True
    Custum = Row(*full_columns)
    for date in utils.date_range(training_start,training_end):
        files=utils.search("data/"+date, "summary")
        for file in files:
            lines = sc.textFile(file)
            parts = lines.map(lambda l: l.split(","))
            if begin:
                rows = parts.map(lambda p: Custum(*p)).filter(lambda p: p["cid"]!="" and p["cid"] is not None and p["cid"]!="cid")
            else:
                
                row_add = parts.map(lambda p: Custum(*p)).filter(lambda p: p["cid"]!="" and p["cid"] is not None and p["cid"]!="cid")
                rows=rows.union(row_add)
            begin=False

    
  
    for column in catagory+quality:
        columns_dict[column]=[]
    for column in catagory:
        columns_dict[column]+=[list(x.asDict().values())[0] for x in rows.select(column).distinct().collect()]
    for column in quality:
        columns_dict[column]+=[val for sublist in [utils.split(list(x.asDict().values())[0],"|") for x in rows.select(column).distinct().collect()] for val in sublist]
#    
#    for column in catagory:
#        unique=[i for i in list(set(columns_dict[column])) if i is not None]
#        columns_dict[column]=dict(zip(unique,range(1,1+len(unique))))
#        columns_dict_len[column]=len(unique)
#        columns_dict[column][None]=0
#        new_columns+=[column+"_"+str(c) for c in unique]
#
#    for column in quality:
#        unique=[i for i in list(set(columns_dict[column]))]
#        columns_dict[column]=dict(zip(unique,range(len(unique))))
#        columns_dict_len[column]=len(unique)
#        new_columns+=[column+"_"+str(c) for c in unique]
    
#    Custom=Row(*new_columns)
    
#    row=rows.first()
#    df_tran=rows.map(rowTransform)
#
#
    print("rows.collect(): ",rows.collect())
    print("type(rows): ",type(rows))
#    df_tran.show()
#    print (columns_dict["sex"])
#    print (columns_dict["hobby"])
#    print ("new_columns: ",new_columns)
#    print ("columns_dict_len: ",columns_dict_len)
#    print ("row: ",row)
#    print("rowTransform(row): ",rowTransform(row))
    
    
    
    
    
    
    spark.stop()

#test=sqlContext.read.csv("20170101",header=True)
#l=test.select("sex").rdd.distinct().map(lambda x:list(x.asDict().values())[0]).collect()
#l=test.select("hobby").rdd.distinct().flatMap(lambda x:list(x.asDict().values())[0].split("|")).collect()
