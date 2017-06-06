#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Jun  5 06:26:19 2017

@author: tangent
"""


from pyspark.sql.session import SparkSession
from pyspark.sql.context import SQLContext
import utils
import datetime

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
        .appName("OneHotEncoderCtr")\
        .config("spark.some.config.option", "some-value") \
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
    
    files=[]
    for date in utils.date_range(training_start,training_end):
        files+=[(i,date) for i in utils.search("data/"+date, "summary")]
    df = spark.read.csv(files[0][0],header=True)
    df.withColumn('date', date)
    for file in files[1:]:
        df_add= spark.read.csv(file[0],header=True)
        df_add.withColumn('date', date)
        df=df.union(df_add)
    
  
    for column in catagory+quality+["date"]:
        columns_dict[column]=[]
    for column in catagory:
        columns_dict[column]+=[list(x.asDict().values())[0] for x in df.select(column).distinct().collect()]
    for column in quality:
        columns_dict[column]+=[val for sublist in [utils.split(list(x.asDict().values())[0],"|") for x in df.select(column).distinct().collect()] for val in sublist]
    
    for column in catagory:
        unique=[i for i in list(set(columns_dict[column])) if i is not None]
        columns_dict[column]=dict(zip(unique,range(1,1+len(unique))))
        columns_dict_len[column]=len(unique)
        columns_dict[column][None]=0
        new_columns+=[column+"_"+str(c) for c in unique]

    for column in quality:
        unique=[i for i in list(set(columns_dict[column]))]
        columns_dict[column]=dict(zip(unique,range(len(unique))))
        columns_dict_len[column]=len(unique)
        new_columns+=[column+"_"+str(c) for c in unique]
    
#    Custom=Row(*new_columns)
    
#    row=df.first()
#    rdd=df.rdd
#    df_tran=rdd.map(rowTransform)
    
    
    
    df.show()
#    print(df_tran.collect())
    print (columns_dict["sex"])
    print (columns_dict["hobby"])
    print ("new_columns: ",new_columns)
    print ("columns_dict_len: ",columns_dict_len)
    
    
    
    
    
    
    spark.stop()

#test=sqlContext.read.csv("20170101",header=True)
#l=test.select("sex").rdd.distinct().map(lambda x:list(x.asDict().values())[0]).collect()
#l=test.select("hobby").rdd.distinct().flatMap(lambda x:list(x.asDict().values())[0].split("|")).collect()
