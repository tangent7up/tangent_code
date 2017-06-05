#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Jun  5 06:26:19 2017

@author: tangent
"""
from pyspark import SparkSession
from pyspark.sql import sqlContext

full_columns=["cid","age","sex","hobby"]
main_key=["cid"]
quantity=["age"]
catagory=["sex"]
quality=["hobby"]
match_range=1
training_start=20170101
training_end=20170103

#check() #is reasonable?

new_columns=[]
columns_dict={}

# decide newcolumns and sets
for column in full_columns:
    if column in main_key:
        new_columns.append(column)
    if column in quantity:
        new_columns.append(column)



for date in range(training_start,training_end+1):
    df=sqlContext.read.csv(str(date),header=True)
    for column in catagory:
        columns_dict[column]=[]
    for column in catagory:
        columns_dict[column]+=df.select(column).rdd.distinct().map(lambda x:list(x.asDict().values())[0]).collect()
    for column in quality:
        columns_dict[column]+=df.select(column).rdd.distinct().flatMap(lambda x:list(x.asDict().values())[0].split("|")).collect()

        
for column in catagory:
    unique=list(set(columns_dict[column]))
    columns_dict[column]=dict(zip(unique,range(len(unique))))
    new_columns+=[column+str(c) for c in unique]

for column in quality:
    unique=list(set(columns_dict[column]))
    columns_dict[column]=dict(zip(unique,range(len(unique))))
    new_columns+=[column+str(c) for c in unique]


test=sqlContext.read.csv("20170101",header=True)
l=test.select("sex").rdd.distinct().map(lambda x:list(x.asDict().values())[0]).collect()
l=test.select("hobby").rdd.distinct().flatMap(lambda x:list(x.asDict().values())[0].split("|")).collect()
