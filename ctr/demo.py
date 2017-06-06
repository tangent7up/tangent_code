#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Jun  6 11:26:41 2017

@author: youmi
"""
from pyspark.sql import SparkSession
from pyspark.sql import Row

full_columns=["cid","age","sex","hobby"]
spark = SparkSession \
    .builder \
    .appName("Python Spark SQL basic example") \
    .config("spark.some.config.option", "some-value") \
    .getOrCreate()

sc = spark.sparkContext


Custum = Row(*full_columns)
lines = sc.textFile("data/20170101/summary_20170101")
parts = lines.map(lambda l: l.split(","))
rows = parts.map(lambda p: Custum(*p)).filter(lambda p: p["cid"]!="" and p["cid"] is not None and p["cid"]!="cid")
lines = sc.textFile("data/20170101/summary_20170101_2")
parts = lines.map(lambda l: l.split(","))
rows2 = parts.map(lambda p: Custum(*p)).filter(lambda p: p["cid"]!="" and p["cid"] is not None and p["cid"]!="cid")
rows=rows.union(rows2)
print(rows.collect())
print(type(rows))
#df.rdd.map(lambda x: x)

spark.stop()