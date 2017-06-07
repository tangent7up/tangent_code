#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Jun  6 11:26:41 2017

@author: youmi
"""
from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark import SparkContext
from pyspark.mllib.regression import LabeledPoint, LinearRegressionWithSGD, LinearRegressionModel

spark = SparkSession \
    .builder \
    .appName("Python Spark SQL basic example") \
    .config("spark.some.config.option", "some-value") \
    .getOrCreate()
sc = spark.sparkContext


def parsePoint(line):
    values = [float(x) for x in line.replace(',', ' ').split(' ')]
    return LabeledPoint(values[0], values[1:])

data = sc.textFile("lpsa.data")
parsedData = data.map(parsePoint)

model = LinearRegressionWithSGD.train(parsedData, iterations=100, step=0.00000001)
valuesAndPreds = parsedData.map(lambda p: (p.label, model.predict(p.features)))

MSE = valuesAndPreds \
          .map(lambda tup: (tup[0] - tup[1]) ** 2) \
          .reduce(lambda x, y: x + y) / valuesAndPreds.count()
print("Mean Squared Error = " + str(MSE))


spark.stop()




























