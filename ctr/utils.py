#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Jun  6 09:00:56 2017

@author: youmi
"""
import os
import datetime
from pyspark.sql import Row

def search(path, word=None):
    files=[]
    for filename in os.listdir(path):
        fp = os.path.join(path, filename)
        if os.path.isfile(fp):
            if not filename.startswith("."):
                if word is None:
                    files.append(fp)
                elif filename.startswith(word):
                    files.append(fp)
        elif os.path.isdir(fp):
            search(fp, word)
    return files

#path="data/20170101"
#word="summary"
#search("data/20170101", "summary")
#search("data/20170101", "tran")

def date_range(training_start,training_end):
    dates=[]
    end=datetime.datetime.strptime(training_end,"%Y%m%d")
    point=datetime.datetime.strptime(training_start,"%Y%m%d")
    while(end>=point):
        dates.append(datetime.datetime.strftime( point,'%Y%m%d'))
        point+=datetime.timedelta(days=1)
    return dates

def tran_dates(date,match_range):
    dates=[date]
    start=datetime.datetime.strptime(date,"%Y%m%d")
    for i in range(match_range):
        start+=datetime.timedelta(days=1)
        dates.append(datetime.datetime.strftime( start,'%Y%m%d'))
    return dates

#training_start="20170101"
#training_end="20170103"
#date_range(training_start,training_end)

def split(string,sign):
    if string is None:
        return[]
    else:
        return string.split(sign)


