# -*- coding: utf-8 -*-
"""
Created on Fri Apr 10 22:36:17 2020

@author: sami
"""

import csv, io
from collections import Counter
from pyspark import SparkContext
import sys

def to_csv(x):
    output = io.StringIO("")
    csv.writer(output).writerow(x)
    return output.getvalue().strip()

def extractrecords(partId, records):
    if partId==0:
        next(records)
    reader = csv.reader(records)
    for row in reader:
        year = row[0].split('-')[0]
        product = row[1].lower()
        company = row[7].lower()
        yield ((product,year),(1,[company]))
        
if __name__ == '__main__':
    sc = SparkContext()
    sc.textFile(sys.argv[1])\
        .mapPartitionsWithIndex(extractrecords)\
        .reduceByKey(lambda x,y: (x[0]+y[0], x[1]+y[1]))\
        .mapValues(lambda x: (x[0],x[1],Counter(x[1]).most_common(1)[0][1]))\
        .map(lambda x: (x[0][0],x[0][1],x[1][0],len(set(x[1][1])),round((x[1][2]/x[1][0])*100)))\
        .sortBy(lambda x: (x[0],x[1]))\
        .map(to_csv)\
        .saveAsTextFile(sys.argv[2])