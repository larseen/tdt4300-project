from pyspark import SparkContext
from datetime import datetime, timedelta
from operator import add
from operator import concat
from collections import Counter

sc = SparkContext("local", "tdt4300")
rawData = sc.textFile("../dataset_TIST2015.tsv", use_unicode=False)

header = rawData.first()
data = rawData.filter(lambda x: x != header)

dataWithoutWhite = data.map(lambda x: x.split('\n')[0].split('\t'))

dataTouples = dataWithoutWhite\
    .map(lambda x: (x[2], x[0]))\
    .groupByKey().map(lambda x : (x[0], len(x[1])))\
    .countByValue().map(lambda x : x[1]).items()\
    .take(10)\

print(dataTouples)
