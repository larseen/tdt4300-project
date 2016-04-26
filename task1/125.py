from pyspark import SparkContext
from datetime import datetime, timedelta
from operator import add
from operator import concat
from collections import Counter

sc = SparkContext("local[*]", "tdt4300")
rawData = sc.textFile("../dataset_TIST2015.tsv",
        use_unicode=False).cache()

header = rawData.first()
data = rawData.filter(lambda x: x != header)

dataWithoutWhite = data.map(lambda x: x.split('\n')[0].split('\t'))

dataTouples = dataWithoutWhite\
    .map(lambda x: (x[2], x[0]))\
    .groupByKey()\
    .map(lambda x : (len(x[1]), x[0]))\
    .groupByKey()\
    .map(lambda x: (x[0], len(x[1])))

print dataTouples.take(10)
