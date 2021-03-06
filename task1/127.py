import math
import sys
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

def formatOutput(data):
    return '    '.join(str(d) for d in data)

def computeDistance(locations):
    sum_distance = 0
    for i in range(0, len(locations) - 1):
        sum_distance += hav(float(locations[i][0]), float(locations[i][1]),
                float(locations[i+1][0]), float((locations[i+1][1])))

    return sum_distance

def hav(lat1, lon1, lat2, lon2):
    # convert decimal degrees to radians
    lon1, lat1, lon2, lat2 = map(math.radians, [lon1, lat1, lon2, lat2])

    # haversine formula
    dlon = lon2 - lon1
    dlat = lat2 - lat1
    a = math.sin(dlat/2)**2 + math.cos(lat1) * math.cos(lat2) * math.sin(dlon/2)**2
    c = 2 * math.asin(math.sqrt(a))
    r = 6371 # Radius of earth in kilometers. Use 3956 for miles
    return c * r

userData = dataWithoutWhite\
    .map(lambda x: (x[2], (x[0], x[1], x[3], x[4], x[5], x[6], x[7],
        x[8])))

distanceData = dataWithoutWhite\
    .map(lambda x: (x[2], (x[5], x[6])))\
    .groupByKey()\
    .filter(lambda x: len(x[1]) > 3)\
    .map(lambda x: (x[0], (computeDistance(list(x[1])))))\
    .filter(lambda x: x[1] > 50)\
    .sortBy(lambda x: x[1], ascending=False)\
    .take(100)

distanceAndUserData = sc.parallelize(distanceData)\
    .join(userData)\
    .map(lambda x: (x[0], x[1][0],
    x[1][1][0],x[1][1][1],x[1][1][2],
    x[1][1][3],x[1][1][4],x[1][1][5],x[1][1][6],x[1][1][7]))\
    .map(lambda x: formatOutput(x))\
    .coalesce(1)\
    .saveAsTextFile("output.tsv")

