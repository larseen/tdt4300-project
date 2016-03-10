import os
import sys
import math
from pyspark import  SparkContext, SparkConf

conf = (SparkConf()
        .set("spark.cores.max", 4)
        .set("spark.executor.instances", 4))
sc = SparkContext('local[*]', 'pyspark', conf=conf)
userData = sc.textFile("../dataset_TIST2015.tsv", 4,  use_unicode=False)
cityData = sc.textFile("../dataset_TIST2015_Cities.txt", 4, use_unicode=False)


userHeader = userData.first()
userCoordData = userData.filter(
        lambda x: x != userHeader).map(
        lambda x: x.split('\n')[0].split('\t')).map(
        lambda x: (x[0], x[5], x[6]))

cityCoordData = cityData.map(
        lambda x: x.split('\n')[0].split('\t')).map(
        lambda x: (x[0], x[1], x[2], x[4]))

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
cartUserCity = userCoordData.cartesian(cityCoordData)

cartUserCity = userCoordData.cartesian(cityCoordData).map(
        lambda ((i, lat1, lon1), (c, lat2, lon2, con)): (i, c, con,
            hav(float(lat1), float(lon1), float(lat2),
                float(lon2)), lat1, lon1)).filter(
        lambda (i, c, con, d, lat1, lon1): d < 1)

result = cartUserCity
