import os
import sys
import math
from pyspark import  SparkContext, SparkConf

conf = (SparkConf()
        .set("spark.cores.max", 4)
        .set("spark.executor.instances", 4))
sc = SparkContext('local[*]', 'pyspark', conf=conf)
userData = sc.textFile("../dataset_TIST2015.tsv", 4,
        use_unicode=False).cache()
cityData = sc.textFile("../dataset_TIST2015_Cities.txt", 4, use_unicode=False)

userHeader = userData.first()
cityHeader = cityData.first()

userData = userData.filter(
        lambda x: x != userHeader).map(
        lambda x: x.split('\n')[0].split('\t'))
cityData = cityData.filter(
        lambda x: x != cityHeader).map(
        lambda x: x.split('\n')[0].split('\t'))

numUserIds = userData \
        .map(lambda x: x[1]) \
        .distinct() \
        .count()
numCheckins = userData \
        .map(lambda x: x[0]) \
        .distinct() \
        .count()
numSessions = userData \
        .map(lambda x: x[2]) \
        .distinct() \
        .count()
numCountries = cityData \
        .map(lambda x: x[4]) \
        .distinct() \
        .count()
numCities = cityData \
        .map(lambda x: x[0]) \
        .distinct() \
        .count()
