from pyspark import SparkContext
from datetime import datetime, timedelta
from operator import add

sc = SparkContext("local[*]", "tdt4300")
rawData = sc.textFile("../dataset_TIST2015.tsv", use_unicode=False)

header = rawData.first()
data = rawData.filter(lambda x: x != header)
dataWithoutWhite = data.map(lambda x: x.split('\n')[0].split('\t'))
dataTouples = dataWithoutWhite.map(
    lambda x: (x[3], x[4]))
result = dataTouples.map(
    lambda x: datetime.strptime(x[0], '%Y-%m-%d %H:%M:%S') + timedelta(minutes=int(x[1]))).map(
    lambda x: str(x))

print result.take(10)
