from pyspark import  SparkContext
from datetime import datetime
import sys
import re, string
import calendar

sc = SparkContext("local[*]", "tdt4300")

##
#
# INPUT ARGUMENTS
#
##
input_file = sys.argv[1]
output_file = sys.argv[2]
positive_words = sys.argv[3]
negative_words = sys.argv[4]

##
#
# INSERT AND PREP DATA
#
##
positiveWords = sc.textFile(positive_words).collect()
positiveWords = set(positiveWords)
negativeWords = sc.textFile(negative_words).collect()
negativeWords = set(negativeWords)
rawData = sc.textFile(input_file, use_unicode=False)
data = rawData.map(lambda x: x.split('\n')[0].split('\t'))\

##
#
# HELPER FUNCTIONS
#
##

def parseDate(unix):
    date = datetime.fromtimestamp(float(unix[0:-3]))
    return calendar.day_name[date.weekday()]

def createKey(weekday, city):
    return ''.join([weekday," ",city])

def getCity(line):
    return line.split(' ',1)[1]

def getWeekday(line):
    return line.split(' ', 1)[0]

def mergeTweets(a, b):
    return ''.join([a," ",b])

def analyzeTweet(tweets):
    wordDict = {}
    for word in positiveWords:
        wordDict[word] = 1
    for word in negativeWords:
        wordDict[word] = -1

    tweetsList = tweets.split(' ')
    score = 0
    for tweet in tweetsList:
        if tweet in wordDict:
            score += wordDict[tweet]
    return score

def formatOutput(data):
    return '  '.join(str(d) for d in data)

##
#
# PROCESS DATA
#
##

tweets = data\
    .filter(lambda row: row[5] == 'en' and row[2] == 'US' and row[3] == 'city')\
    .map(lambda row: (createKey(parseDate(row[0]), row[4]), row[10]))\
    .combineByKey(str, mergeTweets, mergeTweets)\
    .map(lambda row: (getCity(row[0]), getWeekday(row[0]), analyzeTweet(row[1])))\
    .map(lambda row: formatOutput(row))\
    .saveAsTextFile(output_file)\
