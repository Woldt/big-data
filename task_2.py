import pyspark
from pyspark import SparkConf, SparkContext
from operator import add

conf = (SparkConf()
         .setMaster("local")
         .setAppName("My app")
         .set("spark.executor.memory", "1g"))
sc = SparkContext()


logFile = "./data/geotweets.tsv"  # Should be some file on your system

file = sc.textFile(logFile)  # Entire file
sample_file = file.sample(False, 0.01, 5)  # Sample file, 10% of original file

UTC_TIME = 0
COUNTRY_NAME = 1
COUNTRY_CODE = 2
PLACE_TYPE = 3
PLACE_NAME = 4
LANGUAGE = 5
USERNAME = 6
USER_SCREEN_NAME = 7
TIMEZONE_OFFSET = 8
NUMBER_OF_FRIENDS = 9
TWEET_TEXT = 10
LATITUDE = 11
LONGITUDE = 12


def total_tweets_from_country(input_file=sample_file):
    """: return total numbers of tweets per country"""
    input_file\
        .map(lambda country: (country.split("\t")[COUNTRY_NAME], 1))\
        .reduceByKey(add) \
        .sortBy(lambda country: (-country[1], country[0])) \
        .map(lambda city: city[0] + "\t" + str(city[1])) \
        .coalesce(1)\
        .saveAsTextFile("data/result_2.tsv")


total_tweets_from_country(file)

sc.stop()

