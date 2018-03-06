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

def tweets_per_city(input_file=sample_file):
    """
    : return  most active hours
    """
    return input_file\
        .map(lambda tweet: (tweet.split("\t")[PLACE_NAME], tweet.split("\t")[PLACE_TYPE], tweet.split("\t")[COUNTRY_CODE]))\
        .filter(lambda place: place[1] == "city" and place[2] == "US") \
        .map(lambda city: (city[0], 1))\
        .reduceByKey(add)\
        .sortBy(lambda city: (-city[1], city[0]))\
        .collect()

        # .map(lambda element: (element[0][0], (element[0][1], element[1]))) \
        # .reduceByKey(lambda x, y: x if x[1] > y[1] else y) \
        # .sortByKey() \


def write_to_file(collection):
    """Writes the collection to a .tsv file"""
    sc.parallelize(collection).coalesce(1).saveAsTextFile("data/result_5.tsv")

write_to_file(tweets_per_city(file))
sc.stop()

