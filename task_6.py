import pyspark
from pyspark import SparkConf, SparkContext
from operator import add

conf = (SparkConf()
         .setMaster("local")
         .setAppName("My app")
         .set("spark.executor.memory", "1g"))
sc = SparkContext()


logFile = "./data/geotweets.tsv"  # Should be some file on your system
stopwords = "./data/stop_words.txt"  # Should be some file on your system

file = sc.textFile(logFile)  # Entire file
stopwordFile = sc.textFile(stopwords)  # Entire file
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

def most_frequent_words(input_file=sample_file):
    """
    : returns
    """
    stopwords = stopwordFile.map(lambda word: word).collect()
    return input_file\
        .map(lambda tweet: (tweet.split("\t")[COUNTRY_CODE], tweet.split("\t")[TWEET_TEXT]))\
        .filter(lambda tweet: tweet[0] == "US") \
        .collect()


def write_to_file(collection):
    """Writes the collection to a .tsv file"""
    sc.parallelize(collection).coalesce(1).saveAsTextFile("data/result_1.tsv")


def mergelists():
    rdd = sc.parallelize([[1, 2, 4, 2, 5], [1, 4, 2, 6, 1]])
    return rdd.flatMap(lambda x: x).map((lambda y: (y, 1))).reduceByKey(add).collect()


print(most_frequent_words())
# print(mergelists())
sc.stop()

