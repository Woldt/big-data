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
    """Return total numbers of tweets per country"""
    return sorted(input_file.map(lambda tweet: tweet.split("\t")[COUNTRY_NAME]).map(lambda country: (country, 1)).reduceByKey(add).collect(), key=lambda country: (-country[1], country[0]))


def convert(collection):
    """Converts collection of tuples, to collection in tsv friendly format"""
    tmp_collection = []
    for t in collection:
        tmp_collection.append(t[0] + "\t" + str(t[1]))
    return tmp_collection


def write_to_file(collection):
    """Writes the collection to a .tsv file"""
    sc.parallelize(convert(collection)).coalesce(1).saveAsTextFile("data/result_2.tsv")


write_to_file(total_tweets_from_country(file))

sc.stop()

