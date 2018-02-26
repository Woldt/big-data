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
    return sorted(input_file.map(lambda tweet: tweet.split("\t")[COUNTRY_NAME]).map(lambda country: (country, 1)).reduceByKey(add).collect(), key=lambda country: (-country[1], country[0]))




def write_to_file(collection):
    """Writes the collection to a .tsv file"""
    sc.parallelize(collection).coalesce(1).saveAsTextFile("data/result_2.tsv")

write_to_file(total_tweets_from_country(file)
              )
# print(total_tweets_from_country(file))

sc.stop()

