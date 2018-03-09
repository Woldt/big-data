import pyspark
from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext, SparkSession
from operator import add

conf = (SparkConf()
         .setMaster("local")
         .setAppName("My app")
         .set("spark.executor.memory", "1g"))
sc = SparkContext()

spark = SparkSession.builder.getOrCreate()




logFile = "./data/geotweets.tsv"  # Should be some file on your system
stopwords = "./data/stop_words.txt"  # Should be some file on your system

data_frame = spark.read.option("sep", "\t").csv(logFile).toDF('UTC_TIME', 'COUNTRY_NAME', 'COUNTRY_CODE', 'PLACE_TYPE', 'PLACE_NAME', 'LANGUAGE', 'USERNAME', 'USER_SCREEN_NAME', 'TIMEZONE_OFFSET', 'NUMBER_OF_FRIENDS', 'TWEET_TEXT', 'LATITUDE', 'LONGITUDE')
# sample_file = file.sample(False, 0.01, 5)  # Sample file, 10% of original file
# data_frame.show()

data_frame.createOrReplaceTempView("tweets")


def get_number_of_tweets():
    return spark.sql("SELECT * FROM tweets").count()


def get_distinct_usernames():
    return spark.sql("SELECT USERNAME FROM tweets").distinct().count()


def get_distinct_country_names():
    return spark.sql("SELECT COUNTRY_NAME FROM tweets").distinct().count()


def get_distinct_place_names():
    return spark.sql("SELECT PLACE_NAME FROM tweets").distinct().count()


def get_distinct_languages():
    return spark.sql("SELECT LANGUAGE FROM tweets").distinct().count()


