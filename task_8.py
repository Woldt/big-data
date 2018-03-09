import pyspark
from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext, SparkSession

from pyspark.sql.types import IntegerType, FloatType

conf = (SparkConf()
         .setMaster("local")
         .setAppName("My app")
         .set("spark.executor.memory", "1g"))
sc = SparkContext()

spark = SparkSession.builder.getOrCreate()

logFile = "./data/geotweets.tsv"  # Should be some file on your system

data_frame = spark.read.option("sep", "\t").option("quote", "\n").csv(logFile).toDF('UTC_TIME', 'COUNTRY_NAME', 'COUNTRY_CODE', 'PLACE_TYPE', 'PLACE_NAME', 'LANGUAGE', 'USERNAME', 'USER_SCREEN_NAME', 'TIMEZONE_OFFSET', 'NUMBER_OF_FRIENDS', 'TWEET_TEXT', 'LATITUDE', 'LONGITUDE')
data_frame=data_frame.withColumn("LATITUDE", data_frame["LATITUDE"].cast(FloatType()))
data_frame=data_frame.withColumn("LONGITUDE", data_frame["LONGITUDE"].cast(FloatType()))

data_frame.createOrReplaceTempView("tweets")


def get_number_of_tweets():
    return spark.sql("SELECT COUNT(*) as NumberOfTweets FROM tweets")


def get_distinct_usernames():
    return spark.sql("SELECT COUNT(DISTINCT USERNAME) as DistinctUsernames FROM tweets")


def get_distinct_country_names():
    return spark.sql("SELECT COUNT(DISTINCT COUNTRY_NAME) as DistinctCountryNames FROM tweets")


def get_distinct_place_names():
    return spark.sql("SELECT COUNT(DISTINCT PLACE_NAME) as DistinctPlaceNames FROM tweets")


def get_distinct_languages():
    return spark.sql("SELECT COUNT(DISTINCT LANGUAGE) as DistictLanguages FROM tweets")


def get_max_lat_long():
    return spark.sql("SELECT MAX(LATITUDE), MAX(LONGITUDE) FROM tweets")


def get_min_lat_long():
    # return spark.sql("SELECT MIN(LATITUDE) FROM tweets")
    return spark.sql("SELECT MIN(LATITUDE), MIN(LONGITUDE) FROM tweets")

