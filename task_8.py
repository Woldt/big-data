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
data_frame.show()


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
