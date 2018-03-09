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

data_frame = spark.read.option("sep", "\t").csv(logFile).toDF('UTC_TIME', 'COUNTRY_NAME', 'COUNTRY_CODE', 'PLACE_TYPE', 'PLACE_NAME', 'LANGUAGE', 'USERNAME', 'USER_SCREEN_NAME', 'TIMEZONE_OFFSET', 'NUMBER_OF_FRIENDS', 'TWEET_TEXT', 'LATITUDE', 'LONGITUDE')
data_frame=data_frame.withColumn("LATITUDE", data_frame["LATITUDE"].cast(FloatType()))
data_frame=data_frame.withColumn("LONGITUDE", data_frame["LONGITUDE"].cast(FloatType()))
# sample_file = file.sample(False, 0.01, 5)  # Sample file, 10% of original file
# data_frame.show()

data_frame.createOrReplaceTempView("tweets")


def get_number_of_tweets():
    return spark.sql("SELECT * FROM tweets").count()


def get_max_lat_long():
    return spark.sql("SELECT MAX(LATITUDE), MAX(LONGITUDE) FROM tweets").collect()


def get_min_lat_long():
    return spark.sql("SELECT MIN(LATITUDE) FROM tweets")
    # return spark.sql("SELECT MIN(LATITUDE), MIN(LONGITUDE) FROM tweets").collect()

print(get_min_lat_long())