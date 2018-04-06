import pyspark
from pyspark import SparkConf, SparkContext
from datetime import datetime as dt

conf = (SparkConf()
         .setMaster("local")
         .setAppName("My app")
         .set("spark.executor.memory", "1g"))
sc = SparkContext()


logFile = "../data/geotweets.tsv"  # Should be some file on your system

file = sc.textFile(logFile)  # Entire file as RDD object
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


def find_most_active_hours(input_file=sample_file):
    """ Create a file {result_4.tsv} containing total country, most active hour on twitter and
        number of tweets for that hour from {input_file}

    FORMAT -- {Country} {HOUR}  {Number of tweets}
    
    Keyword Arguments:
         input_file {Spark RDD object} -- Spark rdd object based on TSV file (default: {sample_file})
    """
    input_file\
        .map(lambda tweet: ((tweet.split("\t")[COUNTRY_NAME], dt.fromtimestamp(float(tweet.split("\t")[UTC_TIME]) / 1000 - float(tweet.split("\t")[TIMEZONE_OFFSET])).hour), 1)) \
        .reduceByKey(lambda list_x, list_y: list_x+list_y) \
        .map(lambda country_hour: (country_hour[0][0], (country_hour[0][1], country_hour[1]))) \
        .reduceByKey(lambda x, y: x if x[1] > y[1] else y) \
        .map(lambda country_hour: country_hour[0] + "\t" + str(country_hour[1][0]) + "\t" + str(country_hour[1][1])) \
        .coalesce(1) \
        .saveAsTextFile("data/result_4.tsv")

find_most_active_hours(file)


sc.stop()

