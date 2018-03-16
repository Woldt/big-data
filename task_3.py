import pyspark
from pyspark import SparkConf, SparkContext
from operator import add
conf = (SparkConf()
         .setMaster("local")
         .setAppName("My app")
         .set("spark.executor.memory", "1g"))
sc = SparkContext()


logFile = "./data/geotweets.tsv"  # Should be some file on your system

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


def geographical_centroids_per_country(input_file=sample_file):
    """Creates a file {result_3.tsv} containing centroids, containing Country, (lat, long), for countries with more
        than 10 tweets from {input_file}
    
    FORMAT -- {Country} {Latitude}  {Longitude} 

    Keyword Arguments:
         input_file {Spark RDD object} -- Spark rdd object based on TSV file (default: {sample_file})
    """
    input_file\
        .map(lambda tweet: (tweet.split("\t")[COUNTRY_NAME], ((float(tweet.split("\t")[LATITUDE]), float(tweet.split("\t")[LONGITUDE])), 1)))\
        .reduceByKey(lambda countryLatLngX, countryLatLngY: ((countryLatLngX[0][0] + countryLatLngY[0][0], countryLatLngX[0][1] + countryLatLngY[0][1]), countryLatLngX[1] + countryLatLngY[1]))\
        .filter(lambda country: country[1][1] > 10) \
        .map(lambda country: country[0] + "\t" + str(country[1][0][0] / country[1][1]) + "\t" + str(country[1][0][1] / country[1][1])) \
        .coalesce(1)\
        .saveAsTextFile("data/result_3.tsv")

geographical_centroids_per_country(file)


sc.stop()

