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

def lat_lng_from_country(input_file=sample_file):
    return input_file\
        .map(lambda tweet: (tweet.split("\t")[COUNTRY_NAME], ((float(tweet.split("\t")[LATITUDE]), float(tweet.split("\t")[LONGITUDE])), 1)))\
        .reduceByKey(lambda x, y : ((x[0][0] + y[0][0], x[0][1] + y[0][1]), x[1] + y[1]))\
        .filter(lambda country: country[1][1] > 10)\
        .collect()

def reduce_lat_long(input_file=sample_file):
    countries = lat_lng_from_country(input_file)
    centroids = []
    for country in countries:
        string = country[0] + "\t" + str(country[1][0][0] / country[1][1]) + "\t" + str(country[1][0][1] / country[1][1])
        centroids.append(string)
    return centroids



def write_to_file(collection):
    """Writes the collection to a .tsv file"""
    sc.parallelize(collection).coalesce(1).saveAsTextFile("data/result_3.tsv")

write_to_file(reduce_lat_long())

sc.stop()

