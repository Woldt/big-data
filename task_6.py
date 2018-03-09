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


def find_most_frequent_words(input_file=sample_file):
    """
    : returns RDD containing the most frequent word in input_file with a count of the current word
    speparated with a tab. Everything is written to result_6.tsv.
    """
    stopwords = stopwordFile.map(lambda word: word).collect()
    return input_file\
        .map(lambda tweet: (tweet.split("\t")[COUNTRY_CODE], [word for word in tweet.split("\t")[TWEET_TEXT].lower().split(" ") if word not in stopwords and len(word) >= 2]))\
        .filter(lambda tweet: (tweet[0] == "US"))\
        .flatMap(lambda word: word[1]) \
        .map(lambda word: (word, 1)) \
        .reduceByKey(add) \
        .sortBy(lambda word: (-word[1], word[0])) \
        .zipWithIndex() \
        .filter(lambda word: word[1] < 10) \
        .map(lambda word: str(word[0][0]) + "\t" + str(word[0][1])) \
        .coalesce(1).saveAsTextFile("data/result_6.tsv")

find_most_frequent_words(file)

sc.stop()

