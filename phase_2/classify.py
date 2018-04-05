import pyspark
from pyspark import SparkConf, SparkContext
from operator import add

conf = (SparkConf()
         .setMaster("local")
         .setAppName("My app")
         .set("spark.executor.memory", "1g"))
sc = SparkContext()


logFile = "../data/geotweets.tsv"  # Should be some file on your system
stopwords = "../data/stop_words.txt"  # Should be some file on your system

file = sc.textFile(logFile)  # Entire file as RDD object
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


def total_number_of_tweets(input_file=sample_file):
    return input_file.count()


def tweets_per_city(input_file=sample_file):
    return input_file \
        .map(lambda tweet: (tweet.split("\t")[PLACE_NAME], 1)) \
        .reduceByKey(add) \
        .collect()


def classify(input_file=sample_file):
    cities = dict(tweets_per_city())
    stopwords = stopwordFile.map(lambda word: word).collect()
    input_file \
        .map(lambda tweet: (tweet.split("\t")[PLACE_NAME], list({word for word in tweet.split("\t")[TWEET_TEXT].lower().split(" ") if word not in stopwords and len(word) >= 2}))) \
        .reduceByKey(lambda wordlist_x, wordlist_y: wordlist_x + wordlist_y) \
        .flatMapValues(lambda city_words_key: city_words_key) \
        .map(lambda key: (key, 1)) \
        .reduceByKey(add) \
        .map(lambda city: ((city[0][0], cities[city[0][0]]), (city[0][1], city[1]))) \
        .groupByKey().mapValues(list) \
        .coalesce(1) \
        .saveAsTextFile("data/result4.tsv")



classify()