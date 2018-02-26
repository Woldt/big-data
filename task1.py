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


def get_number_of_tweets(input_file):
    """
        Return number of lines in file
    """
    return input_file.count()


def get_number_of_distinct_usernames(input_file=sample_file):
    """Return number of distinct user names"""
    return input_file.map(lambda tweet:  tweet.split("\t")[USERNAME]).distinct().count()


def get_number_of_distinct_country_names(input_file=sample_file):
    """Return number of distinct country names"""
    return input_file.map(lambda tweet: tweet.split("\t")[COUNTRY_NAME]).distinct().count()


def get_number_of_distinct_places(input_file=sample_file):
    """Return number of distinct places """
    return input_file.map(lambda tweet: tweet.split("\t")[PLACE_NAME]).distinct().count()


def get_number_of_languages(input_file=sample_file):
    """Return number of languages"""
    return input_file.map(lambda tweet: tweet.split("\t")[LANGUAGE]).distinct().count()


def get_min_latitude(input_file=sample_file):
    """Return the lowest latitude"""
    return input_file.map(lambda tweet: float(tweet.split("\t")[LATITUDE])).min()


def get_min_longitude(input_file=sample_file):
    """Return the lowest longitude"""
    return input_file.map(lambda tweet: float(tweet.split("\t")[LONGITUDE])).min()


def get_max_latitude(input_file=sample_file):
    """Return the highest latitude"""
    return input_file.map(lambda tweet: float(tweet.split("\t")[LATITUDE])).max()


def get_max_longitude(input_file=sample_file):
    """Return the highest longitude"""
    return input_file.map(lambda tweet: float(tweet.split("\t")[LONGITUDE])).max()


def get_average_characters(input_file=sample_file):
    """Return the average number of characters in tweets"""
    tweet_lengths = input_file.map(lambda tweet: len(tweet.split("\t")[TWEET_TEXT])).collect()
    return sum(tweet_lengths) / len(tweet_lengths)


def get_average_words(input_file=sample_file):
    """Return the average length of a tweet in terms of words"""
    wordCounts = input_file.map(lambda tweet: tweet.split("\t")[TWEET_TEXT]).map(lambda text: len(text.split(" "))).collect()
    return sum(wordCounts) / len(wordCounts)

print()

sc.stop()

