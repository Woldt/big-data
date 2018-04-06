from pyspark import SparkConf, SparkContext
from operator import add
import argparse
parser = argparse.ArgumentParser()
parser.add_argument("-training")
parser.add_argument("-input")
parser.add_argument("-output")
args = parser.parse_args()

conf = (SparkConf()
         .setMaster("local")
         .setAppName("My app")
         .set("spark.executor.memory", "1g"))
sc = SparkContext()


# logFile = "../data/geotweets.tsv"  # Should be some file on your system
logFile = args.training  # Should be some file on your system
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


def naive_bayes(T, T_place, word_list, tweet_words):
    word_dict = dict(word_list)
    probability = abs(T_place)/abs(T)
    for word in tweet_words:
        if word in word_dict:
            probability *= abs(word_dict[word])/abs(T_place)
        else:
            probability = 0
    return probability

# , input="../data/input1.txt", output="data/result.tsv"
def classify(training=sample_file):
    stopwords = stopwordFile.map(lambda word: word).collect()
    input = [word for word in open(args.input, "r").read().lower().split(" ") if word not in stopwords and len(word) >= 2]
    total_tweets = total_number_of_tweets(training)
    cities = dict(tweets_per_city(training))
    training \
        .map(lambda tweet: (tweet.split("\t")[PLACE_NAME], list({word for word in tweet.split("\t")[TWEET_TEXT].lower().split(" ") if word not in stopwords and len(word) >= 2}))) \
        .reduceByKey(lambda wordlist_x, wordlist_y: wordlist_x + wordlist_y) \
        .flatMapValues(lambda city_words_key: city_words_key) \
        .map(lambda key: (key, 1)) \
        .reduceByKey(add) \
        .map(lambda place: ((place[0][0], cities[place[0][0]]), (place[0][1], place[1]))) \
        .groupByKey().mapValues(list) \
        .map(lambda place : (naive_bayes(total_tweets, place[0][1], place[1], input), place[0][0])) \
        .groupByKey().mapValues(list) \
        .sortBy(lambda place: -place[0]) \
        .filter(lambda place: place[0] > 0) \
        .map(lambda places: (".".join(places[1]).replace(".", "\t"), places[0])) \
        .zipWithIndex() \
        .filter(lambda word: word[1] < 1) \
        .map(lambda place: place[0][0] + "\t" + str(place[0][1])) \
        .coalesce(1) \
        .saveAsTextFile(args.output)


classify(file)