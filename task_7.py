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


def most_frequent_cities(input_file=sample_file):
    """ Returns five most frequent cities from the US from {input_file}
    
    Keyword Arguments:
         input_file {Spark RDD object} -- Spark rdd object based on TSV file (default: {sample_file})
    
    Returns:
        LIST -- returns list of five cities as strings
    """
    return input_file\
        .map(lambda tweet: (tweet.split("\t")[PLACE_NAME], tweet.split("\t")[PLACE_TYPE], tweet.split("\t")[COUNTRY_CODE]))\
        .filter(lambda place: place[1] == "city" and place[2] == "US") \
        .map(lambda city: (city[0], 1))\
        .reduceByKey(add)\
        .sortBy(lambda city: (-city[1], city[0])) \
        .map(lambda city: city[0]).take(5)


def find_most_frequent_words_per_city(input_file=sample_file):
    """Creates a file {result_7.tsv} containg five most frequent cities and the 10 most
        frequent words for current city, with count for each word from {input_file}

        FORMAT -. {City} {Word} {Count} {Word}  {Count} ...
    
    Keyword Arguments:
         input_file {Spark RDD object} -- Spark rdd object based on TSV file (default: {sample_file})
    """

    cities = most_frequent_cities(input_file)
    stopwords = stopwordFile.map(lambda word: word).collect()
    input_file \
        .map(lambda tweet: (tweet.split("\t")[PLACE_NAME], [word for word in tweet.split("\t")[TWEET_TEXT].lower().split(" ") if word not in stopwords and len(word) >= 2])) \
        .filter(lambda tweet: (tweet[0] in cities)) \
        .reduceByKey(lambda wordlist_x, wordlist_y: wordlist_x + wordlist_y) \
        .flatMapValues(lambda city_words_key: city_words_key) \
        .map(lambda key: (key, 1)) \
        .reduceByKey(add) \
        .map(lambda city: (city[0][0], (city[0][1], city[1]))) \
        .sortBy(lambda word: (word[0], -word[1][1], word[1][0])) \
        .groupByKey().mapValues(list) \
        .map(lambda x : x[0] \
                        + "\t" + x[1][0][0] + "\t" + str(x[1][0][1])\
                        + "\t" + x[1][1][0] + "\t" + str(x[1][1][1])\
                        + "\t" + x[1][2][0] + "\t" + str(x[1][2][1])\
                        + "\t" + x[1][3][0] + "\t" + str(x[1][3][1])\
                        + "\t" + x[1][4][0] + "\t" + str(x[1][4][1])\
                        + "\t" + x[1][5][0] + "\t" + str(x[1][5][1])\
                        + "\t" + x[1][6][0] + "\t" + str(x[1][6][1])\
                        + "\t" + x[1][7][0] + "\t" + str(x[1][7][1])\
                        + "\t" + x[1][8][0] + "\t" + str(x[1][8][1])\
                        + "\t" + x[1][9][0] + "\t" + str(x[1][9][1])) \
        .coalesce(1).saveAsTextFile("data/result_7.tsv")


find_most_frequent_words_per_city()

sc.stop()

