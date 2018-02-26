import pyspark
from pyspark import SparkConf, SparkContext

conf = (SparkConf()
         .setMaster("local")
         .setAppName("My app")
         .set("spark.executor.memory", "1g"))
sc = SparkContext()


logFile = "./data/geotweets.tsv"  # Should be some file on your system

file = sc.textFile(logFile)  # Entire file
sample_file = file.sample(False, 0.1, 5)  # Sample file, 10% of original file


def count_number_of_tweets(input_file):
    """
        Return number of lines in file
    """
    # number_of_tweets = sample_file.count()
    # number_of_tweets_total = file.count()
    return input_file.count()


def distinct_usernames(input_file=sample_file):
    username = input_file.map(lambda r: r[6]).distinct()
    return username


distinct_usernames()
sc.stop()

