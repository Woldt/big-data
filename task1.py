import pyspark
from pyspark import SparkConf, SparkContext

conf = (SparkConf()
         .setMaster("local")
         .setAppName("My app")
         .set("spark.executor.memory", "1g"))
sc = SparkContext()


logFile = "./data/geotweets.tsv"  # Should be some file on your system

file = sc.textFile(logFile)  # Entire file
sample_file = file.sample(False, 0.01, 5)  # Sample file, 10% of original file


def get_number_of_tweets(input_file):
    """
        Return number of lines in file
    """
    return input_file.count()


def get_number_of_distinct_usernames(input_file=sample_file):
    """Return number of distinct user names"""
    return input_file.map(lambda x:  x.split("\t")[6]).distinct().count()

def get_number_of_distinct_places(input_file=sample_file):
    """Return number of distinct user names"""
    return input_file.map(lambda x:  x.split("\t")[4]).distinct().count()

print(get_number_of_distinct_places())
sc.stop()

