import pyspark
from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext, SparkSession

from pyspark.sql.types import IntegerType, FloatType

conf = (SparkConf()
         .setMaster("local")
         .setAppName("My app")
         .set("spark.executor.memory", "1g"))
sc = SparkContext()

spark = SparkSession.builder.getOrCreate()  # Create spark session

logFile = "../data/geotweets.tsv"  # Should be some file on your system containing geotweets

# Creates data frame with tab as separation new line as quotation, with columnnames equal to columns
# Quotation on new line is done because of the converter merges some of the "real" quatation marks, which results in merged lines
data_frame = spark.read.option("sep", "\t").option("quote", "\n").csv(logFile).toDF('UTC_TIME', 'COUNTRY_NAME', 'COUNTRY_CODE', 'PLACE_TYPE', 'PLACE_NAME', 'LANGUAGE', 'USERNAME', 'USER_SCREEN_NAME', 'TIMEZONE_OFFSET', 'NUMBER_OF_FRIENDS', 'TWEET_TEXT', 'LATITUDE', 'LONGITUDE')
data_frame=data_frame.withColumn("LATITUDE", data_frame["LATITUDE"].cast(FloatType()))  # Casts latitude to float, so it can be calculated on later
data_frame=data_frame.withColumn("LONGITUDE", data_frame["LONGITUDE"].cast(FloatType()))  # Casts longitude to float, so it can be calculated on later

data_frame.createOrReplaceTempView("tweets")  # Name the data table 'tweets'


def get_number_of_tweets():
    """ Returns data frame containing number of tweets
    Returns:
        DataFrame -- DataFrame containing number of tweets
    """
    return spark.sql("SELECT COUNT(*) as NumberOfTweets FROM tweets")
get_number_of_tweets().show()


def get_distinct_usernames():
    """Returns data frame containg distinct usernames
    
    Returns:
        DataFrame -- DataFrame containing distinct usernames
    """
    return spark.sql("SELECT COUNT(DISTINCT USERNAME) as DistinctUsernames FROM tweets")
get_distinct_usernames().show()


def get_distinct_country_names():
    """Returns data frame containg distinct country names
    
    Returns:
        DataFrame -- DataFrame containing distinct country names
    """
    return spark.sql("SELECT COUNT(DISTINCT COUNTRY_NAME) as DistinctCountryNames FROM tweets")
get_distinct_country_names().show()


def get_distinct_place_names():
    """Returns data frame containg distinct place names
    
    Returns:
        DataFrame -- DataFrame containing distinct place names
    """
    return spark.sql("SELECT COUNT(DISTINCT PLACE_NAME) as DistinctPlaceNames FROM tweets")
get_distinct_place_names().show()


def get_distinct_languages():
    """Returns data frame containg distinct languages
    
    Returns:
        DataFrame -- DataFrame containing distinct languages
    """
    return spark.sql("SELECT COUNT(DISTINCT LANGUAGE) as DistictLanguages FROM tweets")
get_distinct_languages().show()


def get_min_lat_long():
    """Returns data frame containing minimum latitude and minumum longitude
    
    Returns:
        DataFrame -- DataFrame containing minimum latitude and minimum longitude
    """

    return spark.sql("SELECT MIN(LATITUDE), MIN(LONGITUDE) FROM tweets")
get_min_lat_long().show()


def get_max_lat_long():
    """Returns data frame containing maximum latitude and maximum longitude
    
    Returns:
        DataFrame -- DataFrame containing maximum latitude and maximum longitude
    """
    return spark.sql("SELECT MAX(LATITUDE), MAX(LONGITUDE) FROM tweets")
get_max_lat_long().show()


