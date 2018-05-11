import pyspark
from pyspark import SparkConf, SparkContext
from operator import add

conf = (SparkConf()
         .setMaster("local")
         .setAppName("My app")
         .set("spark.executor.memory", "1g"))
sc = SparkContext()


logFile = "./countries.tsv"  # Should be some file on your system

data = sc.textFile(logFile)  # Entire file as RRD object
sample_file = data.sample(False, 0.01, 5)  # Sample file, 10% of original file


#B)	
data2 = data.map(lambda line: (line.split("\t")))

#C)
data3 = data.map(lambda line: (line.split('\t')[0], line.split('\t')[1].split(',')))

#D)
data4 = sum(data3.map(lambda line: len(line[1])).collect())

#D)
data4 = sum(data3.map(lambda line: len(line[1])).collect())
data4 = data3.flatMap(lambda line: line[1]).count()

#E)
data4 = data3.flatMap(lambda line: line[1]).distinct().count()

#F)
	
data5 = data3.map(lambda c: (c[0], set(c[1]).count()))