from pyspark import SparkConf, SparkContext, 
conf = (SparkConf()
         .setMaster("local")
         .setAppName("My app")
         .set("spark.executor.memory", "1g"))
sc = SparkContext(conf = conf)


logFile = "./data/test.txt"  # Should be some file on your system

file = sc.textFile(logFile)

lineLiength = file.map(lambda s: len(s))

sc.stop()

