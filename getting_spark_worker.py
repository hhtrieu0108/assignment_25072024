from pyspark.sql import SparkSession
from pyspark import SparkConf
conf = SparkConf().set("spark.driver.host", "10.88.51.141").set("spark.driver.port","7077")

spark = SparkSession.builder.appName("Test").config(conf=conf).getOrCreate()

sc = spark._jsc.sc() 
n_workers =  len([executor.host() for executor in sc.statusTracker().getExecutorInfos() ]) -1

print(n_workers)

sc = spark._jsc.sc() 

result1 = sc.getExecutorMemoryStatus().keys()

result2 = len([executor.host() for executor in sc.statusTracker().getExecutorInfos() ]) -1

print(result1, end ='\n')
print(result2)