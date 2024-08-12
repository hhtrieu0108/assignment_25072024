from pyspark.sql import SparkSession
from pyspark import SparkConf, SparkContext
from pyspark.sql import functions as func
import time

# conf = SparkConf().set("spark.driver.host", "10.88.51.141").set("spark.driver.port","7077").set("spark.executor.cores","2").set("spark.executor.memory","2g")
conf = SparkConf().set("spark.executor.cores","2")\
                .set("spark.executor.memory","2g")

sc = SparkContext(appName="Spark Repartition Testing",conf=conf)

from math import sqrt,ceil
def is_prime(n):
    if n <= 1:
        return False
    for i in range(2,ceil(sqrt(n))+1):
        if n%i == 0:
            return False
    return True

start_time = time.time()
numbers_file = "input_data/numbers.txt"
prime_numbers_rdd_output_path = "output_data/prime_numbers_rdd.txt"
numbers_rdd = sc.textFile(numbers_file)
numbers_rdd = numbers_rdd.repartition(2) # 6 DANG DEP NHAT
numbers_rdd = numbers_rdd.map(lambda x: int(x))
prime_numbers_rdd = numbers_rdd.filter(is_prime)
print(prime_numbers_rdd.collect())

end_time = time.time()
interval = end_time - start_time
print(interval)