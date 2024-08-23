import findspark
findspark.init()

from pyspark import SparkConf, SparkContext
import time

def check_prime(n):
    if n <= 1:
        return False
    if n == 2 or n == 3:
        return True
    if n % 2 == 0 or n % 3 == 0:
        return False
    i = 5
    while i * i <= n:
        if n % i == 0 or n % (i + 2) == 0:
            return False
        i += 6
    return True

def main():
    start_time = time.time()

    conf = SparkConf().setAppName("Prime Number").setMaster("local[*]").setSparkHome("E:/spark")
    sc = SparkContext(conf=conf)

    prime_numbers_file = sc.textFile("C:\\Users\\laptop\\Downloads\\numbers_large.txt")

    prime_numbers_file_convert = prime_numbers_file.map(lambda x: int(x)).filter(lambda x: check_prime(x))
    prime_numbers_file_convert_2 = prime_numbers_file_convert.repartition(1)

    print(prime_numbers_file_convert_2.collect())

    end_time = time.time()
    duration = end_time - start_time

    print(duration)

if __name__ == "__main__":
    main()
