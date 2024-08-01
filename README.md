# Assignment 25/07/2024

## Exercise 1
1. Given a dataset of 1000 arbitrary numbers in a text file
2. Find all prime numbers in the given dataset
3. Save result under a new text file

### I try 2 kind of generate number. Random and no random
- Length of file :  10000000 numbers

- No random number:
    - Action 
        - Take
            - Time: 
                - CPU times: total: 15.6 ms
                - Wall time: 34.4 s
                ```bash
                %%time
                prime_numbers_rdd.take(10)
                ```
        - saveAsTextFile
            - Time: 
                - CPU times: total: 31.2 ms
                - Wall time: 7.18 s
                ```bash
                %%time
                prime_numbers_rdd.saveAsTextFile(path=prime_numbers_rdd_output_path)
                ```
- Random number:
    - Action 
        - Take
            - Time: 
                - CPU times: total: 0 ns
                - Wall time: 31.6 s
                ```bash
                %%time
                prime_numbers_rdd_random.take(10)
                ```
        - saveAsTextFile
            - Time:
                - CPU times: total: 62.5 ms
                - Wall time: 6.97 s
                ```bash
                %%time
                prime_numbers_rdd_random.saveAsTextFile(path=numbers_random_rdd_output_path)
                ```

## Exercise 2
1. Generate a text file of 10,000 lines, each line contains a pair of (key,value)
2. Calculate the average value for each key
3. Apply GroupByKey() and ReduceByKey() Functions

- GroupByKey:
    - Action:
        - Take:
            - CPU times: total: 31.2 ms
            - Wall time: 1.9 s
            ```bash
            %%time

            grouped_rdd = key_lenValue_rdd.groupByKey()

            sum_count_rdd = grouped_rdd.mapValues(sum_count)

            avg_rdd_groupByKey = sum_count_rdd.mapValues(avg_cal_groupByKey)

            avg`_rdd_groupByKey.take(5)
            ```
- ReduceByKey:
    - Action:
        - Take:
            - CPU times: total: 0 ns
            - Wall time: 1.88 s
            ```bash
            %%time
            sum_count_rdd = key_lenValue_rdd.mapValues(value_and_count) \
                                            .reduceByKey(sum_lenValue)

            avg_rdd_reduceByKey = sum_count_rdd.mapValues(avg_cal_reduceByKey)

            avg_rdd_reduceByKey.take(5)
            ```

## More Conclusion
- With the not really large dataset or small repartition. GroupByKey and ReduceByKey not really different. But the most of time. GroupByKey faster
- With the large dataset and a lot of partition (I test with partition >= 12). ReduceByKey is faster than GroupByKey.
- I tested with increase number of keys and decrease it. So that affect the performance too.