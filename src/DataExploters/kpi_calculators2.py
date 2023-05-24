from pyspark.sql import SparkSession

# Compute the rental affordability ratio for each neighborhood
# The Affordability Ratio, often defined as the average rent price divided by the average income, is a common indicator of housing affordability.
def kpi_calculators2():
    spark = SparkSession.builder.appName("ReadParquetRDD").getOrCreate()

    # read data from hdfs into rdd
    iris_df = spark.read.parquet("hdfs://10.4.41.44:27000/user/bdm/parquet/iris").select("neighborhoodID", "type", "anyDataTancament")
    income_df = spark.read.parquet("hdfs://10.4.41.44:27000/user/bdm/parquet/income").select("neighborhood_id", "population", "year" )





    # ((2009, 'Q1758503'), ('Q1758503', 49315))
    # ((2007, 'Q1758503'), ('Q1758503', 46595))

    # convert to rdd
    iris_rdd = iris_df.rdd.map(tuple)
    income_rdd = income_df.rdd.map(tuple)
    print(iris_rdd.first()) # ('Q990510', 'INCIDENCIA', '2019')
    print(income_rdd.first()) # ('Q1758503', 49315, 2009)
    # Step 1: Map iris_df to (year, (type, 1)) pairs
    iris_rdd = iris_rdd.map(lambda x: ((x[2], x[0]), (x[1], 1)))
    print(iris_rdd.first()) # (('2019', 'Q990510'), ('INCIDENCIA', 1))
    # Step 2: Reduce by key to get the count of petitions per type and year
    petitions_count_rdd = iris_rdd.reduceByKey(lambda a, b: (a[0], a[1] + b[1])) # [(2022, [('Type A', 50), ('Type B', 70), ('Type C', 30)]),...]
    print(petitions_count_rdd.first()) # (('2019', 'Q3321805'), ('INCIDENCIA', 1743))
    # Step 3: Map income_df to (year, (neighborhood_id, population)) pairs
    income_rdd = income_rdd.map(lambda x: ((x[2], x[0]), (x[0], x[1])))
    print(income_rdd.first()) # ((2009, 'Q1758503'), ('Q1758503', 49315))
    # Step 4: Reduce by key to sum the population per neighborhood and year
    population_sum_rdd = income_rdd.reduceByKey(lambda a, b: (a[0], a[1] + b[1]))
    print(population_sum_rdd.first()) # ((2007, 'Q1758503'), ('Q1758503', 46595))

    # Step 5: Join the two RDDs based on the year
    joined_rdd = petitions_count_rdd.join(population_sum_rdd)

    # Step 6: Compute the ratio by dividing the count of petitions by the population
    ratio_rdd = joined_rdd.map(lambda x: (x[0], (x[1][0][0], x[1][0][1] / x[1][1][1])))

    # Step 7: Collect the results by year
    results_by_year = ratio_rdd.collect()

    print("RESULTS")
    for result in results_by_year:
        print(result)



