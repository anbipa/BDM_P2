from pyspark.sql import SparkSession

# Compute the rental affordability ratio for each neighborhood
# The Affordability Ratio, often defined as the average rent price divided by the average income, is a common indicator of housing affordability.

spark = SparkSession.builder.appName("ReadParquetRDD").getOrCreate()

# read data from hdfs into rdd
income_df = spark.read.parquet("hdfs://10.4.41.44:27000/user/bdm/parquet/income").select("neighborhood_id", "RFD")
rents_df = spark.read.parquet("hdfs://10.4.41.44:27000/user/bdm/parquet/rent").select("neighborhoodId", "price")


# convert to rdd
income_rdd = income_df.rdd.map(tuple)
rents_rdd = rents_df.rdd.map(tuple)

# First the income and rents RDDs are key-value pair format where key is the neighborhood_id
# and value is the RFD (for income) or price (for rents)


# Then, use the reduceByKey transformation to compute the total RFD/income and count for each neighborhood
income_totals_rdd = income_rdd.aggregateByKey((0, 0),
                                               (lambda x, value: (x[0] + value, x[1] + 1)),
                                               (lambda x, y: (x[0] + y[0], x[1] + y[1])))

# Do the same for the rents data
rents_totals_rdd = rents_rdd.aggregateByKey((0, 0),
                                               (lambda x, value: (x[0] + value, x[1] + 1)),
                                               (lambda x, y: (x[0] + y[0], x[1] + y[1])))

# Compute the average income and rent price for each neighborhood
average_income_per_neighborhood_rdd = income_totals_rdd.mapValues(lambda x: x[0] / x[1])
average_rent_per_neighborhood_rdd = rents_totals_rdd.mapValues(lambda x: x[0] / x[1])

# Join the two RDDs
combined_rdd = average_income_per_neighborhood_rdd.join(average_rent_per_neighborhood_rdd)

# Compute the rental affordability ratio for each neighborhood
rental_affordability_ratio_rdd = combined_rdd.mapValues(lambda x: x[1] / x[0])

average_income_per_neighborhood_rdd = income_totals_rdd.mapValues(lambda x: x[0] / x[1])
average_rent_per_neighborhood_rdd = rents_totals_rdd.mapValues(lambda x: x[0] / x[1])

# Join the two RDDs
combined_rdd = average_income_per_neighborhood_rdd.join(average_rent_per_neighborhood_rdd)

# Compute the rental affordability ratio and average rental price per neighborhood
kpi_rdd = combined_rdd.mapValues(lambda x: (x[1] / x[0], x[1]))

# Sort by rental affordability ratio
kpi_rdd = kpi_rdd.sortBy(lambda x: x[1][0], ascending=False)

# Collect results
results = kpi_rdd.collect()

# Print the results
print(results)


