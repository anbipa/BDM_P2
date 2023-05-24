from incomeFormatter import load_lookup_tables
from irisFormatter import load_lookup_table
from rentFormatter import load_rent_lookup_tables
import pyspark
from pyspark.sql import SparkSession

def prepareRDD(rdd):
    #We do not take the lower case reconciled neighborhoods/districts to join with Iris lookup
    newRDD = rdd.map(lambda x: (x[0], x[1], x[3], x[4], x[5], x[7]))
    return newRDD

def extract_first(result_iterable):
    return next(iter(result_iterable))

def getDistinctNeighborhoods(lookupRDD1, lookupRDD2, lookupRDD3):
    reconciledRDD = lookupRDD1.union(lookupRDD2).union(lookupRDD3)\
        .distinct() \
        .map(lambda x: (x[0], x)) \
        .groupByKey() \
        .map(lambda x: (extract_first(x[1])))\
        .map(lambda x: (x[0], x[2], x[3], x[5])) #take only neighborhood_id, neighborhood_reconciled, district_id, district_reconciled
    return reconciledRDD

def save_to_parquet(rdd):
    # Create a SparkSession
    spark = SparkSession.builder \
        .appName("RDD to Parquet") \
        .getOrCreate()
    # Convert RDD to DataFrame
    df = spark.createDataFrame(rdd, ["neighborhood_id", "neighborhood_reconciled", "district_id", "district_reconciled"])
    # Write DataFrame to Parquet file in HDFS
    df.write.parquet("hdfs://10.4.41.44:27000/user/bdm/parquet/neighborhoods", mode="overwrite")
    # Stop the SparkSession
    spark.stop()

if __name__ == "__main__":
    sc = pyspark.SparkContext.getOrCreate()
    lookupIncomeRDD = prepareRDD(load_lookup_tables(sc))
    lookupIrisRDD = load_lookup_table(sc)
    lookupRentRDD = prepareRDD(load_rent_lookup_tables(sc))
    reconciledLookupRDD = getDistinctNeighborhoods(lookupIncomeRDD, lookupIrisRDD, lookupRentRDD)
    save_to_parquet(reconciledLookupRDD)


