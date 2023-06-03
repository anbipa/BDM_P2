from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StringType, StructField, IntegerType, LongType, BooleanType, DoubleType

def generate_ml_dataset():
    conf = SparkConf() \
        .set("spark.master", "local") \
        .set("spark.app.name", "datasetGenerator2") \

    spark = SparkSession.builder\
        .config(conf=conf) \
        .getOrCreate()

    rentRDD = spark.read.parquet("hdfs://10.4.41.44:27000/user/bdm/parquet/rent")\
        .select("neighborhoodId", "bathrooms", "rooms", "exterior", "hasLift", "newDevelopment", "price", "size")\
        .rdd\
        .map(tuple)\
        .map(lambda row: (row[0], row[1:]))

    incomeRDD = spark.read.parquet("hdfs://10.4.41.44:27000/user/bdm/parquet/income")\
        .select("neighborhood_id", "RFD")\
        .rdd\
        .map(tuple)\
        .map(lambda row: (row[0], (row[1], 1)))\
        .reduceByKey(lambda x, y: (x[0] + y[0], x[1] + y[1]))\
        .mapValues(lambda row: row[0] / row[1])\

    joinedRDD = rentRDD.join(incomeRDD)\
        .map(lambda row: (*row[1][0], row[1][1]))

    print(joinedRDD.take(10))

    schema = StructType([
        StructField("bathrooms", LongType(), nullable=True),
        StructField("rooms", LongType(), nullable=True),
        StructField("exterior", BooleanType(), nullable=True),
        StructField("hasLift", BooleanType(), nullable=True),
        StructField("newDevelopment", BooleanType(), nullable=True),
        StructField("price", DoubleType(), nullable=True),
        StructField("size", DoubleType(), nullable=True),
        StructField("RFD", DoubleType(), nullable=True)
    ])

    joinedDataFrame = joinedRDD.toDF(schema)

    joinedDataFrame.write.option("header", "true").csv("hdfs://10.4.41.44:27000/user/bdm/dataset/rentdataset.csv", mode="overwrite")

generate_ml_dataset()