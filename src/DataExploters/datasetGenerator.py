from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StringType, StructField, IntegerType, LongType, BooleanType, DoubleType

def generate_ml_dataset():
    conf = SparkConf() \
        .set("spark.master", "local") \
        .set("spark.app.name", "datasetGenerator") \
        .set("spark.jars", "../../data/postgresql-42.6.0.jar") \
        .set("spark.driver.extraClassPath", "../../data/postgresql-42.6.0.jar")

    spark = SparkSession.builder\
        .config(conf=conf) \
        .getOrCreate()

    rentRDD = spark.read.parquet("hdfs://10.4.41.44:27000/user/bdm/parquet/rent")\
        .rdd\
        .map(tuple)\
        .map(lambda x: (x[-1], x[-4], x[1], x[29], x[2], x[4], x[5], x[6], x[11], x[17], x[19], x[24], x[20], x[25], x[27], x[31]))\
        .map(lambda row: (row[0], row[1:]))

    incomeRDD = spark.read.parquet("hdfs://10.4.41.44:27000/user/bdm/parquet/income")\
        .rdd\
        .map(lambda x: (x[-1], x[1], x[-2]))\
        .map(lambda row: (row[0], row[1:]))

    joinedRDD = rentRDD.join(incomeRDD)\
        .filter(lambda row: row[1][0][0] == row[1][1][0])\
        .map(lambda row: (row[0], *row[1][0], row[1][1][1]))

    schema = StructType([
        StructField("neighborhoodId", StringType(), nullable=False),
        StructField("year", IntegerType(), nullable=False),
        StructField("bathrooms", LongType(), nullable=True),
        StructField("rooms", LongType(), nullable=True),
        StructField("country", StringType(), nullable=True),
        StructField("distance", StringType(), nullable=True),
        StructField("district", StringType(), nullable=True),
        StructField("exterior", BooleanType(), nullable=True),
        StructField("hasLift", BooleanType(), nullable=True),
        StructField("municipality", StringType(), nullable=True),
        StructField("newDevelopment", BooleanType(), nullable=True),
        StructField("price", DoubleType(), nullable=True),
        StructField("newDevelopmentFinished", BooleanType(), nullable=True),
        StructField("priceByArea", DoubleType(), nullable=True),
        StructField("propertyType", StringType(), nullable=True),
        StructField("size", DoubleType(), nullable=True),
        StructField("RFD", DoubleType(), nullable=True)
    ])

    joinedDataFrame = joinedRDD.toDF(schema)

    joinedDataFrame.write.option("header", "true").csv("hdfs://10.4.41.44:27000/user/bdm/dataset/rentdataset.csv", mode="overwrite")

