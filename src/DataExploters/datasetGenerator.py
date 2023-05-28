from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StringType, StructField, IntegerType, LongType, BooleanType, DoubleType

conf = SparkConf() \
        .set("spark.master", "local") \
        .set("spark.app.name", "datasetGenerator") \
        .set("spark.jars", "../../data/postgresql-42.6.0.jar") \
        .set("spark.driver.extraClassPath", "../../data/postgresql-42.6.0.jar")

spark = SparkSession.builder\
    .config(conf=conf) \
    .getOrCreate()

# project rent data from parquet
rentDataFrame = spark.read.parquet("hdfs://10.4.41.44:27000/user/bdm/parquet/rent")\
        .select("neighborhoodId", "year", "bathrooms", "rooms", "country", "distance", "district", "exterior", "hasLift", "municipality", "newDevelopment", "price", "newDevelopmentFinished", "priceByArea", "propertyType", "size")

rentRDD = rentDataFrame.rdd.map(tuple)\
        .map(lambda row: (row[0], row[1:]))

print(rentRDD.take(5))
print("rows: ", rentRDD.count())

incomeDataFrame = spark.read.parquet("hdfs://10.4.41.44:27000/user/bdm/parquet/income")\
        .select("neighborhood_id", "year", "RFD")

incomeRDD = incomeDataFrame.rdd.map(tuple)\
        .map(lambda row: (row[0], row[1:]))

print(incomeRDD.take(5))

# join rent and income data
# Join rent and income data
joinedRDD = rentRDD.join(incomeRDD)\
        .filter(lambda row: row[1][0][0] == row[1][1][0])\
        .map(lambda row: (row[0], *row[1][0], row[1][1][1]))

print(joinedRDD.take(5))
print("rows: ", joinedRDD.count())


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

# Convert RDD to DataFrame
joinedDataFrame = joinedRDD.toDF(schema)

print(joinedDataFrame.show())

# store data in HDFS in csv format
rentDataFrame.write.option("header", "true").csv("hdfs://10.4.41.44:27000/user/bdm/dataset/rentdataset.csv", mode="overwrite")

