import psycopg2
from pyspark import SparkConf
from pyspark.sql import SparkSession



conf = SparkConf() \
        .set("spark.master", "local") \
        .set("spark.app.name", "datasetGenerator") \
        .set("spark.jars", "../../data/postgresql-42.6.0.jar") \
        .set("spark.driver.extraClassPath", "../../data/postgresql-42.6.0.jar")

spark = SparkSession.builder\
    .config(conf=conf) \
    .getOrCreate()

# project rent data from parquet
rentDataFrame = spark.read.parquet("hdfs://10.4.41.44:27000/user/bdm/parquet/rent")

# select only the columns related to rent and not to the idealista ad
rentDataFrame = rentDataFrame.select("bathrooms", "country", "distance", "district", "exterior", "hasLift", "municipality", "neighborhoodId", "newDevelopment", "price", "newDevelopmentFinished", "priceByArea", "propertyType", "size")

# store data in HDFS in csv format
rentDataFrame.write.csv("hdfs://10.4.41.44:27000/user/bdm/dataset/rent.csv")

