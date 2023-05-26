import psycopg2
from pyspark import SparkConf
from pyspark.sql import SparkSession



conf = SparkConf() \
        .set("spark.master", "local") \
        .set("spark.app.name", "Spark Dataframes Tutorial") \
        .set("spark.jars", "../../data/postgresql-42.6.0.jar") \
        .set("spark.driver.extraClassPath", "../../data/postgresql-42.6.0.jar")

spark = SparkSession.builder\
    .appName("storeDimensionTable")\
    .config(conf=conf) \
    .getOrCreate()

# Read income data from HDFS into RDD
neighborhoods = spark.read.parquet("hdfs://10.4.41.44:27000/user/bdm/parquet/neighborhoods")

# Save the DataFrame to PostgreSQL
neighborhoods.write \
    .format("jdbc") \
    .option("url", "jdbc:postgresql://10.4.41.44:5432/bdmp2") \
    .option("dbtable", "neighborhoods") \
    .option("user", "anioldani") \
    .option("password", "anioldani") \
    .mode("overwrite") \
    .save()
