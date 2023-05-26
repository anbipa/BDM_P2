from pyspark import SparkConf
from pyspark.sql import SparkSession

conf = SparkConf() \
        .set("spark.master", "local") \
        .set("spark.app.name", "Spark Dataframes Tutorial") \
        .set("spark.jars", "../../data/postgresql-42.6.0.jar") \
        .set("spark.driver.extraClassPath", "../../data/postgresql-42.6.0.jar")

spark = SparkSession.builder\
    .appName("ComputeKPIs")\
    .config(conf=conf) \
    .getOrCreate()

# read data from hdfs into rdd
iris_rdd = spark.read.parquet("hdfs://10.4.41.44:27000/user/bdm/parquet/iris").select("neighborhoodID", "type", "diaDataAlta", "mesDataAlta", "anyDataAlta").rdd.map(tuple)
income_df = spark.read.parquet("hdfs://10.4.41.44:27000/user/bdm/parquet/income2").select("neighborhood_id", "year", "population")#.rdd.map(tuple)
rents_rdd = spark.read.parquet("hdfs://10.4.41.44:27000/user/bdm/parquet/rent").select("day", "month", "year", "neighborhoodId").rdd.map(tuple)

# Get counting of housing per neighborhood and year
housing_rdd = rents_rdd.map(lambda x: (x, 1)).reduceByKey(lambda a, b: a+b)

# Get counting of incidences per neighborhood and day
incidences_rdd = iris_rdd.filter(lambda x: x[1]=='INCIDENCIA').map(lambda x: ((int(x[2]), int(x[3]), int(x[4]), x[0]),  1)).reduceByKey(lambda a, b: (a + b))

#Join them in one single rdd
incidencesAndHousing = incidences_rdd.leftOuterJoin(housing_rdd).map(lambda x: (*x[0], *x[1])) # leftOuterJoins produces NULLS because only exist data for 2020 and 2021 in renting dataset. We can put join to just have joins

result_df = incidencesAndHousing.toDF(["day", "month", "year", "neighborhood", "count_incidences", "count_housing"])
result_df.write \
        .format("jdbc") \
        .option("url", "jdbc:postgresql://10.4.41.44:5432/bdmp2") \
        .option("dbtable", "kpi2") \
        .option("user", "anioldani") \
        .option("password", "anioldani") \
        .mode("overwrite") \
        .save()
income_df.write \
        .format("jdbc") \
        .option("url", "jdbc:postgresql://10.4.41.44:5432/bdmp2") \
        .option("dbtable", "population") \
        .option("user", "anioldani") \
        .option("password", "anioldani") \
        .mode("overwrite") \
        .save()

print("DONEEE")

# #NO FUNCIONA NO SE PERQUE :( NO FALLA PERO VA MOLT LENT I NO ACABA MAI
# incidencesAndHousing = incidencesAndHousing.map(lambda x: ((x[2], x[3]), x[0], x[1], x[4], x[5]))
# print(incidencesAndHousing.first())
# income = income_df.rdd.map(tuple).map(lambda x: ((x[1],x[0]), x[2]))
# print(income.first())
# r = incidencesAndHousing.join(income)#.map(lambda x: ((x[0][2], x[0][3]),(x[0][0], x[0][1], x[1][0],x[1][1])))# ((year, neighborhood),(day, month, incidenceCounts, housingCount))
# print(r.take(3))







