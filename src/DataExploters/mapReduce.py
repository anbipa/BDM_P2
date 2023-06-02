import psycopg2
from pyspark import SparkConf
from pyspark.sql import SparkSession

def insert_row(row):
    conn = psycopg2.connect(
        host="10.4.41.44",
        database="bdmp2",
        user="anioldani",
        password="anioldani"
    )

    # Establish a connection to the PostgreSQL database
    cursor = conn.cursor()

    # Extract the values from the row
    neighborhood, year, avg_income, avg_rent = row

    # Prepare the SQL statement for insertion
    sql = "INSERT INTO AVGIND (neighborhood, year, avg_income, avg_rent) VALUES (%s, %s, %s, %s)"
    values = (neighborhood, year, avg_income, avg_rent)

    # Execute the SQL statement
    cursor.execute(sql, values)

    # Commit the changes and close the connection
    conn.commit()
    cursor.close()
    conn.close()


class NeighborhoodYearPartitioner:
    def __init__(self, num_partitions):
        self.num_partitions = num_partitions

    def __call__(self, key):
        neighborhood, year = key
        # Hash the combined key (neighborhood, year) to determine the partition
        return hash((neighborhood, year)) % self.num_partitions


def execute_map_reducers():

    num_partitions = 10

    conf = SparkConf() \
            .set("spark.master", "local") \
            .set("spark.app.name", "Spark Dataframes Tutorial") \
            .set("spark.jars", "../../data/postgresql-42.6.0.jar") \
            .set("spark.driver.extraClassPath", "../../data/postgresql-42.6.0.jar")\
            .set("spark.default.parallelism", num_partitions)

    spark = SparkSession.builder\
        .appName("mapReduce")\
        .config(conf=conf) \
        .getOrCreate()

    # Read income data from HDFS into RDD
    income_rdd = spark.read.parquet("hdfs://10.4.41.44:27000/user/bdm/parquet/income") \
        .select("neighborhood_id", "year", "RFD") \
        .rdd\
        .map(tuple)\
        .cache()

    # Read rental data from HDFS into RDD
    rents_rdd = spark.read.parquet("hdfs://10.4.41.44:27000/user/bdm/parquet/rent") \
        .select("neighborhoodId", "year", "price", "day", "month") \
        .rdd\
        .map(tuple)\
        .cache()

    iris_rdd = spark.read.parquet("hdfs://10.4.41.44:27000/user/bdm/parquet/iris")\
        .select("neighborhoodID", "type", "diaDataAlta", "mesDataAlta", "anyDataAlta")\
        .rdd\
        .map(tuple)



    ################################### AVG INDICATORS ########################################

    # Map the income RDD to key-value pairs
    income_mapped_rdd = income_rdd.map(lambda row: ((row[0], row[1]), (row[2], 1)))

    # Map the rental RDD to key-value pairs
    rents_mapped_rdd = rents_rdd.map(lambda row: ((row[0], row[1]), (row[2], 1)))

    # here rdd's are transformed to key-value rdd's
    # very important to optimize the partitioning in this part because no data is shuffled

    income_mapped_rdd = income_mapped_rdd.partitionBy(num_partitions, NeighborhoodYearPartitioner(num_partitions))
    rents_mapped_rdd = rents_mapped_rdd.partitionBy(num_partitions, NeighborhoodYearPartitioner(num_partitions))


    # Reduce by key to compute total income and count per neighborhood and year
    income_totals_rdd = income_mapped_rdd.reduceByKey(lambda x, y: (x[0] + y[0], x[1] + y[1]))

    # Reduce by key to compute total rent price and count per neighborhood and year
    rents_totals_rdd = rents_mapped_rdd.reduceByKey(lambda x, y: (x[0] + y[0], x[1] + y[1]))

    # Compute average income per neighborhood and year
    average_income_rdd = income_totals_rdd.mapValues(lambda x: x[0] / x[1])

    # Compute average rent per neighborhood and year
    average_rent_rdd = rents_totals_rdd.mapValues(lambda x: x[0] / x[1])

    # Join average income and average rent RDDs by key (neighborhood and year)
    joined_rdd = average_income_rdd.join(average_rent_rdd)

    # Convert the joined RDD to a new RDD with the required structure
    kpi_rdd = joined_rdd.map(lambda row: (*row[0], *row[1]))

    # Save the RDD to PostgreSQL
    result_df = kpi_rdd.toDF(["neighborhood", "year", "avg_income", "avg_rent"])
    print(result_df.show())

    # Save the DataFrame to PostgreSQL
    result_df.write \
        .format("jdbc") \
        .option("url", "jdbc:postgresql://10.4.41.44:5432/bdmp2") \
        .option("dbtable", "AVGIND") \
        .option("user", "anioldani") \
        .option("password", "anioldani") \
        .mode("overwrite") \
        .save()



    ################################### COUNT INDICATORS ########################################

    housing_rdd = rents_rdd.map(lambda x: ((x[3], x[4], x[1], x[0]), 1))\
            .reduceByKey(lambda a, b: a+b)


    # Get counting of incidences per neighborhood and day
    incidences_rdd = iris_rdd.filter(lambda x: x[1]=='INCIDENCIA')\
            .map(lambda x: ((int(x[2]), int(x[3]), int(x[4]), x[0]),  1))\
            .reduceByKey(lambda a, b: (a + b))

    #Join them in one single rdd
    incidencesAndHousing = incidences_rdd.leftOuterJoin(housing_rdd)\
            .map(lambda x: (*x[0], *x[1])) # leftOuterJoins produces NULLS because only exist data for 2020 and 2021 in renting dataset. We can put join to just have joins


    result_df = incidencesAndHousing.toDF(["day", "month", "year", "neighborhood", "count_incidences", "count_housing"],sampleRatio=1.0)

    result_df.write \
            .format("jdbc") \
            .option("url", "jdbc:postgresql://10.4.41.44:5432/bdmp2") \
            .option("dbtable", "LISTINGIND") \
            .option("user", "anioldani") \
            .option("password", "anioldani") \
            .mode("overwrite") \
            .save()

    ################################### POPULATION ########################################

    income_df = spark.read.parquet("hdfs://10.4.41.44:27000/user/bdm/parquet/income").select("neighborhood_id", "year", "population")#.rdd.map(tuple)

    income_df.write \
            .format("jdbc") \
            .option("url", "jdbc:postgresql://10.4.41.44:5432/bdmp2") \
            .option("dbtable", "POPULATION") \
            .option("user", "anioldani") \
            .option("password", "anioldani") \
            .mode("overwrite") \
            .save()


    ################################### NEIGHBORHOOD DIMENSION ########################################

    # Read income data from HDFS into RDD
    neighborhoods = spark.read.parquet("hdfs://10.4.41.44:27000/user/bdm/parquet/neighborhoods")

    # Save the DataFrame to PostgreSQL
    neighborhoods.write \
        .format("jdbc") \
        .option("url", "jdbc:postgresql://10.4.41.44:5432/bdmp2") \
        .option("dbtable", "NEIGHBORHOODS") \
        .option("user", "anioldani") \
        .option("password", "anioldani") \
        .mode("overwrite") \
        .save()


