import psycopg2
from pyspark import SparkConf
from pyspark.sql import SparkSession


# Function to save each partition of the DataFrame to PostgreSQL
def save_partition(iter):
    # Establish a connection to PostgreSQL
    conn = psycopg2.connect(
        host="database-bdmp2.cjhqgymunwij.eu-north-1.rds.amazonaws.com",
        port="5432",
        dbname="postgres",
        user="anioldani",
        password="anioldaniP2"
    )
    cursor = conn.cursor()

    # Drop the table if it exists
    drop_table_statement = "DROP TABLE IF EXISTS kpi1"
    cursor.execute(drop_table_statement)

    # Create table if not exists
    cursor.execute("CREATE TABLE IF NOT EXISTS kpi1 (neighborhood VARCHAR(255), year INT, avg_income FLOAT, avg_rent FLOAT)")

    # Iterate over the partition and insert rows into the PostgreSQL table
    for row in iter:
        neighborhood = row.neighborhood
        year = row.year
        avg_income = row.avg_income
        avg_rent = row.avg_rent


        # Construct the INSERT statement
        insert_statement = f"INSERT INTO kpi1 (neighborhood, year, avg_income, avg_rent) VALUES ('{neighborhood}', {year}, {avg_income}, {avg_rent})"

        # Execute the INSERT statement
        cursor.execute(insert_statement)

    # Commit the changes and close the cursor and connection
    conn.commit()
    cursor.close()
    conn.close()

conf = SparkConf() \
        .set("spark.master", "local") \
        .set("spark.app.name", "Spark Dataframes Tutorial") \
        .set("spark.jars", "../../data/postgresql-42.6.0.jar") \
        .set("spark.driver.extraClassPath", "../../data/postgresql-42.6.0.jar")

spark = SparkSession.builder\
    .appName("ComputeKPIs")\
    .config(conf=conf) \
    .getOrCreate()

# Read income data from HDFS into RDD
income_rdd = spark.read.parquet("hdfs://10.4.41.44:27000/user/bdm/parquet/income") \
    .select("neighborhood_id", "RFD", "year") \
    .rdd

# Read rental data from HDFS into RDD
rents_rdd = spark.read.parquet("hdfs://10.4.41.44:27000/user/bdm/parquet/rent") \
    .select("neighborhoodId", "price", "year") \
    .rdd

# Map the income RDD to key-value pairs
income_mapped_rdd = income_rdd.map(lambda row: ((row.neighborhood_id, row.year), (row.RFD, 1)))

# Map the rental RDD to key-value pairs
rents_mapped_rdd = rents_rdd.map(lambda row: ((row.neighborhoodId, row.year), (row.price, 1)))

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

# Convert the RDD to DataFrame
result_df = kpi_rdd.toDF(["neighborhood", "year", "avg_income", "avg_rent"])
print(result_df.show())

# Save the DataFrame to PostgreSQL
result_df.write \
    .format("jdbc") \
    .option("url", "jdbc:postgresql://database-bdmp2.cjhqgymunwij.eu-north-1.rds.amazonaws.com:5432/postgres") \
    .option("dbtable", "kpi1") \
    .option("user", "anioldani") \
    .option("password", "anioldaniP2") \
    .mode("overwrite") \
    .save()
