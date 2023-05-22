import os

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, LongType, BooleanType, DoubleType

# Create a SparkSession
spark = SparkSession.builder.appName("Schema Validation").getOrCreate()

# Define the Parquet file path
parquet_path = "./idealista"

# Define your expected schema
expected_schema = StructType([StructField('address', StringType(), True), StructField('bathrooms', LongType(), True), StructField('country', StringType(), True), StructField('detailedType', StructType([StructField('subTypology', StringType(), True), StructField('typology', StringType(), True)]), True), StructField('distance', StringType(), True), StructField('district', StringType(), True), StructField('exterior', BooleanType(), True), StructField('externalReference', StringType(), True), StructField('floor', StringType(), True), StructField('has360', BooleanType(), True), StructField('has3DTour', BooleanType(), True), StructField('hasLift', BooleanType(), True), StructField('hasPlan', BooleanType(), True), StructField('hasStaging', BooleanType(), True), StructField('hasVideo', BooleanType(), True), StructField('latitude', DoubleType(), True), StructField('longitude', DoubleType(), True), StructField('municipality', StringType(), True), StructField('neighborhood', StringType(), True), StructField('newDevelopment', BooleanType(), True), StructField('newDevelopmentFinished', BooleanType(), True), StructField('numPhotos', LongType(), True), StructField('operation', StringType(), True), StructField('parkingSpace', StructType([StructField('hasParkingSpace', BooleanType(), True), StructField('isParkingSpaceIncludedInPrice', BooleanType(), True), StructField('parkingSpacePrice', DoubleType(), True)]), True), StructField('price', DoubleType(), True), StructField('priceByArea', DoubleType(), True), StructField('propertyCode', StringType(), True), StructField('propertyType', StringType(), True), StructField('province', StringType(), True), StructField('rooms', LongType(), True), StructField('showAddress', BooleanType(), True), StructField('size', DoubleType(), True), StructField('status', StringType(), True), StructField('suggestedTexts', StructType([StructField('subtitle', StringType(), True), StructField('title', StringType(), True)]), True), StructField('thumbnail', StringType(), True), StructField('topNewDevelopment', BooleanType(), True), StructField('url', StringType(), True)])



# Read the Parquet data with the specified schema

for dir_name in os.listdir(parquet_path):
    data_df = spark.read.schema(expected_schema).parquet(os.path.join(parquet_path, dir_name, "part-*"))


    # Get the actual schema of the data
    data_schema = data_df.schema

    # Check if the expected schema is complete
    if data_schema == expected_schema:
        print("The schema is complete.")
    else:
        print("The schema is not complete. There are missing or extra columns.")
        print("Expected Schema:")
        print(expected_schema)
        print("Actual Schema:")
        print(data_schema)

# Stop the SparkSession
spark.stop()
