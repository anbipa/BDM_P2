import os
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import *
import json


def drop_duplicates(rdd):
    return rdd.distinct()


def row_to_tuple(row):
    def recursive_tuple(x):
        if isinstance(x, list):
            return tuple(recursive_tuple(item) for item in x)
        elif isinstance(x, dict):
            return recursive_tuple(list(x.values()))
        else:
            return x
    return tuple(recursive_tuple(x) if not isinstance(x, Row) else recursive_tuple(x.asDict()) for x in row)


def load_rent_lookup_tables(sc):
    rentDistricts = sc.textFile("../data/lookup_tables/rent_lookup_district.json")
    rentNeighborhoods = sc.textFile("../data/lookup_tables/rent_lookup_neighborhood.json")

    rentDistrictsRDD = rentDistricts \
        .map(lambda line: json.loads(line)) \
        .flatMap(lambda obj: [(obj['_id'], obj['di'], obj['di_n'], obj['di_re'], obj['ne_id'])]) \
        .flatMap(lambda x: [(neighborhood, x[0], x[1], x[2], x[3]) for neighborhood in x[4]])

    rentNeighborhoodsRDD = rentNeighborhoods \
        .map(lambda line: json.loads(line)) \
        .flatMap(lambda obj: [(obj['_id'], obj['ne'], obj['ne_n'], obj['ne_re'])])

    rentLookupRDD = rentDistrictsRDD.map(lambda x: (x[0], x[1:])) \
        .join(rentNeighborhoodsRDD.map(lambda x: (x[0], x[1:]))) \
        .flatMap(lambda x: [(x[0], x[1][1][0], x[1][1][1], x[1][1][2], x[1][0][0], x[1][0][1], x[1][0][2], x[1][0][3])]) \
        .map(lambda x: (x[0], x[1].replace('\"', ""), x[2].replace('\"', ""), x[3].replace('\"', ""), x[4].replace('\"', ""),
                        x[5].replace('\"', ""), x[6].replace('\"', ""), x[7].replace('\"', ""))).filter(lambda t: len(t) == 8)

    return rentLookupRDD

def load_rent_data(spark, parent_dir, schema):
    union_rdd = spark.sparkContext.emptyRDD()

    for dir_name in os.listdir(parent_dir):

        df = spark.read.schema(schema).parquet(os.path.join(parent_dir, dir_name, "part-*"))

        # count all the rows that neighborhood is not null
        year, month, day, *_ = dir_name.split("_")

        # Convert the DataFrame into an RDD and add year, month, and day columns
        rdd = df.rdd.map(lambda row: row_to_tuple(row) + (int(year), int(month), int(day)))


        # Union the RDD with the previous RDDs
        union_rdd = union_rdd.union(rdd)

    return union_rdd

def reconcile_rent_data(rentRDD, rentLookupRDD):
    rentRDDmapped = rentRDD.map(lambda x: (x[18], x))
    rentLookupRDDmapped = rentLookupRDD.map(lambda x: (x[1], x))

    reconciledRDD = rentRDDmapped.join(rentLookupRDDmapped) \
        .map(lambda row: (*row[1][0], row[1][1][0]) if row[1][1] is not None else (*row[1][0], None))

    return reconciledRDD


def save_to_parquet(rdd, output_path, schema):
    spark = SparkSession.builder \
        .appName("RDD to Parquet") \
        .getOrCreate()

    df = spark.createDataFrame(rdd, schema)

    # Write DataFrame to Parquet file
    df.write.parquet(output_path, mode="overwrite")
    spark.stop()


def execute_housing_formatter():
    sc = pyspark.SparkContext.getOrCreate()
    spark = SparkSession.builder.appName("ReadParquetRDD").getOrCreate()

    # Load lookup tables
    rentLookupRDD = load_rent_lookup_tables(sc)
    print(rentLookupRDD.first())  # Gives (neighborhood_id, neighborhood, neighborhood_n, neighborhood_reconciled, District_id, district, district_n, district_reconciled)

    parent_dir = "../data/idealista"

    # Define the schema
    schema = StructType([StructField('address', StringType(), True),
                         StructField('bathrooms', LongType(), True),
                         StructField('country', StringType(), True),
                         StructField('detailedType', StructType([StructField('subTypology', StringType(), True),
                                                                 StructField('typology', StringType(), True)
                                                                 ]),True),
                         StructField('distance', StringType(), True),
                         StructField('district', StringType(), True),
                         StructField('exterior', BooleanType(), True),
                         StructField('externalReference', StringType(), True),
                         StructField('floor', StringType(), True),
                         StructField('has360', BooleanType(), True),
                         StructField('has3DTour', BooleanType(), True),
                         StructField('hasLift', BooleanType(), True),
                         StructField('hasPlan', BooleanType(), True),
                         StructField('hasStaging', BooleanType(), True),
                         StructField('hasVideo', BooleanType(), True),
                         StructField('latitude', DoubleType(), True),
                         StructField('longitude', DoubleType(), True),
                         StructField('municipality', StringType(), True),
                         StructField('neighborhood', StringType(), True),
                         StructField('newDevelopment', BooleanType(), True),
                         StructField('newDevelopmentFinished', BooleanType(), True),
                         StructField('numPhotos', LongType(), True),
                         StructField('operation', StringType(), True),
                         StructField('parkingSpace', StructType([StructField('hasParkingSpace', BooleanType(), True),
                                                                 StructField('isParkingSpaceIncludedInPrice', BooleanType(),True),
                                                                 StructField('parkingSpacePrice', DoubleType(), True)]),
                                     True),
                         StructField('price', DoubleType(), True),
                         StructField('priceByArea', DoubleType(), True),
                         StructField('propertyCode', StringType(), True),
                         StructField('propertyType', StringType(), True),
                         StructField('province', StringType(), True),
                         StructField('rooms', LongType(), True),
                         StructField('showAddress', BooleanType(), True),
                         StructField('size', DoubleType(), True),
                         StructField('status', StringType(), True),
                         StructField('suggestedTexts', StructType([StructField('subtitle', StringType(), True),
                                                                   StructField('title', StringType(), True)]),
                                     True),
                         StructField('thumbnail', StringType(), True),
                         StructField('topNewDevelopment', BooleanType(), True),
                         StructField('url', StringType(), True)
                         ])


    # Load rent data
    rentRDD = load_rent_data(spark, parent_dir, schema)
    rentRDD = drop_duplicates(rentRDD)

    rentRDD = rentRDD.filter(lambda x: x[18] is not None)
    rentLookupRDD = rentLookupRDD.filter(lambda x: x[1] is not None)


    # Reconcile rent data
    rentRDD_reconciled = reconcile_rent_data(rentRDD, rentLookupRDD)

    # Create new column fields
    col1 = StructField('year', IntegerType(), True)
    col2 = StructField('month', IntegerType(), True)
    col3 = StructField('day', IntegerType(), True)
    col4 = StructField('neighborhoodId', StringType(), True)

    # Create a new schema by appending the new column fields
    new_fields = schema.fields + [col1, col2, col3, col4]
    new_schema = StructType(new_fields)

    # Save reconciled data to Parquet
    save_to_parquet(rentRDD_reconciled, "hdfs://10.4.41.44:27000/user/bdm/parquet/rent", new_schema)
