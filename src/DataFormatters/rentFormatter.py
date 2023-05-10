import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql import SQLContext
from pyspark.sql import Row
from pyspark import SparkConf
import json
import os


sc = pyspark.SparkContext.getOrCreate()
sqlContext = SQLContext(sc)

def row_to_tuple(row):
    def recursive_tuple(x):
        if isinstance(x, list):
            return tuple(recursive_tuple(item) for item in x)
        elif isinstance(x, dict):
            return recursive_tuple(list(x.values()))
        else:
            return x
    return tuple(recursive_tuple(x) if not isinstance(x, Row) else recursive_tuple(x.asDict()) for x in row)


#Load data from both neighborhoods and districts datasets
rentDistricts = sc.textFile("../../data/lookup_tables/rent_lookup_district.json")
rentNeighborhoods = sc.textFile("../../data/lookup_tables/rent_lookup_neighborhood.json")

rentDistrictsRDD = rentDistricts \
    .map(lambda line: json.loads(line)) \
    .flatMap(lambda obj: [(obj['_id'], obj['di'], obj['di_n'], obj['di_re'], obj['ne_id'])])\
    .flatMap(lambda x: [(neighborhood, x[0], x[1], x[2], x[3]) for neighborhood in x[4]]) #Split each neighborhood code in a different line with its corresponding district data
#parsed_districts.collect() # (neighborhood_id, district_id, district, district_n, district_reconciled)

rentNeighborhoodsRDD = rentNeighborhoods \
   .map(lambda line: json.loads(line)) \
   .flatMap(lambda obj: [(obj['_id'], obj['ne'], obj['ne_n'], obj['ne_re'])]) # We remove key:value format (e.g., "district":"Nou Barris"), we only take the value.
#parsed_neighborhoods.collect() #(neighborhood_id, neighborhood, neighborhood_n, neighborhood_reconciled)

#Join districts and neighborhoods, so we get a line for each neighborhood
rentLookupRDD = rentDistrictsRDD.map(lambda x: (x[0], x[1:]))\
                                .join(rentNeighborhoodsRDD\
                                .map(lambda x: (x[0], x[1:])))\
                                .flatMap(lambda x: [(x[0], x[1][1][0], x[1][1][1], x[1][1][2], x[1][0][0], x[1][0][1], x[1][0][2], x[1][0][3])])\
                                .map(lambda x: (x[0], x[1].replace('\"', ""), x[2].replace('\"', ""), x[3].replace('\"', ""), x[4].replace('\"', ""),\
                                                x[5].replace('\"', ""), x[6].replace('\"', ""), x[7].replace('\"', ""))).filter(lambda t: len(t) == 8) #Some corrections to allow correct reconciliation
                                                                                                                         #(i.e., we remove \" to avoid having repeated quotes in some fields like in "neighborhood":"\"Sant Pere, Santa Caterina i la Ribera\"")
print(rentLookupRDD.first()) # Gives (neighborhood_id, neighborhood, neighborhood_n, neighborhood_reconciled, District_id, district, district_n, district_reconciled)

import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit

spark = SparkSession.builder.appName("ReadParquetRDD").getOrCreate()
parent_dir = "../../data/idealista"

union_rdd = spark.sparkContext.emptyRDD()

for dir_name in os.listdir(parent_dir):
    df = spark.read.parquet(os.path.join(parent_dir, dir_name, "part-*"))

    year, month, day, *_ = dir_name.split("_")

    # convert the DataFrame into an RDD
    rdd = df.rdd.filter(lambda row: len(row) == 35).map(lambda row: row_to_tuple(row))  # convert to tuple
    # union the RDD with the previous RDDs
    union_rdd = union_rdd.union(rdd)

union_rdd = union_rdd.filter(lambda x: x[18] is not None)
rentLookupRDD = rentLookupRDD.filter(lambda x: x[1] is not None)

rentRDDmapped = union_rdd.map(lambda x: (x[18], x))  # Our key in rent dataset is neighborhood_name
rentLookupRDDmapped = rentLookupRDD.map(lambda x: (x[1], x))  # Our key in lookup table is neighborhood

rentRDD = rentRDDmapped.join(rentLookupRDDmapped) \
    .map(lambda row: (*row[1][0], row[1][1][0]) if row[1][1] is not None else (*row[1][0], None))


#rentRDD = remove_nulls_rdd(rentRDD)
#rentRDD = remove_duplicates(rentRDD)

for row in rentRDD.take(3):
  print(row)


