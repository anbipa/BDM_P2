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

# INCOME DATASET
incomes = sc.textFile("../../data/income_opendata/income_opendata_neighborhood.json")
incomeRDD = incomes \
    .map(lambda line: json.loads(line)) \
    .flatMap(lambda obj: [(obj['_id'], measure['year'], obj['neigh_name '], obj['district_id'], obj['district_name'],
                           measure['pop'], measure['RFD'])
                          for measure in obj['info']]).filter(lambda t: len(t) == 7)

print(incomeRDD.first())  # Gives (_id, year, neighborhood_name, district_id, district_name, population, RFD)


#LOOKUP TABLE
#Load data from both neighborhoods and districts datasets
incomeDistricts = sc.textFile("../../data/lookup_tables/income_lookup_district.json")
incomeNeighborhoods = sc.textFile("../../data/lookup_tables/income_lookup_neighborhood.json")

districtsRDD = incomeDistricts \
    .map(lambda line: json.loads(line)) \
    .flatMap(lambda obj: [(obj['_id'], obj['district'], obj['district_name'], obj['district_reconciled'], obj['neighborhood_id'])])\
    .flatMap(lambda x: [(neighborhood, x[0], x[1], x[2], x[3]) for neighborhood in x[4]]) #Split each neighborhood code in a different line with its corresponding district data
#parsed_districts.collect() # (neighborhood_id, district_id, district, district_n, district_reconciled)

neighborhoodsRDD = incomeNeighborhoods \
   .map(lambda line: json.loads(line)) \
   .flatMap(lambda obj: [(obj['_id'], obj['neighborhood'], obj['neighborhood_name'], obj['neighborhood_reconciled'])]) # We remove key:value format (e.g., "district":"Nou Barris"), we only take the value.
#parsed_neighborhoods.collect() #(neighborhood_id, neighborhood, neighborhood_n, neighborhood_reconciled)

#Join districts and neighborhoods, so we get a line for each neighborhood
lookupRDD = districtsRDD.map(lambda x: (x[0], x[1:]))\
                                .join(neighborhoodsRDD\
                                .map(lambda x: (x[0], x[1:])))\
                                .flatMap(lambda x: [(x[0], x[1][1][0], x[1][1][1], x[1][1][2], x[1][0][0], x[1][0][1], x[1][0][2], x[1][0][3])])\
                                .map(lambda x: (x[0], x[1].replace('\"', ""), x[2].replace('\"', ""), x[3].replace('\"', ""), x[4].replace('\"', ""),\
                                                x[5].replace('\"', ""), x[6].replace('\"', ""), x[7].replace('\"', ""))).filter(lambda t: len(t) == 8) #Some corrections to allow correct reconciliation
                                                                                                                         #(i.e., we remove \" to avoid having repeated quotes in some fields like in "neighborhood":"\"Sant Pere, Santa Caterina i la Ribera\"")
print(lookupRDD.first()) # Gives (neighborhood_id, neighborhood, neighborhood_n, neighborhood_reconciled, District_id, district, district_n, district_reconciled)


#RECONCILIATION
incomeRDDmapped = incomeRDD.map(lambda x: (x[2], x)) # Our key in income dataset is neighborhood_name
lookupRDDmapped = lookupRDD.map(lambda x: (x[1], x)) # Our key in lookup table is neighborhood

# Perform a left outer join on the two RDDs
rdd = incomeRDDmapped.leftOuterJoin(lookupRDDmapped).map(lambda x: (str(x[1][0][1])+'_'+x[1][1][0],x[1][0][1], x[1][0][5], x[1][0][6], x[1][1][0]))
print(rdd.first()) #Gives (neighborhood_id, year, population, RFD)