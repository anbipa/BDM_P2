import pyspark
from pyspark.sql import SparkSession
import json


def drop_duplicates(rdd):
    return rdd.distinct()

def load_income_dataset(sc):
    incomes = sc.textFile("../data/income_opendata/income_opendata_neighborhood.json")
    incomeRDD = incomes \
        .map(lambda line: json.loads(line)) \
        .flatMap(
        lambda obj: [(obj['_id'], measure['year'], obj['neigh_name '], obj['district_id'], obj['district_name'],
                      measure['pop'], measure['RFD'])
                     for measure in obj['info']]).filter(lambda t: len(t) == 7)
    return incomeRDD


def load_lookup_tables(sc):
    incomeDistricts = sc.textFile("../data/lookup_tables/income_lookup_district.json")
    incomeNeighborhoods = sc.textFile("../data/lookup_tables/income_lookup_neighborhood.json")

    districtsRDD = incomeDistricts \
        .map(lambda line: json.loads(line)) \
        .flatMap(lambda obj: [
        (obj['_id'], obj['district'], obj['district_name'], obj['district_reconciled'], obj['neighborhood_id'])]) \
        .flatMap(lambda x: [(neighborhood, x[0], x[1], x[2], x[3]) for neighborhood in x[4]])

    neighborhoodsRDD = incomeNeighborhoods \
        .map(lambda line: json.loads(line)) \
        .flatMap(
        lambda obj: [(obj['_id'], obj['neighborhood'], obj['neighborhood_name'], obj['neighborhood_reconciled'])])

    lookupRDD = districtsRDD.map(lambda x: (x[0], x[1:])) \
        .join(neighborhoodsRDD.map(lambda x: (x[0], x[1:]))) \
        .flatMap(lambda x: [(x[0], x[1][1][0], x[1][1][1], x[1][1][2], x[1][0][0], x[1][0][1], x[1][0][2], x[1][0][3])]) \
        .map(
        lambda x: (x[0], x[1].replace('\"', ""), x[2].replace('\"', ""), x[3].replace('\"', ""), x[4].replace('\"', ""),
                   x[5].replace('\"', ""), x[6].replace('\"', ""), x[7].replace('\"', ""))).filter(
        lambda t: len(t) == 8)

    return lookupRDD


def reconcile_income_data(incomeRDD, lookupRDD):
    incomeRDDmapped = incomeRDD.map(lambda x: (x[2], x))
    lookupRDDmapped = lookupRDD.map(lambda x: (x[1], x))

    rdd = incomeRDDmapped.leftOuterJoin(lookupRDDmapped).map(
        lambda x: (str(x[1][0][1]) + '_' + x[1][1][0], x[1][0][1], x[1][0][5], x[1][0][6], x[1][1][0]))
    return rdd


def save_to_parquet(rdd):
    spark = SparkSession.builder \
        .appName("RDD to Parquet") \
        .getOrCreate()

    df = spark.createDataFrame(rdd, ["id", "year", "population", "RFD", "neighborhood_id"])

    # Write DataFrame to Parquet file in HDFS
    df.write.parquet("hdfs://10.4.41.44:27000/user/bdm/parquet/income", mode="overwrite")
    spark.stop()

def execute_income_formatter():
    sc = pyspark.SparkContext.getOrCreate()
    incomeRDD = load_income_dataset(sc)
    incomeRDD = drop_duplicates(incomeRDD)
    lookupRDD = load_lookup_tables(sc)
    rdd = reconcile_income_data(incomeRDD, lookupRDD)
    save_to_parquet(rdd)



