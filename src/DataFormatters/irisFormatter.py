import pyspark
from pyspark.sql import SQLContext
import re


def drop_duplicates(rdd):
    return rdd.distinct()


def load_lookup_table(sc):
    lookupRDD = sc.textFile("../data/lookup_tables/lookupIRIS.csv")
    pattern = r'(".*?),(.*)(")'
    headerLookup = lookupRDD.first()  # Filter header row
    # Map each CSV row to a tuple
    lookupRDDtuples = lookupRDD.filter(lambda row: row != headerLookup) \
        .map(lambda row: re.sub(pattern, r'\1\2', row)) \
        .map(lambda row: row.split(",")) \
        .map(lambda row: tuple(f.strip('"') for f in row)) \
        .filter(lambda row: len(row) == 6) \
        .map(lambda row: (row[0], row[2], row[1], row[3], row[5], row[4])) \
        # (neighborhood_id, neighborhood, neighborhood_reconciled, district_id, district, district_reconciled)

    return lookupRDDtuples


def load_iris_dataset(sc):
    irisRDD = sc.textFile("../data/iris/*.csv")
    headerIris = irisRDD.first()
    rdd_tuples = irisRDD.filter(lambda row: row != headerIris) \
        .map(lambda row: tuple(row.split(","))) \
        .map(lambda row: tuple(f.strip('"') for f in row)) \
        .filter(lambda x: x[14] != '') \
        .filter(lambda row: len(row) == 25)
    return rdd_tuples


def reconcile_data(lookupRDDtuples, rdd_tuples):
    irisRDDmapped = rdd_tuples.map(lambda x: (x[14], x))  # Our key in the income dataset is neighborhood_name
    lookupRDDmapped = lookupRDDtuples.map(lambda row: (row[1], row))  # Our key in the lookup table is neighborhood
    iris_reconciled = irisRDDmapped.leftOuterJoin(lookupRDDmapped) \
        .map(lambda row: (row[1][1][0], row[1][0])) \
        .map(lambda x: (x[0], *x[1])) \
        .filter(lambda row: len(row) == 26)\
        .map(lambda x: (*x[:12], *x[16:]))

    return iris_reconciled


def save_to_parquet(df, output_path):
    df.write.parquet(output_path, mode="overwrite")


def execute_iris_formatter():
    sc = pyspark.SparkContext.getOrCreate()
    sqlContext = SQLContext(sc)


    lookupRDDtuples = load_lookup_table(sc)
    rdd_tuples = load_iris_dataset(sc)
    rdd_tuples = drop_duplicates(rdd_tuples)

    iris_reconciled = reconcile_data(lookupRDDtuples, rdd_tuples)

    df = iris_reconciled.toDF(
        ["neighborhoodID", "fitxaID", "type", "area", "element", "detall", "diaDataAlta", "mesDataAlta",
         "anyDataAlta", "diaDataTancament", "mesDataTancament", "anyDataTancament", "seccioCensal", "tipusVia", "carrer", "numero", "coordenadaX",
         "coordenadaY", "longitud", "latitud", "suport", "canalsResposta"])

    print(df.show())
    output_path = "hdfs://10.4.41.44:27000/user/bdm/parquet/iris"
    save_to_parquet(df, output_path)
