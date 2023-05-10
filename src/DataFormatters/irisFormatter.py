from incomeFormatter import sc
rdd = sc.textFile("../../data/iris/*.csv")


# FALTA LOOKUP IRIS
lookupRDDmapped = lookupRDD.map(lambda x: (x[1], x)) # Our key in lookup table is neighborhood


# Filter header row
header = rdd.first()
# Map each CSV row to a tuple
rdd_tuples = rdd.filter(lambda row: row != header)\
                .map(lambda row: tuple(row.split(",")))\
                .filter(lambda row: len(row) == 25)\
                .map(lambda row: (row[14], *row[:14], *row[15:]))\
                .map(lambda row: (row[0].replace('"', ''), row[1:]))\
                .leftOuterJoin(lookupRDDmapped)\
                .map(lambda row: (*row[1][0], row[1][1][0]))


for row in rdd_tuples.take(10):
    print(row)