import pyspark
from pyspark.ml.feature import StandardScaler
from pyspark.ml.linalg import Vectors
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import RandomForestRegressor
from pyspark.ml.evaluation import RegressionEvaluator

# Create a SparkSession
spark = pyspark.sql.SparkSession.builder.getOrCreate()

# Load the data and select the required columns
df = spark.read.parquet("hdfs://10.4.41.44:27000/user/bdm/parquet/rent") \
    .select("price", "year", "bathrooms", "distance", "latitude", "longitude", "rooms", "size")

# Filter out rows with invalid values
df = df.filter(df.bathrooms > 0)
df = df.filter(df.distance > 0)
df = df.filter(df.size > 0)

# Convert the "distance" column to a numerical data type
df = df.withColumn("distance", df["distance"].cast("double"))

# Create a feature vector assembler
assembler = VectorAssembler(
    inputCols=["year", "bathrooms", "distance", "latitude", "longitude", "rooms", "size"],
    outputCol="features"
)

# Transform the DataFrame to include the feature vector column
df = assembler.transform(df)

# Scale the features using StandardScaler
scaler = StandardScaler(
    inputCol="features",
    outputCol="scaledFeatures",
    withStd=True,
    withMean=True
)

# Compute summary statistics and transform the data
scalerModel = scaler.fit(df)
df = scalerModel.transform(df)

# Split the data into training and test sets
(trainingData, testData) = df.randomSplit([0.7, 0.3])

# Create a Random Forest Regressor
rf = RandomForestRegressor(
    labelCol="price",
    featuresCol="scaledFeatures",
    numTrees=10
)

# Train the model
model = rf.fit(trainingData)

# Make predictions on the test set
predictions = model.transform(testData)

# Evaluate the model
evaluator = RegressionEvaluator(
    labelCol="price",
    predictionCol="prediction",
    metricName="rmse"
)
rmse = evaluator.evaluate(predictions)

print("Root Mean Squared Error (RMSE):", rmse)
