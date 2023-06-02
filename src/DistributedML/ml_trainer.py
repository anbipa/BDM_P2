import pyspark
from pyspark.ml.feature import StandardScaler, VectorAssembler, StringIndexer, OneHotEncoder
from pyspark.ml.regression import RandomForestRegressor
from pyspark.ml.evaluation import RegressionEvaluator
import datetime

def load_data(file_path):
    # Create a SparkSession
    spark = pyspark.sql.SparkSession.builder.getOrCreate()

    # Load the data and select the required columns
    df = spark.read.csv(file_path, header=True, inferSchema=True)
    df = df.na.drop()  # Remove rows with null values

    return df

def preprocess_data(df):
    # Split the data into training and test sets
    (trainingData, testData) = df.randomSplit([0.7, 0.3])

    numeric_features = [t[0] for t in df.dtypes if t[1] == 'int' or t[1] == 'double']
    numeric_features.remove('price')
    numeric_features.remove('year')

    boolean_features = [t[0] for t in df.dtypes if t[1] == 'boolean']

    # Impute numeric features
    imputer = pyspark.ml.feature.Imputer(inputCols=numeric_features, outputCols=numeric_features)
    imputer = imputer.fit(trainingData)
    train = imputer.transform(trainingData)
    test = imputer.transform(testData)

    # Assemble numeric and boolean features
    numeric_assembler = VectorAssembler(inputCols=numeric_features, outputCol="numeric_features")
    train = numeric_assembler.transform(train)
    test = numeric_assembler.transform(test)

    boolean_assembler = VectorAssembler(inputCols=boolean_features, outputCol="boolean_features")
    train = boolean_assembler.transform(train)
    test = boolean_assembler.transform(test)

    # Encode categorical features
    categorical_features = ['propertyType', 'district', 'neighborhoodId']

    for col in categorical_features:
        string_indexer = StringIndexer(inputCol=col, outputCol=col + "_index")
        string_indexer_model = string_indexer.fit(df)
        train = string_indexer_model.transform(train)
        test = string_indexer_model.transform(test)

        one_hot_encoder = OneHotEncoder(inputCols=[col + "_index"], outputCols=[col + "_encoded"])
        one_hot_encoder_model = one_hot_encoder.fit(train)
        train = one_hot_encoder_model.transform(train)
        test = one_hot_encoder_model.transform(test)

    # Assemble all features, including boolean and encoded categorical features
    assembler = VectorAssembler(
        inputCols=["numeric_features"] + ["boolean_features"],
        outputCol="features"
    )
    train = assembler.transform(train)
    test = assembler.transform(test)

    return train, test

def train_model(train_data):
    # Train a RandomForest model
    rf = RandomForestRegressor(featuresCol="features", labelCol="price", numTrees=10)
    model = rf.fit(train_data)

    return model

def evaluate_model(model, test_data):
    # Make predictions
    predictions = model.transform(test_data)

    # Select example rows to display
    predictions.select("prediction", "price", "features").show(5)

    # Evaluate the model
    evaluator = RegressionEvaluator(labelCol="price", predictionCol="prediction", metricName="rmse")
    rmse = evaluator.evaluate(predictions)
    print("Root Mean Squared Error (RMSE) on test data = %g" % rmse)

def save_model(model, file_path):
    # Get the current timestamp
    timestamp = datetime.datetime.now().strftime("%Y%m%d%H%M%S")

    # Define the model version with the timestamp
    model_version = "v" + timestamp

    # Define the path to save the model with the version
    model_path = file_path + "_" + model_version

    # Save the model
    model.save(model_path)

def train_and_save():
    # Define the file path
    file_path = "hdfs://10.4.41.44:27000/user/bdm/dataset/rentdataset.csv"

    # Load the data
    data = load_data(file_path)

    # Preprocess the data
    train_data, test_data = preprocess_data(data)

    # Train the model
    trained_model = train_model(train_data)

    # Evaluate the model
    evaluate_model(trained_model, test_data)

    # Define the model path
    model_path = "hdfs://10.4.41.44:27000/user/bdm/models/random_forest_model"

    # Save the model
    save_model(trained_model, model_path)
