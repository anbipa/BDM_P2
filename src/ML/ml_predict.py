from hdfs import InsecureClient
from pyspark.ml.regression import RandomForestRegressionModel
from pyspark.sql import SparkSession
from ml_trainer import preprocess_data, load_data, evaluate_model


def choose_model(path):
    # Connect to the HDFS cluster
    hdfs = InsecureClient(url='http://10.4.41.44:9870', user='bdm')

    # List the models
    model_files = hdfs.list(path)

    # Extract the model names from the file paths
    model_names = [file_path.split('/')[-1] for file_path in model_files]

    # Display the model names and let the user choose
    print("Available models:")
    for i, model_name in enumerate(model_names):
        print(f"{i+1}. {model_name}")

    # Prompt the user to choose a model
    while True:
        try:
            choice = int(input("Enter the number corresponding to the model you want to choose: "))
            if 1 <= choice <= len(model_names):
                break
            else:
                print("Invalid choice. Please enter a valid number.")
        except ValueError:
            print("Invalid input. Please enter a number.")

    # get the model file path
    model_file_path = "hdfs://10.4.41.44:27000/user/bdm/" + path + '/' + model_files[choice - 1]

    # Load the model
    model = RandomForestRegressionModel.load(model_file_path)

    # Return the chosen model
    return model


# Define the path to the models
model_path = "models"

# Create a SparkSession
spark = SparkSession.builder.appName("ModelPrediction").getOrCreate()

# Choose the model
model = choose_model(model_path)

# define dataset path
dataset_path = "hdfs://10.4.41.44:27000/user/bdm/dataset/rentdataset.csv"

# Load the data
data = load_data(dataset_path)

# Preprocess the data
train_data, test_data = preprocess_data(data)

# Evaluate the model
evaluate_model(model, test_data)


