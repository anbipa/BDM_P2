from DataFormatters.incomeFormatter import execute_income_formatter
from DataFormatters.housingFormatter import execute_housing_formatter
from DataFormatters.irisFormatter import execute_iris_formatter
from DataFormatters.neighborhoodsFormatter import execute_neighborhood_formatter

from DataFormatters.reconciliation import execute_lookup_iris

# Explotation
from DataExploters.mapReduce import execute_map_reducers
from DataExploters.datasetGenerator import generate_ml_dataset

# Distributed ML
from DistributedML.ml_trainer import train_and_save
from DistributedML.ml_predict import deploy_and_predict

def menu_options():
    print("\n\n\n ============= MENU OPTIONS =============\n")

    print("0. Create LookUp Data (IRIS)")
    print("1. Execute Data Formatters")
    print("2. Prepare Data for KPIs")
    print("3. Generate datasets for ML")
    print("4. Train and deploy ML model")
    print("5. Deploy ML model and predict")
    print("6. Exit")

    option = input("\nSelect a option (number): ")

    if option.isdigit():
        option = int(option)

    return str(option)

def main():
    print("Exectute P2...")

    while True:
        option = menu_options()
        if option.isdigit():
            # Execute the different zones of the DMBackbone pipeline

            if int(option) == 0:
                print("Create LookUp Data (IRIS)")
                execute_lookup_iris()
                print("LookUp Data Formatted")

            elif int(option) == 1:

                print("Execute Data Formatters")
                print("Available formatters:")
                print("0. Housing")
                print("1. Income")
                print("2. IRIS")
                print("3. Neighborhood")

                option = input("\nWhich data Formatter do you want to execute: ")

                if option.isdigit():
                    option = int(option)
                    if option == 0:
                        print("Execute Data Formatter: Housing")
                        execute_housing_formatter()
                        print("Housing Data Formatted")
                    elif option == 1:
                        print("Execute Data Formatter: Income")
                        execute_income_formatter()
                        print("LookUp Data Formatted")
                    elif option == 2:
                        print("Execute Data Formatter: IRIS")
                        execute_iris_formatter()
                        print("Income Data Formatted")
                    elif option == 3:
                        print("Execute Data Formatter: Neighborhood")
                        execute_neighborhood_formatter()
                        print("Incidences Data Formatted")
                    else:
                        print("Invalid option")
                        continue

            elif int(option) == 2:
                print("Prepare Data for KPIs")
                execute_map_reducers()
                print("Data prepared")

            elif int(option) == 3:
                print("Generate datasets for ML")
                generate_ml_dataset()
                print("Datasets generated")

            elif int(option) == 4:
                print("Train and save ML model")
                train_and_save()
                print("Model trained and saved")

            elif int(option) == 5:
                print("Deploy ML model and predict")
                deploy_and_predict()
                print("Model deployed and predicted")

            elif int(option) == 6:
                print("Exit")
                break










if __name__ == "__main__":
    main()