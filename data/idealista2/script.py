import os
import json


def count_total_rows(directory):
    total_rows = 0

    # Iterate over all files in the directory
    for filename in os.listdir(directory):
        if filename.endswith(".json"):
            file_path = os.path.join(directory, filename)
            with open(file_path, 'r') as file:
                # Load the JSON data
                data = json.load(file)
                if isinstance(data, list):
                    # If the JSON data is a list, add its length to the total rows count
                    total_rows += len(data)

    return total_rows


# Provide the directory path
directory_path = './'

# Call the function and print the total rows count
total_rows_count = count_total_rows(directory_path)
print("Total rows across all JSON files:", total_rows_count)
