from reconciler import reconcile
import pandas as pd
import os

directory = '../../data/iris'
lookupIris = pd.DataFrame()
for filename in os.listdir(directory):
    print(filename)
    if filename.endswith('.csv'):  # replace with the file extension you want to iterate over
        file = os.path.join(directory, filename)
        df = pd.read_csv(file)
        df = df[['BARRI', 'DISTRICTE']]
        df = df.dropna()
        df = df.drop_duplicates()

        for index, row in df.iterrows():
            series = pd.Series(row[0]) #Barri
            neighborhood = reconcile(series, type_id="Q149621", property_mapping={'P131': pd.Series(row[1])}) \
                            .drop(columns={"description", "score", "match", "score", "type","type_id"}) \
                            .rename(columns={"id": "neighborhood_id", "input_value": "neighborhood",
                                                     "name": "neighborhood_n_reconciled"})
            series = pd.Series(row[1])
            district = reconcile(series, type_id="Q149621") \
                .drop(columns={"description", "score", "match", "score", "type", "type_id"}) \
                .rename(columns={"id": "district_id", "input_value": "district",
                                 "name": "district_n_reconciled"})

            lookup = pd.concat([neighborhood, district], axis=1)
            lookupIris = pd.concat([lookupIris, lookup], axis=0)

lookupIris = lookupIris.drop_duplicates()
lookupIris.to_csv('../../data/lookupIRIS.csv', index=False)