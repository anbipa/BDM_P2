# BDM P2: Formatted and Exploitation Zones

#### Aniol Bisquert & Daniel Cantabella

The project is structured in such a way that [data](data) contains the necessary files in order to execute the code.
Inside this folder you can find the files from the different sources (which are assumed to be in a Landing Zone storage), 
and also the necessary driver ([postgresql-42.6.0.jar](data%2Fpostgresql-42.6.0.jar)) to connect and send the processed
data to the exploitation zone.

The [src](src) folder contains 3 different folders:
* [DataFormatters](src%2FDataFormatters) contains the different formatters used for each different source. 
It also contains the [reconciliation.py](src%2FDataFormatters%2Freconciliation.py) script, which performs the 
reconciliation process of our IRIS dataset and creates the IRIS lookup file: [lookupIRIS.csv](data%2Flookup_tables%2FlookupIRIS.csv)

    **_NOTE:_** The reconciliation of some neihgborhoods achieved with the script contained some repeated instances with slightly differences between 
them (e.g., repeated neighborhoods with/without double spaces between hyphens)
so [lookupIRIS.csv](data%2Flookup_tables%2FlookupIRIS.csv) was manually modified to ensure the correct reconciliation 
with other lookup files. We assume this process should have been created in previous steps of the project as the other lookup files.

* [DataExploters](src%2FDataExploters) contains the files necessary for our Exploitation zone. It contains 
[datasetGenerator.py](src%2FDataExploters%2FdatasetGenerator.py) to generate the file our machine learning model will work with and 
[mapReduce.py](src%2FDataExploters%2FmapReduce.py) which computes/prepares the data necessary for the user to calculate the KPIs.
* [DistributedML](src%2FDistributedML) contains the files to train our machine learning model ([ml_trainer.py](src%2FDistributedML%2Fml_trainer.py))
and the file to predict the rental price of a new apartment ([ml_predict.py](src%2FDistributedML%2Fml_predict.py)) based on the data generated with [datasetGenerator.py](src%2FDataExploters%2FdatasetGenerator.py).

## Instructions:
In order to run the project, please install the requirements using the following command:

```pip install -r requirements.txt```

You just need to run the [executeProgram.py](src%2FexecuteProgram.py) by using the following command:

```python src/executeProgram.py```

This will show a menu with the different options to run the project:
```
 ============= MENU OPTIONS =============

0. Create LookUp Data (IRIS)
1. Execute Data Formatters
2. Prepare Data for KPIs
3. Generate datasets for ML
4. Train and deploy ML model
5. Deploy ML model and predict
6. Exit
```

The results can be seen by accessing the server, where the PostgreSQL database was also created.