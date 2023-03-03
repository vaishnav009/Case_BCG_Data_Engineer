"""Class to help raed the data from various data files"""
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("BCG_Case_Project").getOrCreate()


class DataReader():

    @classmethod
    def read_data_from(cls, file_name: str):
        if file_name:
            file_loc = cls._get_file_loc_for(file_name)
            data_df = spark.read.options(inferSchema = True, header = True).csv(file_loc)
            return data_df
        else:
            raise Exception('No file name provided')
    
    @classmethod
    def _get_file_loc_for(cls, file_name: str):
        loc_map = {
                    "Charges": "Data/Charges_use.csv",
                    "Damages": "Data/Damages_use.csv",
                    "Endorse": "Data/Endorse_use.csv",
                    "Primary_Person": "Data/Primary_Person_use.csv",
                    "Restrict" : "Data/Restrict_use.csv",
                    "Units": "Data/Units_use.csv"
                }
        try:
            location =  loc_map[file_name]
        except KeyError:
            print('No such file name found in file location map. Please verify file name.')
        return location
    
    @classmethod
    def get_input_analytics(cls, input_file = "Input_Analytics"):
        file_loc = 'Data/Input_Analytics.csv'
        input_df = spark.read.csv(file_loc)
        return list(input_df.first())

    