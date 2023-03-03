"""Handler file for Analytics 6"""
from Config.helper import DataReader as DR
from pyspark.sql.functions import col


def get_analytics():
    """Retruns the output for the required Analytics6"""
    person_df = DR.read_data_from(file_name = "Primary_Person")

    person_df = person_df.filter(person_df.DRVR_ZIP.isNotNull())
    person_df = person_df.drop_duplicates(['CRASH_ID', 'UNIT_NBR', 'PRSN_NBR'])

    person_df = person_df.filter(person_df.PRSN_ALC_RSLT_ID == 'Positive')
    
    person_df = person_df.groupby(person_df.DRVR_ZIP).count().withColumnRenamed("count", "Total")
    result_a6 = person_df.orderBy(col('Total').desc()).select('DRVR_ZIP')

    print('The Top 5 Zip Codes with highest number crashes with alcohols \
           as the contributing factor to a crash :-')
    result_a6.limit(5).show()
