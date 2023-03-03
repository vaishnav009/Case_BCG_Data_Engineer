"""Handler file for Analytics 5"""
from Config.helper import DataReader as DR
from pyspark.sql.functions import col, row_number
from pyspark.sql.window import Window


def get_analytics():
    """Retruns the output for the required Analytics5"""
    units_df = DR.read_data_from(file_name = "Units")
    person_df = DR.read_data_from(file_name = "Primary_Person")

    units_df = units_df.filter(units_df.VEH_BODY_STYL_ID != 'NA')
    units_df = units_df.filter(units_df.VEH_BODY_STYL_ID != 'NOT REPORTED')

    person_df = person_df.join(units_df, (person_df.CRASH_ID == units_df.CRASH_ID) & (person_df.UNIT_NBR == units_df.UNIT_NBR),\
                            'inner').\
                            select(units_df.CRASH_ID,units_df.UNIT_NBR,person_df.PRSN_ETHNICITY_ID, units_df.VEH_BODY_STYL_ID)
    person_df = person_df.dropDuplicates(['CRASH_ID', 'UNIT_NBR'])


    person_df = person_df.groupby('VEH_BODY_STYL_ID', 'PRSN_ETHNICITY_ID').count().withColumnRenamed("count", "Total")
    person_df = person_df.orderBy('VEH_BODY_STYL_ID',col('Total').desc())

    wn = Window.partitionBy('VEH_BODY_STYL_ID').orderBy(col('Total').desc())
    person_df = person_df.withColumn('Rank', row_number().over(wn))
    result_a5 = person_df.filter(person_df.Rank == 1).select('VEH_BODY_STYL_ID','PRSN_ETHNICITY_ID')

    print('The top ethnic user group of each unique body style :-')
    result_a5.show()
