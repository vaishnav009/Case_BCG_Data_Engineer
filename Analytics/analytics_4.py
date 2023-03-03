"""Handler file for Analytics 4"""
from Config.helper import DataReader as DR
from pyspark.sql.window import Window
from pyspark.sql.functions import min, max, sum, col, row_number, monotonically_increasing_id


def get_analytics():
    """Retruns the output for the required Analytics4"""
    units_df = DR.read_data_from(file_name = "Units")
    units_df = units_df.filter(units_df.VEH_MAKE_ID != 'NA')

    units_df = units_df.groupby(units_df.VEH_MAKE_ID).sum('TOT_INJRY_CNT','DEATH_CNT')
    units_df = units_df.withColumnRenamed("sum(TOT_INJRY_CNT)", "Sum_Injuries").withColumnRenamed("sum(DEATH_CNT)", "Sum_Deaths")
    units_df = units_df.withColumn('Total', col('Sum_Injuries')+col('Sum_Deaths'))

    units_df = units_df.orderBy(col('Total').desc())
    units_df = units_df.withColumn("Rank", row_number().over(Window.orderBy(monotonically_increasing_id())))
    result_a4 = units_df.select('VEH_MAKE_ID').filter( (units_df.Rank > 4) & (units_df.Rank  <16))
    
    print('Top 5th to 15th VEH_MAKE_IDs that contribute to largest number of injuries including death :-')
    result_a4.show()
