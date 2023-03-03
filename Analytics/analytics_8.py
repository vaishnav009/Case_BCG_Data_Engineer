"""Handler file for Analytics 8"""
from Config.helper import DataReader as DR
from pyspark.sql.functions import col, row_number, monotonically_increasing_id
from pyspark.sql.window import Window


def _get_speed_related_charges_df():
    """Returns the DF for persons with speed related charges"""
    charges_df = DR.read_data_from(file_name = "Charges")
    person_df = DR.read_data_from(file_name = "Primary_Person")

    person_df = person_df.filter(person_df.PRSN_TYPE_ID.contains('DRIVER'))
    person_df = person_df.join(charges_df, (person_df.CRASH_ID == charges_df.CRASH_ID) & (person_df.UNIT_NBR == charges_df.UNIT_NBR),\
                           'inner').select(charges_df.CRASH_ID,charges_df.UNIT_NBR, charges_df.CHARGE)
    
    speed_related_charges_df = person_df.filter(person_df.CHARGE.contains('SPEED'))
    return speed_related_charges_df


def _get_driver_has_lic_df():
    """Returns the DF for drivers who have driving License"""
    person_df = DR.read_data_from(file_name = "Primary_Person")

    person_df = person_df.filter((person_df.DRVR_LIC_CLS_ID !='NA') & (person_df.DRVR_LIC_CLS_ID !='UNKNOWN') & (person_df.DRVR_LIC_CLS_ID !='UNLICENSED') )
    
    driver_has_lic_df = person_df.select(person_df.CRASH_ID, person_df.UNIT_NBR, person_df.DRVR_LIC_CLS_ID)
    return driver_has_lic_df


def _get_top_ten_colors_crashed_df():
    """Returns the DF for top 10 colors involved in crashes"""
    units_df = DR.read_data_from(file_name = "Units")
    units_df = units_df.dropDuplicates(['CRASH_ID', 'UNIT_NBR'])
    units_df = units_df.filter(units_df.VEH_COLOR_ID != 'NA')

    units_df = units_df.groupby('VEH_COLOR_ID').count().withColumnRenamed("count", "Total_Crashes")
    units_df = units_df.orderBy(col('Total_Crashes').desc())
    units_df = units_df.withColumn(
        "Rank",
        row_number().over(Window.orderBy(monotonically_increasing_id()))
    )
    top_10_color_crashed_df = units_df.filter(units_df.Rank < 11).select('VEH_COLOR_ID')
    return top_10_color_crashed_df


def _get_top_25_states_with_crashes_df():
    """Returns the DF for top 25 states with highest number of crahses"""
    person_df = DR.read_data_from(file_name = "Primary_Person")

    person_df = person_df.filter(person_df.DRVR_LIC_STATE_ID != 'NA')
    person_df = person_df.filter(person_df.DRVR_LIC_STATE_ID != 'Unknown')
    person_df = person_df.filter(person_df.DRVR_LIC_STATE_ID != 'Other')
    person_df = person_df.drop_duplicates(['CRASH_ID'])

    person_df = person_df.groupby(person_df.DRVR_LIC_STATE_ID).count().withColumnRenamed("count", "Total_Crashes")
    person_df = person_df.orderBy(col('Total_Crashes').desc())
    person_df = person_df.withColumn(
        "Rank",
        row_number().over(Window.orderBy(monotonically_increasing_id()))
    )
    top_25_states_df = person_df.filter(person_df.Rank < 26).select('DRVR_LIC_STATE_ID')
    return top_25_states_df


def get_analytics():
    """Retruns the output for the required Analytics8"""
    speed_related_charges_df = _get_speed_related_charges_df()
    driver_has_lic_df = _get_driver_has_lic_df()
    top_10_color_crashed = _get_top_ten_colors_crashed_df()
    top_25_states_df = _get_top_25_states_with_crashes_df()

    units_df = DR.read_data_from(file_name = "Units")
    person_df = DR.read_data_from(file_name = "Primary_Person")
    units_df = units_df.filter(units_df.VIN.isNotNull())
    units_df = units_df.drop_duplicates(['VIN'])

    units_df = units_df.join(person_df, (person_df.CRASH_ID==units_df.CRASH_ID) & (person_df.UNIT_NBR==units_df.UNIT_NBR), 'inner') \
        .select(units_df.CRASH_ID, units_df.UNIT_NBR, units_df.VEH_MAKE_ID, units_df.VEH_COLOR_ID,  person_df.DRVR_LIC_STATE_ID)

    units_df = units_df.join(speed_related_charges_df, (speed_related_charges_df.CRASH_ID==units_df.CRASH_ID) & (speed_related_charges_df.UNIT_NBR==units_df.UNIT_NBR), 'inner') \
        .select(units_df.CRASH_ID, units_df.UNIT_NBR,  units_df.VEH_MAKE_ID,units_df.VEH_COLOR_ID, units_df.DRVR_LIC_STATE_ID, speed_related_charges_df.CHARGE)

    units_df = units_df.join(driver_has_lic_df, (driver_has_lic_df.CRASH_ID == units_df.CRASH_ID) & (driver_has_lic_df.UNIT_NBR == units_df.UNIT_NBR), 'inner') \
        .select(units_df.CRASH_ID, units_df.UNIT_NBR, units_df.VEH_MAKE_ID, units_df.VEH_COLOR_ID, units_df.DRVR_LIC_STATE_ID, units_df.CHARGE, driver_has_lic_df.DRVR_LIC_CLS_ID)

    units_df = units_df.join(top_10_color_crashed, top_10_color_crashed.VEH_COLOR_ID == units_df.VEH_COLOR_ID, 'inner') \
        .select(units_df.CRASH_ID, units_df.UNIT_NBR, units_df.VEH_MAKE_ID, units_df.VEH_COLOR_ID, units_df.DRVR_LIC_STATE_ID, units_df.CHARGE, units_df.DRVR_LIC_CLS_ID)

    units_df = units_df.join(top_25_states_df, top_25_states_df.DRVR_LIC_STATE_ID == units_df.DRVR_LIC_STATE_ID, 'inner') \
        .select(units_df.CRASH_ID, units_df.UNIT_NBR, units_df.VEH_MAKE_ID, units_df.VEH_COLOR_ID, units_df.DRVR_LIC_STATE_ID, units_df.CHARGE, units_df.DRVR_LIC_CLS_ID)


    units_df = units_df.groupby(units_df.VEH_MAKE_ID).count().withColumnRenamed('count', 'Total_Crashes')
    units_df = units_df.orderBy(col('Total_Crashes').desc())

    final_df = units_df.withColumn(
        "Rank",
        row_number().over(Window.orderBy(monotonically_increasing_id()))
    )
    result_a8 = final_df.filter(final_df.Rank < 6).select('VEH_MAKE_ID')
    print("Top 5 vehicle make ids :-")
    result_a8.show()
