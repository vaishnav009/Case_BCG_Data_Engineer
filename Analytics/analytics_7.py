"""Handler file for Analytics 7"""
from Config.helper import DataReader as DR
from pyspark.sql.functions import col, row_number, monotonically_increasing_id
from pyspark.sql.window import Window


def  _get_prop_damages_df():
    """Returns th DF for crashes where No property damage was reported"""
    damages_df = DR.read_data_from(file_name = "Damages")
    prop_damages_df = damages_df.filter(damages_df.DAMAGED_PROPERTY.contains('NO DAMAGE'))
    return prop_damages_df


def _get_vehicle_damage_df():
    """Returns the DF for crahses where vehicles have damages_SCL > 4"""
    units_df = DR.read_data_from(file_name = "Units")
    vehicle_damage_df = units_df.filter((units_df.VEH_DMAG_SCL_1_ID == 'DAMAGED 7 HIGHEST') | \
                                        (units_df.VEH_DMAG_SCL_1_ID == 'DAMAGED 6') | \
                                        (units_df.VEH_DMAG_SCL_1_ID == 'DAMAGED 5') | \
                                        (units_df.VEH_DMAG_SCL_2_ID == 'DAMAGED 7 HIGHEST') |
                                        (units_df.VEH_DMAG_SCL_2_ID == 'DAMAGED 6') | \
                                        (units_df.VEH_DMAG_SCL_2_ID == 'DAMAGED 5')) \
                                        .select(units_df.CRASH_ID, units_df.UNIT_NBR,units_df.VEH_DMAG_SCL_1_ID,\
                                                units_df.VEH_DMAG_SCL_2_ID)
    return vehicle_damage_df


def _get_insured_vehicle_df():
    """Returns the DF for crashes where vehicles had insurance"""
    units_df = DR.read_data_from(file_name = "Units")
    insured_vehicle_df = units_df.filter( units_df.FIN_RESP_TYPE_ID.contains('INSURANCE')) \
        .select(units_df.CRASH_ID, units_df.UNIT_NBR)
    return insured_vehicle_df


def get_analytics():
    """Retruns the output for the required Analytics7"""
    units_df = DR.read_data_from(file_name = "Units")
    prop_damages_df = _get_prop_damages_df()
    vehicle_damage_df = _get_vehicle_damage_df()
    insured_vehicle_df = _get_insured_vehicle_df()

    units_df = units_df.join(prop_damages_df, units_df.CRASH_ID == prop_damages_df.CRASH_ID , 'inner') \
                            .select(units_df.CRASH_ID, units_df.UNIT_NBR, prop_damages_df.DAMAGED_PROPERTY)

    units_df = units_df.join(vehicle_damage_df, (units_df.CRASH_ID == vehicle_damage_df.CRASH_ID) & \
                            (units_df.UNIT_NBR == vehicle_damage_df.UNIT_NBR) ) \
                            .select(units_df.CRASH_ID, units_df.UNIT_NBR)

    units_df = units_df.join(insured_vehicle_df, (units_df.CRASH_ID == insured_vehicle_df.CRASH_ID) & \
                            (units_df.UNIT_NBR == insured_vehicle_df.UNIT_NBR), 'inner') \
                            .select(units_df.CRASH_ID, units_df.UNIT_NBR)

    print('Distinct Crash IDs where No Damaged Property was observed and Damage Level (VEH_DMAG_SCL~) \
           is above 4 and car avails Insurance :-')
    units_df.drop_duplicates(['CRASH_ID']).select('CRASH_ID').show()