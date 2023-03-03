"""Handler file for Analytics 2"""
from Config.helper import DataReader as DR


def get_analytics():
    """Retruns the output for the required Analytics2"""
    units_df = DR.read_data_from(file_name = "Units")
    units_df = units_df.filter((units_df.VEH_BODY_STYL_ID =='MOTORCYCLE') | (units_df.VEH_BODY_STYL_ID =='POLICE MOTORCYCLE') )
    result_a2 = units_df.count()

    print(f'Total number of two wheelers booked for crashes = {result_a2}')
