"""Handler file for Analytics 3"""
from Config.helper import DataReader as DR


def get_analytics():
    """Retruns the output for the required Analytics3"""
    person_df = DR.read_data_from(file_name = "Primary_Person")
    person_df = person_df.filter(person_df.DRVR_LIC_STATE_ID != 'NA')
    person_df = person_df.filter(person_df.PRSN_GNDR_ID == 'FEMALE')
    person_df = person_df.groupby(person_df.DRVR_LIC_STATE_ID).count().withColumnRenamed("count", "Total_Crashes")
    person_df = person_df.orderBy(person_df.Total_Crashes.desc())
    result_a3 = person_df.first().asDict()['DRVR_LIC_STATE_ID']

    print(f'State with highest number of accidents in which females are involved = {result_a3}')
