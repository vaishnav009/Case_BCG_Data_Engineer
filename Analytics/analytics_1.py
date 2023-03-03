"""Handler file for Analytics 1"""
from Config.helper import DataReader as DR


def get_analytics():
    """Retruns the output for the required Analytics1"""
    person_df = DR.read_data_from(file_name = "Primary_Person")
    person_df = person_df.filter( (person_df.PRSN_INJRY_SEV_ID=='KILLED') & (person_df.PRSN_GNDR_ID == 'MALE') )
    person_df = person_df.drop_duplicates(['CRASH_ID'])
    result_a1 = person_df.count()

    print(f'Total number of crashes (accidents) in which number of persons Killed are Male = {result_a1}')
