"""Handler file for all the Analytics files"""
from Analytics import analytics_1 as A1
from Analytics import analytics_2 as A2
from Analytics import analytics_3 as A3
from Analytics import analytics_4 as A4
from Analytics import analytics_5 as A5
from Analytics import analytics_6 as A6
from Analytics import analytics_7 as A7
from Analytics import analytics_8 as A8
from Config.helper import DataReader as DR
    

def handler():  
    call_map = {'1': A1.get_analytics,
                '2': A2.get_analytics,
                '3': A3.get_analytics,
                '4': A4.get_analytics,
                '5': A5.get_analytics,
                '6': A6.get_analytics,
                '7': A7.get_analytics,
                '8': A8.get_analytics
            }
    analytics_to_calculate = DR.get_input_analytics()
    for analytics in analytics_to_calculate:
        call_map[analytics.strip()]()


if __name__ == "__main__":
    handler()