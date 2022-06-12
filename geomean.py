import numpy
import pandas
from scipy import stats
import datetime

def thirty_day_diff(data, start_date):
    end_date = start_date - datetime.timedelta(days = 30)
    mask = (data['SampleDate'] <= start_date) & (data['SampleDate'] >= end_date)
    return data.loc[mask]

def fourtyfive_day_diff(data, start_date):
    end_date = start_date - datetime.timedelta(days = 42)
    mask = (data['SampleDate'] <= start_date) & (data['SampleDate'] >= end_date)
    return data.loc[mask]

def geo_mean(data):
    if not data.empty:
        if data.shape[0] >= 5:
            return stats.gmean(data)
        else:
            return "Less than five samples, geomean not calculated"
    else:
        return 0 

def gm_calc(data):
    for station in data["StationCode"].unique():
        station_data = data[data["StationCode"] == station]
        for analyte in station_data["DW_AnalyteName"].unique():
            analyte_station_data = station_data[station_data["DW_AnalyteName"] == analyte]
            for date in analyte_station_data["SampleDate"]:
                mask = (data['SampleDate'] == date) & (data['StationCode'] == station) & (data["DW_AnalyteName"] == analyte)
                #### 30 DAY GEOMEAN ####
                data30 = thirty_day_diff(analyte_station_data,date)
                geomean = geo_mean(data30.loc[:,"Result"])
                data.loc[mask, 'Geomean30'] = geomean
                #### 42 DAY GEOMEAN ####
                data45 = fourtyfive_day_diff(analyte_station_data,date)
                geomean = geo_mean(data45.loc[:,"Result"])
                data.loc[mask, 'Geomean45'] = geomean
    return data