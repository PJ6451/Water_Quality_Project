import numpy
import pandas
import scipy
import datetime

def thirty_day_diff(data,start_date):
    end_date = start_date - datetime.timedelta(days = 30)
    mask = (data['SampleDate'] < start_date) & (data['SampleDate'] >= end_date)
    df = df.loc[mask]
    return data

def fourtyfive_day_diff(data,start_date):
    end_date = start_date - datetime.timedelta(days = 45)
    mask = (data['SampleDate'] < start_date) & (data['SampleDate'] >= end_date)
    df = df.loc[mask]
    return data

def sixty_day_diff(data,start_date):
    end_date = start_date - datetime.timedelta(days = 60)
    mask = (data['SampleDate'] < start_date) & (data['SampleDate'] >= end_date)
    df = df.loc[mask]
    return data

def geo_mean(data):
    if ~data.empty:
        num = scipy.stats.gmean(data.loc[:,"Result"])
    else:
        num = 0
    
    return num

def gm_calc(data):
    for station in data["StationCode"]:
        station_data = data[data["StationCode"] == station]
        for analyte in station_data["DW_AnalyteName"]:
            analyte_station_data = station_data[station_data["DW_AnalyteName"] == analyte]
            for date in station_data["SampleDate"]:
                #### 30 DAY GEOMEAN ####
                data30 = thirty_day_diff(analyte_station_data,date)
                geomean = geo_mean(data30)
                mask = (data['SampleDate'] == date) & (data['StationCode'] == station)
                data.loc[mask]['Geomean30'] = geomean
                #### 45 DAY GEOMEAN ####
                data45 = fourtyfive_day_diff(analyte_station_data,date)
                geomean = geo_mean(data45)
                mask = (data['SampleDate'] == date) & (data['StationCode'] == station)
                data.loc[mask]['Geomean45'] = geomean
                #### 60 DAY GEOMEAN ####
                data60 = sixty_day_diff(analyte_station_data,date)
                geomean = geo_mean(data60)
                mask = (data['SampleDate'] == date) & (data['StationCode'] == station) & (data["DW_AnalyteName"] == analyte)
                data.loc[mask]['Geomean60'] = geomean

    return data