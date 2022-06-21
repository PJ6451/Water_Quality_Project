import numpy
import pandas
from scipy import stats
import datetime

def thirty_day_diff(data, start_date):
    #### RETURNS DATA SET ONLY CONTAINING LAST 30 DAYS ####
    end_date = start_date - datetime.timedelta(days = 30)
    mask = (data['SampleDate'] <= start_date) & (data['SampleDate'] >= end_date)
    return data.loc[mask]

def fourtyfive_day_diff(data, start_date):
    #### RETURNS DATA SET ONLY CONTAINING LAST 42 DAYS ####
    end_date = start_date - datetime.timedelta(days = 42)
    mask = (data['SampleDate'] <= start_date) & (data['SampleDate'] >= end_date)
    return data.loc[mask]

def geo_mean(data):
    #### CALCULATES GEOMEAN, RETURNS RELEVANT RESULT ####
    if numpy.size(data):
        if data.shape[0] >= 5:
            return (numpy.prod(data))**(1/data.shape[0])
        else:
            return "Less than five samples, geomean not calculated"
    else:
        return "No data points in time period"

def gm_calc(data):
    #### SLICE DATA BY STATION ####
    for station in data["StationCode"].unique():
        station_data = data[data["StationCode"] == station]
        station_data = station_data[~station_data["ResultQualCode"].isin(['ND','NR','DNQ'])]
        #### SLICE DATA BY ANALYTE/MARKER ####
        for analyte in station_data["DW_AnalyteName"].unique():
            analyte_station_data = station_data[station_data["DW_AnalyteName"] == analyte]
            #### SLICE DATA BY DATE ####
            for date in analyte_station_data["SampleDate"]:
                mask = (data['SampleDate'] == date) & (data['StationCode'] == station) & (data["DW_AnalyteName"] == analyte)
                #### ONLY DO CALCULATIONS FOR DATA WITHIN 12 WEEKS OF MAX ####
                if date > max(analyte_station_data["SampleDate"]) - datetime.timedelta(weeks = 52):
                #### ANALYTES THAT NEED DIFFERENT CALCULATIONS ####
                    gm30markers = ["Enterococcus", "Coliform, Fecal", "Coliform, Total"]
                    gm42markers = ["Enterococcus", "E. coli"]
                    stvmarkers = ["Enterococcus"]
        
                    if analyte in gm30markers:
                        #### 30 DAY GEOMEAN ####
                        data30 = thirty_day_diff(analyte_station_data,date)
                        geomean = geo_mean(data30.loc[:,"Result"])
                        data.loc[mask, 'Geomean30'] = geomean
                    if analyte in gm42markers:
                        #### 42 DAY GEOMEAN ####
                        data45 = fourtyfive_day_diff(analyte_station_data,date)
                        geomean = geo_mean(data45.loc[:,"Result"])
                        data.loc[mask, 'Geomean42'] = geomean
                    if analyte in stvmarkers:
                        #### STV ####
                        pass

    return data