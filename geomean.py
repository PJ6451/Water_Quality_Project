############################
# Script calculates geo-mean
# for relevant dates
############################

import numpy as np
import pandas as pd

def geo_mean(data):
    #### CALCULATES GEOMEAN ####
    return (np.prod(data))**(1/data.shape[0])

def gm_calc_30(data):
    data_to_return = []
    for station in data["StationCode"].unique():
        station_data = data[data["StationCode"] == station]
        station_data["Geomean30"] = station_data['Result'][::-1].rolling("30D", min_periods = 5).apply(geo_mean).values
        data_to_return.append(station_data)
    
    data = pd.concat(data_to_return)
    return data

def gm_calc_42(data):
    data_to_return = []
    for station in data["StationCode"].unique():
        station_data = data[data["StationCode"] == station]
        station_data["Geomean42"] = station_data['Result'][::-1].rolling("42D", min_periods = 5).apply(geo_mean).values
        data_to_return.append(station_data)
    
    data = pd.concat(data_to_return)
    return data