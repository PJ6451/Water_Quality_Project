############################
# Script calculates geo-mean
# for relevant dates
############################

import numpy
import pandas
import datetime

def geo_mean(data):
    #### CALCULATES GEOMEAN ####
    return (numpy.prod(data))**(1/data.shape[0])

def gm_calc(data,window):
    return data.groupby(['StationCode','DW_AnalyteName'])['Result'].transform( lambda x: x.rolling(window, min_periods = 5).apply(geo_mean))