import numpy
import pandas
import scipy

def thirty_day_diff(data):
    return data

def fourtyfive_day_diff(data):
    return data

def sixty_day_diff(data):
    return data

def geo_mean(data):
    num = scipy.stats.gmean(data.loc[:,"Result"])
    return num