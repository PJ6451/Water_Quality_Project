####################################
# This script transforms data called 
# from the Cal Open Data CEDEN API and 
# saves it to the csv made from the 
# historical script. Should be run on
# a regular basis
#################################### 

from ca_open_data_api_2020 import *
import pandas
import numpy
import geomean

pandas.options.mode.chained_assignment = None  # default='warn'

def map_exceedance(data:pandas.DataFrame) -> pandas.DataFrame:  
    # create a list of our conditions
    conditions = [
        (data["ResultQualCode"] == '=') & (data['Result'] > data["Single Sample WQO"]),
        (data["ResultQualCode"] == '>') & (data['Result'] > data["Single Sample WQO"]),
        (data["ResultQualCode"] == '>=') & (data['Result'] > data["Single Sample WQO"]),
        (data["ResultQualCode"] == '<') & (data['Result'] > data["Single Sample WQO"]),
        (data["ResultQualCode"] == '<=') & (data['Result'] > data["Single Sample WQO"]),
        (data["ResultQualCode"] == '=') & (data['Result'] < data["Single Sample WQO"]),
        (data["ResultQualCode"] == '<') & (data['Result'] < data["Single Sample WQO"]),
        (data["ResultQualCode"] == '<=') & (data['Result'] < data["Single Sample WQO"]),
        (data["ResultQualCode"] == '>') & (data['Result'] < data["Single Sample WQO"]),
        (data["ResultQualCode"] == '>=') & (data['Result'] < data["Single Sample WQO"]),
        (data['ResultQualCode'] == 'ND'),
        (data['ResultQualCode'] == 'NR'),
        (data['ResultQualCode'] == 'DNQ'),
        (data['Result'] == data["Single Sample WQO"]),
        (data["ResultQualCode"] == 'P') & (data['Result'] < data["Single Sample WQO"]),
        (data["ResultQualCode"] == 'P') & (data['Result'] > data["Single Sample WQO"])
        ]
    # create a list of the values we want to assign for each condition
    values = [
        'Does Exceed Limit', 
        'Does Exceed Limit',
        'Does Exceed Limit', 
        'May Exceed Limit',
        'May Exceed Limit', 
        'Does Not Exceed Limit',
        'Does Not Exceed Limit',  
        'Does Not Exceed Limit',
        'May Exceed Limit', 
        'May Exceed Limit', 
        'Not Detected/Recorded',
        'Not Detected/Recorded',
        'Not Detected/Recorded',
        'Does Not Exceed Limit',
        'Does Not Exceed Limit',
        'Does Exceed Limit'
        ]
    # map values
    data['Exceedance'] = numpy.select(conditions, values)
    return data

def data_transform(data: pandas.DataFrame, dic: dict) -> pandas.DataFrame:
    #### DROP NULL ROWS ####
    data = data[data['Result'].notna()]

    #### RESET LAT/LONGS FOR UNUSABLE VALUES ####
    columns = [
        "TargetLatitude",
        "TargetLongitude"
    ]

    for col in columns:
        data[col] = numpy.where((data[col] == "NaN"),0,data[col])
        data[col] = numpy.where((data[col] == "-88.0"),0,data[col])
    
    #### CHANGE DATA TYPES ####
    num_cols = [
        "TargetLatitude",
        "TargetLongitude",
        "Result"
    ]

    for col in num_cols:
        data[col] = pandas.to_numeric(data[col])
    
    dates = data["SampleDate"]
    dates_split = dates.str.split(pat="T",expand=True)
    data["SampleDate"] = pandas.to_datetime(dates_split[0])

    #### ADD SSW COLUMN ####
    data["Single Sample WQO"] = data["DW_AnalyteName"].map(dic)

    #### ADD EXCEEDANCE COLUMN ####
    data = map_exceedance(data)

    #### ADD COLUMNS FOR MONTHS, YEARS ####
    data['Year'] = pandas.to_datetime(data["SampleDate"]).dt.year
    data['Month'] = pandas.to_datetime(data["SampleDate"]).dt.month
    data['Day'] = pandas.to_datetime(data["SampleDate"]).dt.day

    #### ADD COLUMNS FOR GEOMEANS ####
    data['Geomean30'] = 0
    data['Geomean45'] = 0
    data['Geomean60'] = 0
    
    return data

#### LOAD DATA #####
data = get_data()

#### DICTIONARY ####
dic = {
    "Enterococcus":110,
    "E. coli":320,
    "Coliform, Fecal":400,
    "Coliform, Total":10000,
}

#### TRANSFORM DATA ####
data = data_transform(data,dic)

#### SAVE TO CSV ####
data.to_csv('safetoswim_6-8-2022_transformed.csv',index=False, mode='a', header=False)