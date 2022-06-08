####################################
# This script transforms data called 
# from the Cal Open Data CEDEN crv's
# to be transformed and combined into
# a single csv for later use
#################################### 

import pandas
import numpy
import geomean

pandas.options.mode.chained_assignment = None  # default='warn'

def read_data(path:str) -> pandas.DataFrame:
    #### READ CSV ####
    data = pandas.read_csv(path,low_memory=False)

    #### KEEP/RAARANGE COLUMNS ####
    columns = [
        "StationCode",
        "Program",
        "Project",
        "TargetLatitude",
        "TargetLongitude",
        "Analyte",
        "Unit",
        "SampleDate",
        "MethodName",
        "RL",
        "Result",
        "ResultQualCode"
        ]

    data = data[columns]
    return data

def map_exceedance(data:pandas.DataFrame) -> pandas.DataFrame:  
    #### LIST OF CONDITIONS ####
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
    #### LIST OF VALUES FOR EACH CONDITION ####
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
    #### MAP VALUES ####
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
    for column in columns:
        data[column].loc[data[column] == -88] = 0
        data[column].loc[data[column].isna()] = 0

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

    data = geomean.geo_mean(data)

    return data

#### LOAD DATA #####
data_1969_2010    = read_data(r'https://data.ca.gov/dataset/6723ab78-4530-4e97-ba5e-6ffd17a4c139/resource/18c57345-bf87-4c46-b358-b634d36be4d2/download/safetoswim_1969-2010_2022-06-01.csv')
data_2010_2020    = read_data(r'https://data.ca.gov/dataset/6723ab78-4530-4e97-ba5e-6ffd17a4c139/resource/7639446f-8c62-43d9-a526-8bc7952dd8bd/download/safetoswim_2010-2020_2022-06-01.csv')
data_2020_present = read_data(r'https://data.ca.gov/dataset/6723ab78-4530-4e97-ba5e-6ffd17a4c139/resource/1987c159-ce07-47c6-8d4f-4483db6e6460/download/safetoswim_2020-present_2022-06-01.csv')

#### DICTIONARY ####
dic = {
    "Enterococcus":110,
    "E. coli":320,
    "Coliform, Fecal":400,
    "Coliform, Total":10000,
    "HF183":100
}

#### TRANSFORM DATA ####
data1 = data_transform(data_1969_2010,dic)
data2 = data_transform(data_2010_2020,dic)
data3 = data_transform(data_2020_present,dic)

#### COMINE DATA ####
data = [data1,data2,data3]
result = pandas.concat(data)

#### SAVE TO CSV ####
result.to_csv('safetoswim_6-8-2022_transformed.csv',index=False)