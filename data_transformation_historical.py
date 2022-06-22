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
        "DW_AnalyteName",
        "Unit",
        "SampleDate",
        "MethodName",
        "RL",
        "Result",
        "ResultQualCode"
        ]

    data = data[columns]
    return data

def ssm_map_exceedance(data:pandas.DataFrame) -> pandas.DataFrame:  
    #### LIST OF CONDITIONS ####
    conditions = [
        (data["ResultQualCode"] == '=') & (data['Result'] > data["SSM_WQO"]),
        (data["ResultQualCode"] == '>') & (data['Result'] > data["SSM_WQO"]),
        (data["ResultQualCode"] == '>=') & (data['Result'] > data["SSM_WQO"]),
        (data["ResultQualCode"] == '<') & (data['Result'] > data["SSM_WQO"]),
        (data["ResultQualCode"] == '<=') & (data['Result'] > data["SSM_WQO"]),
        (data["ResultQualCode"] == '=') & (data['Result'] < data["SSM_WQO"]),
        (data["ResultQualCode"] == '<') & (data['Result'] < data["SSM_WQO"]),
        (data["ResultQualCode"] == '<=') & (data['Result'] < data["SSM_WQO"]),
        (data["ResultQualCode"] == '>') & (data['Result'] < data["SSM_WQO"]),
        (data["ResultQualCode"] == '>=') & (data['Result'] < data["SSM_WQO"]),
        (data['ResultQualCode'] == 'ND'),
        (data['ResultQualCode'] == 'NR'),
        (data['ResultQualCode'] == 'DNQ'),
        (data['Result'] == data["SSM_WQO"]),
        (data["ResultQualCode"] == 'P') & (data['Result'] < data["SSM_WQO"]),
        (data["ResultQualCode"] == 'P') & (data['Result'] > data["SSM_WQO"])
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
    data['SSM_Exceedance'] = numpy.select(conditions, values)
    return data

def gm_30_map_exceedance(data:pandas.DataFrame) -> pandas.DataFrame:
    #### LIST OF CONDITIONS ####
    conditions = [
        (data["Geomean30"] > data['GM_30_WQO']),
        (data["Geomean30"] < data['GM_30_WQO']),
        (data["Geomean30"] == data['GM_30_WQO']),
        (data["Geomean30"] == numpy.nan)
        ]
    #### LIST OF VALUES FOR EACH CONDITION ####
    values = [
        'Does Exceed Limit', 
        'Does Not Exceed Limit',
        'Does Not Exceed Limit',  
        'Not Determined'
        ]
    #### MAP VALUES ####
    data['GM_30_Exceedance'] = numpy.select(conditions, values)
    return data  

def gm_42_map_exceedance(data:pandas.DataFrame) -> pandas.DataFrame:
    #### LIST OF CONDITIONS ####
    conditions = [
        (data["Geomean42"] > data['GM_42_WQO']),
        (data["Geomean42"] < data['GM_42_WQO']),
        (data["Geomean42"] == data['GM_42_WQO']),
        (data["Geomean42"] == numpy.nan)
        ]
    #### LIST OF VALUES FOR EACH CONDITION ####
    values = [
        'Does Exceed Limit', 
        'Does Not Exceed Limit',
        'Does Not Exceed Limit',  
        'Not Determined'
        ]
    #### MAP VALUES ####
    data['GM_42_Exceedance'] = numpy.select(conditions, values)
    return data

def stv_42_map_exceedance(data:pandas.DataFrame) -> pandas.DataFrame:
    #### LIST OF CONDITIONS ####
    conditions = [
        (data["STV"] > data['STV_WQO']),
        (data["STV"] < data['STV_WQO']),
        (data["STV"] == data['STV_WQO']),
        (data["STV"] == 'Not Calculated')
        ]
    #### LIST OF VALUES FOR EACH CONDITION ####
    values = [
        'Does Exceed Limit', 
        'Does Not Exceed Limit',
        'Does Not Exceed Limit',  
        'Not Determined'
        ]
    #### MAP VALUES ####
    data['STV_Exceedance'] = numpy.select(conditions, values)
    return data

def data_transform(data: pandas.DataFrame) -> pandas.DataFrame:
    #### DICTIONARIES ####
    ssm_dic = {
        "Enterococcus":104,
        "Coliform, Fecal":400,
        "Coliform, Total":10000
    }

    gm_30_dic = {
        "Enterococcus":35,
        "Coliform, Fecal":200,
        "Coliform, Total":1000
    }

    gm_42_dic = {
        "Enterococcus":30,
        "E. coli":100
    }

    stv_dic = {
        "Enterococcus":110,
        "E. coli":320
    }

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
    data.set_index('SampleDate', inplace=True)
    data = data.sort_index(ascending=False)
    
    data["Result"] = data["Result"].abs()

    #### ADD WQO COLUMNS ####
    data["SSM_WQO"] = data["DW_AnalyteName"].map(ssm_dic)
    data["STV_WQO"] = data["DW_AnalyteName"].map(stv_dic)
    data["GM_30_WQO"] = data["DW_AnalyteName"].map(gm_30_dic)
    data["GM_42_WQO"] = data["DW_AnalyteName"].map(gm_42_dic)

    #### ADD COLUMNS FOR GEOMEANS ####
    data['STV'] = numpy.nan
    data['Geomean30'] = numpy.nan
    data['Geomean42'] = numpy.nan

    return data


#### LOAD DATA #####
data1    = read_data(r'https://data.ca.gov/dataset/6723ab78-4530-4e97-ba5e-6ffd17a4c139/resource/18c57345-bf87-4c46-b358-b634d36be4d2/download/safetoswim_1969-2010_2022-06-17.csv')
data2    = read_data(r'https://data.ca.gov/dataset/6723ab78-4530-4e97-ba5e-6ffd17a4c139/resource/7639446f-8c62-43d9-a526-8bc7952dd8bd/download/safetoswim_2010-2020_2022-06-17.csv')
data3    = read_data(r'https://data.ca.gov/dataset/6723ab78-4530-4e97-ba5e-6ffd17a4c139/resource/1987c159-ce07-47c6-8d4f-4483db6e6460/download/safetoswim_2020-present_2022-06-17.csv')

#### COMBINE AND TRANSFORM DATA ####
data12 = [data1,data2]
data12 = pandas.concat(data12)
data12 = data_transform(data12)
data3 = data_transform(data3)


#### DO CALCULATIONS/MAPPING ####
gm30markers = ["Enterococcus", "Coliform, Fecal", "Coliform, Total"]
gm42markers = ["Enterococcus", "E. coli"]

# separate data into sets for calcualion based on data quality
data_for_calc = data3[~data3["ResultQualCode"].isin(['ND','NR','DNQ'])]
data_not_for_calc = data3[data3["ResultQualCode"].isin(['ND','NR','DNQ'])]

# separate data into sets for calcualion based on reg requirements 
data_calc_30 = data_for_calc[data_for_calc["DW_AnalyteName"].isin(gm30markers)]
data_calc_42 = data_for_calc[data_for_calc["DW_AnalyteName"].isin(gm42markers)]

# Perform calculations
data_calc_30 = geomean.gm_calc(data_calc_30,"30D")
data_calc_42 = geomean.gm_calc(data_calc_30,"42D")

# recombine datasets, do exceedance mapping
data3 = [data_calc_30,data_calc_42,data_not_for_calc]
data3 = pandas.concat(data3)
data = [data12,data3]
data = pandas.concat(data12)
data = data.sort_index(ascending=False)
data = ssm_map_exceedance(data)
data = gm_30_map_exceedance(data)
data = gm_42_map_exceedance(data)

#### SAVE TO CSV ####
data.to_csv('safetoswim_transformed.csv',index=False)