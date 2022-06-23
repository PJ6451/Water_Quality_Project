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
        (data["ResultQualCode"] == 'P') & (data['Result'] > data["SSM_WQO"]),
        (pandas.isna(data["SSM_WQO"]))
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
        'Equals Limit',
        'Does Not Exceed Limit',
        'Does Exceed Limit',
        'Not Applicable'
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
        (pandas.isna(data["Geomean30"]))
        ]
    #### LIST OF VALUES FOR EACH CONDITION ####
    values = [
        'Does Exceed Limit', 
        'Does Not Exceed Limit',
        'Equals Limit',  
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
        (pandas.isna(data["Geomean42"]))
        ]
    #### LIST OF VALUES FOR EACH CONDITION ####
    values = [
        'Does Exceed Limit', 
        'Does Not Exceed Limit',
        'Equals Limit',  
        'Not Determined'
        ]
    #### MAP VALUES ####
    data['GM_42_Exceedance'] = numpy.select(conditions, values)
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
    
    data = data[data["TargetLatitude"] < 33.717]
    data = data[data["TargetLongitude"] < -116.289]

    #### CHANGE DATA TYPES ####
    num_cols = [
        "TargetLatitude",
        "TargetLongitude",
        "Result"
    ]

    for col in num_cols:
        data[col] = pandas.to_numeric(data[col])
    
    data["SampleDate"] = pandas.to_datetime(data["SampleDate"])
    data = data.sort_values(by="SampleDate",ascending=False)
    data.set_index('SampleDate',inplace = True)
    
    data["Result"] = data["Result"].abs()

    #### ADD WQO COLUMNS ####
    data["SSM_WQO"] = data["DW_AnalyteName"].map(ssm_dic)
    data["GM_30_WQO"] = data["DW_AnalyteName"].map(gm_30_dic)
    data["GM_42_WQO"] = data["DW_AnalyteName"].map(gm_42_dic)

    #### ADD COLUMNS FOR GEOMEANS ####
    data['Geomean30'] = numpy.nan
    data['Geomean42'] = numpy.nan

    return data


#### LOAD DATA #####
data1969    = read_data(r'https://data.ca.gov/dataset/6723ab78-4530-4e97-ba5e-6ffd17a4c139/resource/18c57345-bf87-4c46-b358-b634d36be4d2/download/safetoswim_1969-2010_2022-06-22.csv')
data2010    = read_data(r'https://data.ca.gov/dataset/6723ab78-4530-4e97-ba5e-6ffd17a4c139/resource/7639446f-8c62-43d9-a526-8bc7952dd8bd/download/safetoswim_2010-2020_2022-06-22.csv')
data2020    = read_data(r'https://data.ca.gov/dataset/6723ab78-4530-4e97-ba5e-6ffd17a4c139/resource/1987c159-ce07-47c6-8d4f-4483db6e6460/download/safetoswim_2020-present_2022-06-22.csv')

#### COMBINE AND TRANSFORM DATA ####
data123 = [data1969,data2010,data2020]
data123 = pandas.concat(data123)
data123 = data_transform(data123)
del data1969,data2010,data2020


#### DO CALCULATIONS/MAPPING ####
# separate data into sets for calcualion based on data quality
data_for_calc     = data123[~data123["ResultQualCode"].isin(['ND','NR','DNQ'])]
data_not_for_calc = data123[data123["ResultQualCode"].isin(['ND','NR','DNQ'])]
del data123

# separate data into sets for calcualion based on reg requirements
data_entero     = data_for_calc[data_for_calc["DW_AnalyteName"] == "Enterococcus"] 
data_ecoli      = data_for_calc[data_for_calc["DW_AnalyteName"] == "E. coli"]
data_fecal_coli = data_for_calc[data_for_calc["DW_AnalyteName"] == "Coliform, Fecal"]
data_total_coli = data_for_calc[data_for_calc["DW_AnalyteName"] == "Coliform, Total"] 

# Perform calculations
data_fecal_coli = geomean.gm_calc_30(data_fecal_coli)
data_total_coli = geomean.gm_calc_30(data_total_coli)
data_entero     = geomean.gm_calc_30(data_entero)
data_entero     = geomean.gm_calc_42(data_entero)
data_ecoli      = geomean.gm_calc_42(data_ecoli)

# recombine datasets, do exceedance mapping
data = [data_entero,data_ecoli,data_fecal_coli,data_total_coli,data_not_for_calc]
data = pandas.concat(data)
del data_fecal_coli,data_entero,data_ecoli,data_total_coli,data_for_calc,data_not_for_calc
data = data.sort_index(ascending=False)
data = ssm_map_exceedance(data)
data = gm_30_map_exceedance(data)
data = gm_42_map_exceedance(data)

#### SAVE TO CSV ####
data.to_csv('safetoswim_sandeigo_transformed.csv')