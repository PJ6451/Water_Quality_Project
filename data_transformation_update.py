####################################
# This script transforms data called 
# from the Cal Open Data CEDEN API and 
# saves it to the csv made from the 
# historical script. Should be run on
# a regular basis
#################################### 

import ca_open_data_api_2020
import pandas
import numpy
import geomean
import datetime

pandas.options.mode.chained_assignment = None  # default='warn'

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
    
    data["SampleDate"] = pandas.to_datetime(data["SampleDate"])
    data.set_index('SampleDate', inplace=True)
    data = data.sort_index(ascending=False)

    data["Result"] = data["Result"].abs()

    #### ADD SSW COLUMN ####
    data["SSM_WQO"] = data["DW_AnalyteName"].map(ssm_dic)
    data["STV_WQO"] = data["DW_AnalyteName"].map(stv_dic)
    data["GM_30_WQO"] = data["DW_AnalyteName"].map(gm_30_dic)
    data["GM_42_WQO"] = data["DW_AnalyteName"].map(gm_42_dic)

    #### ADD EXCEEDANCE COLUMN ####
    data = ssm_map_exceedance(data)

    #### ADD COLUMNS FOR GEOMEANS ####
    data['STV'] = 'Not Calculated'
    data['Geomean30'] = 'Not Calculated'
    data['Geomean42'] = 'Not Calculated'
    data['STV_Exceedance'] = ''
    data['GM_30_Exceedance'] = ''
    data['GM_42_Exceedance'] = ''
    
    return data

#### LOAD DATA #####
data = ca_open_data_api_2020.get_data()

#### TRANSFORM DATA ####
data = data_transform(data)

#### READ CSV TO CALCULATE GEOMEAN ####
data_old = pandas.read_csv(r'safetoswim_transformed.csv')
one_year = max(data_old["SampleDate"]) - datetime.timedelta(weeks = 54)
data_old = data_old[data_old["SampleDate"] >= one_year]
data = [data,data_old]
data = pandas.concat(data)
del data_old

# separate data into sets for calcualion based on data quality
data_for_calc     = data[~data["ResultQualCode"].isin(['ND','NR','DNQ'])]
data_not_for_calc = data[data["ResultQualCode"].isin(['ND','NR','DNQ'])]

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

#### APPEND TO CSV ####
data.to_csv('safetoswim_transformed.csv', mode='a', header=False)