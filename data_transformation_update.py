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

pandas.options.mode.chained_assignment = None  # default='warn'

def ssm_map_exceedance(data:pandas.DataFrame) -> pandas.DataFrame:  
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

def gm_30_map_exceedance(data:pandas.DataFrame) -> pandas.DataFrame:
    #### LIST OF CONDITIONS ####
    conditions = [
        (data["Geomean30"] > data['GM_30_WQO']),
        (data["Geomean30"] < data['GM_30_WQO']),
        (data["Geomean30"] == data['GM_30_WQO']),
        (data["Geomean30"] == 'Not Calculated')
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
        (data["Geomean42"] == 'Not Calculated')
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

#### APPEND TO CSV ####
data.to_csv('safetoswim_transformed.csv',index=False, mode='a', header=False)

#### READ CSV TO CALCULATE GEOMEAN ####
data = pandas.read_csv(r'safetoswim_transformed.csv',index=False)
data = geomean.gm_calc(data)
data = gm_30_map_exceedance(data)
data = gm_42_map_exceedance(data)
data = stv_42_map_exceedance(data)

#### OVERWRITE CSV ####
data.to_csv('safetoswim_transformed.csv',index=False)