from ca_open_data_api_2020 import *
import pandas

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
    for column in columns:
        data[column].loc[data[column] == -88] = 0
        data[column].loc[data[column].isna()] = 0

    #### ADD SSW COLUMN ####
    data["Single Sample WQO"] = data["DW_AnalyteName"].map(dic)

    #### ADD EXCEEDANCE COLUMN ####
    data = map_exceedance(data)

    #### ADD COLUMNS FOR MONTHS, YEARS ####

    #### ADD GEOMEAN COLUMNS ####
    data["Entero_GM_1"] = 0
    data["Entero_GM_2"] = 0
    data["Entero_GM_3"] = 0
    data["E_coli_GM_1"] = 0
    data["E_coli_GM_2"] = 0
    data["E_coli_GM_3"] = 0
    data["Colif_fec_GM_1"] = 0
    data["Colif_fec_GM_2"] = 0
    data["Colif_fec_GM_3"] = 0
    data["Colif_tot_GM_1"] = 0
    data["Colif_tot_GM_2"] = 0
    data["Colif_tot_GM_3"] = 0

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


