import pandas
import numpy

def read_data(path:str) -> pandas.DataFrame:
    #read csv
    data = pandas.read_csv(path,low_memory=False)

    #keep only columns you need
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
data1 = read_data(r'C:\Users\16617\Downloads\safetoswim_1969-2010_2022-05-23.csv')
data2 = read_data(r'C:\Users\16617\Downloads\safetoswim_2010-2020_2022-05-23.csv')
data3 = read_data(r'C:\Users\16617\Downloads\safetoswim_2020-present_2022-05-23.csv')

#### DICTIONARY ####
dic = {
    "Enterococcus":110,
    "E. coli":320,
    "Coliform, Fecal":400,
    "Coliform, Total":10000,
}

#### TRANSFORM DATA ####
data1 = data_transform(data1,dic)
data2 = data_transform(data2,dic)
data3 = data_transform(data3,dic)

#### COMINE DATA ####
data = [data1,data2,data3]
result = pandas.concat(data)

#### SAVE TO CSV ####
result.to_csv('safetoswim_1969-present_2022-05-23_transformed.csv')