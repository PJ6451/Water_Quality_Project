import pandas
import numpy

def read_data(path) -> pandas.DataFrame:
    #read csv
    data = pandas.read_csv(path)

    #keep only columns you need
    columns = [
        "StationCode",
        "Program",
        "Project",
        "ParentProject",
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

def map_exceedance(data):  
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
    data['Exceedance'] = numpy.select(conditions, values)
    return data

def data_transform(data: list,dic: dict) -> list:
    #drop rows where result null
    data = data[data['Result'].notna()]

    #drop rows where lat/long not usable
    data = data[data["TargetLatitude"] > 0]

    #add ssw column
    data["Single Sample WQO"] = data["DW_AnalyteName"].map(dic)

    # create a new column and use np.select to assign values to it using our lists as arguments
    data = map_exceedance(data)

    return data

#### LOAD DATA #####
data = read_data(r'C:\Users\16617\Downloads\safetoswim_2020-present_2022-05-23.csv')

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
data.to_csv('safetoswim_1969-2010_2022-05-23_transformed.csv')
