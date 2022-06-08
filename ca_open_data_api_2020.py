####################################
# This script handles all the API Calls 
# to the Cal Open Data CEDEN database 
# for updating data available to the user
####################################

from numpy import datetime64
import requests
import pandas
import datetime

def get_date() -> str:
    dates = pandas.read_csv(
        r'safetoswim_6-1-2022_transformed.csv',
        usecols = ["SampleDate"], 
        parse_dates = ["SampleDate"]
        )
    dates = dates[dates["SampleDate"] < datetime.datetime.now()]
    date_str = str(dates["SampleDate"].max())
    return date_str[:10]

def create_url(date) -> str:
    #### CREATE SQL QUERY, URL FOR API CALL ####
    columns = '"StationCode","DW_AnalyteName",  "SampleDate","TargetLatitude","TargetLongitude","Unit","Program","ParentProject","Project","MethodName","RL","Result","ResultQualCode"'
    table = '"1987c159-ce07-47c6-8d4f-4483db6e6460"'
    cond  = ' "ParentProject" LIKE \'%San Diego%\' AND "SampleDate" > DATE \'' + date + '\''
    query = 'SELECT ' + columns + ' FROM ' + table + ' WHERE ' + cond
    url = 'https://data.ca.gov/api/3/action/datastore_search_sql?sql= '
    return url + query

def api_call(url_query:str) -> dict:
    #### API CALL ####
    response = requests.request("GET", url_query)
    print("Endpoint Response Code: = " + str(response.status_code))
    if response.status_code != 200:
        raise Exception(response.status_code,response.text)
    return response.json()

def get_data() -> pandas.DataFrame:
    #### GET DATE ####
    #date = get_date()
    date = '05-01-2022'
    #### GET URL STRING ####
    url_query = create_url(date)

    #### API CALL ####
    my_dict = api_call(url_query)

    #### PARSE OUT RAW DATA ####
    jsdata = my_dict['result']['records']
    
    #### TRANSFORM RAW DATA TO PANDAS DATAFRAME ####
    data = pandas.DataFrame(jsdata)

    #### KEEP/REARANGE COLUMNS ####
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