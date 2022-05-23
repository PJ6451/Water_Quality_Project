import requests

def create_url() -> str:
    #Create query, url
    columns = '"StationCode","DW_AnalyteName","SampleDate","TargetLatitude","TargetLongitude","Unit","Program","ParentProject","Project","MethodName","RL","Result","ResultQualCode"'
    table = '"1987c159-ce07-47c6-8d4f-4483db6e6460"'
    cond  = ' "ParentProject" LIKE \'%San Diego%\' '
    query = 'SELECT ' + columns + ' FROM ' + table + ' WHERE ' + cond
    url = 'https://data.ca.gov/api/3/action/datastore_search_sql?sql= '
    return url + query

def api_call(url_query:str) -> dict:
    #API call
    response = requests.request("GET", url_query)
    print("Endpoint Response Code: = " + str(response.status_code))
    if response.status_code != 200:
        raise Exception(response.status_code,response.text)
    return response.json()

def get_data() -> list:
    #get url string
    url_query = create_url()

    #API call
    my_dict = api_call(url_query)

    #Parse out raw data
    data = my_dict['result']['records']

    return data