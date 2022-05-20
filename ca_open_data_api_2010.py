import requests

def get_data() -> list:
    #Create query, url
    columns = '"StationCode","DW_AnalyteName","SampleDate","TargetLatitude","TargetLongitude","Unit","Program","ParentProject","Project","MethodName","RL","Result","ResultQualCode"'
    table = '"7639446f-8c62-43d9-a526-8bc7952dd8bd"'
    cond  = ' "ParentProject" LIKE \'%San Diego%\' '
    query = 'SELECT ' + columns + ' FROM ' + table + ' WHERE ' + cond
    url = 'https://data.ca.gov/api/3/action/datastore_search_sql?sql= '
    url_query = url + query

    #API call
    r = requests.get(url_query)
    my_dict = r.json()

    #Parse out raw data
    data = my_dict['result']['records']

    return data