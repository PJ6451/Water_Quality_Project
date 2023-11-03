import polars as pl
import requests


class CalOpenDataPortalAPI:
    def __init__(self, date):
        self.api_columns = '"StationCode","DW_AnalyteName",  "SampleDate","TargetLatitude","TargetLongitude","Unit","Program","ParentProject","Project","MethodName","RL","Result","ResultQualCode"'
        self.api_table = '"1987c159-ce07-47c6-8d4f-4483db6e6460"'
        self.columns_for_polars = [
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
            "ResultQualCode",
        ]
        self.date_for_query_cond = date

    def create_url(self) -> str:
        #### CREATE SQL QUERY, URL FOR API CALL ####
        cond = f"'SampleDate' > DATE '{self.date_for_query_cond}'"
        query = f"SELECT {self.api_columns} FROM {self.api_table} WHERE {cond}"
        url = "https://data.ca.gov/api/3/action/datastore_search_sql?sql= "
        return url + query

    @staticmethod
    def api_call(url_query: str) -> dict:
        #### API CALL ####
        response = requests.request("GET", url_query)
        print("Endpoint Response Code: = " + str(response.status_code))
        if response.status_code != 200:
            raise Exception(response.status_code, response.text)
        return response.json()

    def get_data(self) -> pl.DataFrame:
        url_query = self.create_url()
        my_dict = self.api_call(url_query)
        jsdata = my_dict["result"]["records"]
        return pl.DataFrame(jsdata)[self.columns_for_polars]
