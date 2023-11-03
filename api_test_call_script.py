import polars as pl
from safe2swimETL.CalOpenDataPortalAPI import CalOpenDataPortalAPI
import time

if __name__ == "__main__":
    api = CalOpenDataPortalAPI("10/31/2023")
    start = time.perf_counter()
    data = api.get_data()
    print(f"Time to get data: {time.perf_counter() - start}")
    print("fuck")
