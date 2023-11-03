import polars as pl
from safe2swimETL.DataProcessing import DataProcessing
import time

if __name__ == "__main__":
    # for the path, go here: https://data.ca.gov/dataset/surface-water-fecal-indicator-bacteria-results/resource/1987c159-ce07-47c6-8d4f-4483db6e6460
    # copy the URL below "Safe to Swim 2020"
    url_paths = [
        "https://data.ca.gov/dataset/6723ab78-4530-4e97-ba5e-6ffd17a4c139/resource/1987c159-ce07-47c6-8d4f-4483db6e6460/download/safetoswim_2020-present_2023-11-02.csv",
    ]
    data_proc = DataProcessing()
    processed_data = []
    start = time.perf_counter()
    for path in url_paths:
        processed_data.append(
            data_proc.data_processing(pl.read_csv(path, ignore_errors=True))
        )
    processed_data = pl.concat(processed_data)
    print(f"Done in {(time.perf_counter()-start)/60} minutes")
