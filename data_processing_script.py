import polars as pl
from DataProcessing import DataProcessing
import time
from tqdm import tqdm

if __name__ == "__main__":
    url_paths = [
        "https://data.ca.gov/dataset/6723ab78-4530-4e97-ba5e-6ffd17a4c139/resource/18c57345-bf87-4c46-b358-b634d36be4d2/download/safetoswim_1969-2010_2023-10-27.csv",
        "https://data.ca.gov/dataset/6723ab78-4530-4e97-ba5e-6ffd17a4c139/resource/7639446f-8c62-43d9-a526-8bc7952dd8bd/download/safetoswim_2010-2020_2023-10-23.csv",
        "https://data.ca.gov/dataset/6723ab78-4530-4e97-ba5e-6ffd17a4c139/resource/1987c159-ce07-47c6-8d4f-4483db6e6460/download/safetoswim_2020-present_2023-10-27.csv",
    ]
    data_proc = DataProcessing()
    processed_data = []
    start = time.perf_counter()
    for path in tqdm(url_paths):
        processed_data.append(data_proc.data_processing(path))
    processed_data = pl.concat(processed_data)
    print(f"Done in {(time.perf_counter()-start)/60} minutes")
    print("bleh")
