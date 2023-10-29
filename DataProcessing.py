import polars as pl
from datetime import datetime
import numpy as np


class DataProcessing:
    def __init__(self):
        self.ssm_dic = {
            "Enterococcus": 104,
            "Coliform, Fecal": 400,
            "Coliform, Total": 10000,
        }

        self.gm_30_dic = {
            "Enterococcus": 35,
            "Coliform, Fecal": 200,
            "Coliform, Total": 1000,
        }

        self.gm_42_dic = {"Enterococcus": 30, "E. coli": 100}

        self.stv_dic = {"Enterococcus": 110, "E. coli": 320}
        self.columns_to_keep = [
            "StationCode",
            "Program",
            "Project",
            "TargetLatitude",
            "TargetLongitude",
            "DW_AnalyteName",
            "Unit",
            "SampleDate",
            "MethodName",
            "Result",
            "ResultQualCode",
        ]
        self.position_columns = ["TargetLatitude", "TargetLongitude"]

    def data_processing(self, path):
        data = pl.read_csv(path, ignore_errors=True)
        clean_data = self.data_cleaning(data)
        clean_data_with_cols_maps = self.add_columns_and_dict_mappings(clean_data)
        return self.split_data(clean_data_with_cols_maps)

    def data_cleaning(self, data: pl.DataFrame) -> pl.DataFrame:
        return (
            data.select(self.columns_to_keep)
            .filter((pl.col("Result").is_not_null()))
            .with_columns(
                pl.col("SampleDate").str.strptime(pl.Datetime),
                pl.col("Result").cast(pl.Float64).abs(),
            )
            .filter(
                (~pl.all(pl.col(self.position_columns).is_in([np.NaN, -88.0])))
                & (pl.col("SampleDate") <= datetime.today())
            )
        )

    def add_columns_and_dict_mappings(self, data: pl.DataFrame):
        return data.with_columns(
            pl.col("DW_AnalyteName").map_dict(self.ssm_dic).alias("SSM_WQO"),
            pl.col("DW_AnalyteName").map_dict(self.stv_dic).alias("STV_WQO"),
            pl.col("DW_AnalyteName").map_dict(self.gm_30_dic).alias("GM_30_WQO"),
            pl.col("DW_AnalyteName").map_dict(self.gm_42_dic).alias("GM_42_WQO"),
        )

    def split_data(self, data: pl.DataFrame):
        return data.with_columns(
            pl.when(pl.col("ResultQualCode").is_in(["ND", "NR", "DNQ", "NA"]))
            .then(pl.lit(False))
            .otherwise(pl.lit(True))
            .alias("calc_flag")
        )
