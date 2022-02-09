# from pandas.tseries.offsets import DateOffset
import numpy as np
import pandas as pd
import geopandas as gpd
from pathlib import Path
from shapely import wkt
import json
import numpy as np
import logging

logging.basicConfig(level=logging.INFO)


class Summarizer:
    def __init__(
        self,
        dir_cwd: Path,
        dir_zip: Path,
        dir_csv: Path,
        dir_out: Path,
        dir_summary: Path,
    ):
        """Initialize a CBAnalysis instance with the cwd

        Args:
            start_cwd ([type], optional): [description]. Defaults to Path("./..").

        Returns:
            [type]: [description]
        """
        self.dir_cwd = dir_cwd
        self.dir_zip = dir_zip
        self.dir_csv = dir_csv
        self.dir_out = dir_out
        self.dir_summary = dir_summary

    def agg_by_hour(self, df, orient="start", station=None) -> pd.DataFrame:
        """Given a df, aggregate the count of all rides in each hour for every station.

        Args:
            df ([type]): Dataframe of all rides; ideally concatted from multiple months
            orient (str, optional): Consider `start` station or `end` station. Defaults to 'start'.
            station ([type], optional): Station to filter by. Defaults to None.

        Returns:
            [type]: DataFrame
        """
        # first, resample to every hour, counting all of the trips per hour
        df1 = (
            df[[f"{orient}time", f"{orient}_station_id"]]
            .set_index(f"{orient}time", drop=True)
            .groupby(f"{orient}_station_id")
            .resample("1H")
            .count()
            .rename(columns={f"{orient}_station_id": "counts"})
            .reset_index()
        )
        # Get time fields for grouping
        df1[f"{orient}_weekday"] = df1[f"{orient}time"].dt.weekday
        df1[f"{orient}_hour"] = df1[f"{orient}time"].dt.hour
        # Now we aggregate, and get the count per hour and per weekday
        by_hr = df1.groupby(
            [f"{orient}_station_id", f"{orient}_weekday", f"{orient}_hour"]
        ).agg([np.sum])
        by_hr.columns = by_hr.columns.droplevel(0)
        # by_hr.columns = ['start_station_id', 'start_weekday', 'start_hour', 'ride_sum', 'ride_mean']
        # by_hr.to_json('byhr.json', orient="records")
        if station == None:
            return by_hr
        else:
            idx = pd.IndexSlice
            return by_hr.loc[idx[station, :, :]]

    def count_stations_per_nta(df: pd.DataFrame) -> pd.DataFrame:
        """Given a df of rides with NTAs identified, count # of stations per NTA.

        Args:
            df ([type]): DataFrame of rides

        Returns:
            pd.DataFrame: [description] DataFrame
        """
        stations_per_nta = (
            df[["ntacode", "start_station_id"]]
            .groupby("ntacode")[["start_station_id"]]
            .nunique()
            .reset_index()
            .set_index("ntacode")
            .rename({"start_station_id": "stations_count"}, axis=1)
        )
        return stations_per_nta

    # Given df of rides with NTA's joined, count the number of rides and rank them within each group.
    def rank_stations_by_nta(df, df_stations_per_nta):
        stations_ranked_by_nta = (
            df[["uuid", "ntacode", "start_station_id"]]
            .groupby(["ntacode", "start_station_id"])
            .count()
            .sort_values(by=["ntacode", "uuid"], ascending=False)
            .groupby(["ntacode"])
            .rank(method="dense", ascending=False, pct=False)
            .reset_index()
            .rename({"uuid": "station_rank"}, axis=1)
            .merge(df_stations_per_nta, on="ntacode", how="left")
            .set_index("start_station_id")
        )

        return stations_ranked_by_nta

    def compute_aggs_by_hour(self, df):
        """Given all rides, compute the average # of new started rides per hour

        Args:
            df ([type]): [description]

        Returns:
            [type]: [description]
        """
        by_hour_summary = (
            df.reset_index()
            .groupby(["start_station_id", "start_hour"])
            .mean()
            .drop("start_weekday", axis=1)
            .rename({"sum": "mean_rides"}, axis=1)
            .reset_index()
            .round(1)
            .groupby("start_station_id")
            .apply(lambda x: x.to_dict(orient="records"))
            .to_dict()
        )
        return by_hour_summary

    def export_by_hour_json(self, df):
        """Given all rides, compute aggregations by the hour and export to JSON

        Args:
            df ([type]): [description]
        """
        output = self.dir_summary / "./aggs_by_hour.json"
        by_hour_summary = (
            df.reset_index()
            .groupby(["start_station_id", "start_hour"])
            .mean()
            .drop("start_weekday", axis=1)
            .rename({"sum": "mean_rides"}, axis=1)
            .reset_index()
            .round(1)
            .groupby("start_station_id")
            .apply(lambda x: x.to_dict(orient="records"))
            .to_dict()
        )
        logging.info(f"Outputting to {output}")
        # Problem: Create the file if it ∂oesn't exist
        if not self.dir_summary.exists:
            self.dir_summary.mkdir()
        with open(output, "w+") as outfile:
            json.dump(by_hour_summary, outfile)
