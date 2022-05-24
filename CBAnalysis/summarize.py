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
    def __init__(self, paths: Path):
        """Initialize a CBAnalysis instance with the cwd

        Args:
            start_cwd ([type], optional): [description]. Defaults to Path("./..").

        Returns:
            [type]: [description]
        """
        self.paths = paths

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
            df[[f"{orient}_time", f"{orient}_station_id"]]
            .set_index(f"{orient}_time", drop=True)
            .groupby(f"{orient}_station_id")
            .resample("1H")
            .count()
            .rename(columns={f"{orient}_station_id": "counts"})
            .reset_index()
        )
        # Get time fields for grouping
        df1[f"{orient}_weekday"] = df1[f"{orient}_time"].dt.weekday
        df1[f"{orient}_hour"] = df1[f"{orient}_time"].dt.hour
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

    def count_stations_per_nta(self, df: pd.DataFrame) -> pd.DataFrame:
        """Given a df of rides with NTAs identified, count # of stations per NTA.

        Args:
            df ([type]): DataFrame of rides

        Returns:
            pd.DataFrame: [description] DataFrame
        """
        # Use station_id since we're only using station info, not rides
        stations_per_nta = (
            df[["ntacode", "station_id"]]
            .groupby("ntacode")[["station_id"]]
            .nunique()
            .reset_index()
            .set_index("ntacode")
            .rename({"station_id": "stations_count"}, axis=1)
        )
        return stations_per_nta

    def rank_stations_by_nta(
        self,
        df_rides: pd.DataFrame,
        df_station_geo: pd.DataFrame,
        df_stations_per_nta: pd.DataFrame,
    ) -> pd.DataFrame:
        """Given df of rides with NTA's joined,
         count the number of rides and rank them within each group.

        Args:
            df (pd.DataFrame): [description] DataFrame of rides with NTA's joined
            df_stations_per_nta (pd.DataFrame): [description] DataFrame of NTA's with station counts

        Returns:
            pd.DataFrame: [description]
        """

        # join the geo data onto rides
        df_rides = df_rides.astype({"start_station_id": str})

        df_joined = df_rides.merge(
            df_station_geo,
            how="left",
            left_on="start_station_id",
            right_on="station_id",
        )

        stations_ranked_by_nta = (
            df_joined[["uuid", "ntacode", "start_station_id"]]
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
        output = self.paths.summary / "./aggs_by_hour.json"
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
        if not self.paths.summary.exists:
            self.paths.summary.mkdir()
        with open(output, "w+") as outfile:
            json.dump(by_hour_summary, outfile)
