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
        df1: pd.DataFrame = (
            df.loc[:, [f"{orient}_time", f"{orient}_short_name"]]
            .set_index(f"{orient}_time", drop=True)
            .groupby(f"{orient}_short_name")
            .resample("1H")
            .count()
            .rename(columns={f"{orient}_short_name": "counts"})
            .reset_index()
        )
        # Get time fields for grouping
        df1[f"{orient}_weekday"] = df1[f"{orient}_time"].dt.weekday
        df1[f"{orient}_hour"] = df1[f"{orient}_time"].dt.hour
        # Now we aggregate, and get the count per hour and per weekday
        # by_weekday_hr = df1.groupby(
        #     [f"{orient}_short_name", f"{orient}_weekday", f"{orient}_hour"]
        # ).aggregate(np.sum)
        # by_weekday_hr.columns = by_weekday_hr.columns.droplevel(0)

        by_hr = (
            df1.groupby([f"{orient}_short_name", f"{orient}_hour"])
            .aggregate(np.sum)
            .reset_index()
            .rename(columns={f"{orient}_short_name": "short_name"})
        )

        by_hr = by_hr.loc[:, ["short_name", "start_hour", "counts"]]
        if station == None:
            return by_hr
        else:
            idx = pd.IndexSlice
            return by_hr.loc[idx[station, :, :]]

    def count_stations_per_nta(self, df_stations: pd.DataFrame) -> pd.DataFrame:
        """Given a df of stations with NTAs identified, count # of stations per NTA.

        Args:
            df ([type]): DataFrame of rides

        Returns:
            pd.DataFrame: [description] DataFrame
        """
        # Use station_id since we're only using station info, not rides
        stations_per_nta = (
            df_stations.loc[:, ["ntacode", "station_id"]]
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
        df_rides = df_rides.astype({"start_short_name": str})

        df_joined = pd.merge(
            left=df_rides,
            right=df_station_geo,
            how="left",
            left_on="start_short_name",
            right_on="short_name",
        )

        stations_ranked_by_nta = (
            df_joined.loc[:, ["uuid", "ntacode", "start_short_name"]]
            .groupby(["ntacode", "start_short_name"])
            .count()
            .sort_values(by=["ntacode", "uuid"], ascending=False)
            .groupby(["ntacode"])
            .rank(method="dense", ascending=False, pct=False)
            .reset_index()
            .rename({"uuid": "station_rank"}, axis=1)
            .merge(df_stations_per_nta, on="ntacode", how="left")
            # Cast type to int
            .astype({"station_rank": "int32"})
            .set_index("start_short_name")
            .rename(columns={"start_short_name": "short_name"})
        )

        return stations_ranked_by_nta

    def join_rankings(
        self, df_station_geo: gpd.GeoDataFrame, df_rankings: pd.DataFrame
    ) -> gpd.GeoDataFrame:
        """Create a GeoDataFrame with ranking data in properties

        Args:
            df_stations (pd.DataFrame): _description_
            df_ranks (pd.DataFrame): _description_
            df_station_counts (pd.DataFrame): _description_
        """

        # Join the rankings onto the station_geo for mapbox
        merged: gpd.GeoDataFrame = pd.merge(
            left=df_station_geo,
            right=df_rankings,
            how="left",
            left_on="short_name",
            # right_on="short_name",
            right_index=True,
        )

        return merged

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
            .groupby(["start_short_name", "start_hour"])
            .mean()
            # .drop("start_weekday", axis=1)
            .rename({"sum": "mean_rides"}, axis=1)
            .reset_index()
            .round(1)
            .groupby("start_short_name")
            .apply(lambda x: x.to_dict(orient="records"))
            .to_dict()
        )
        logging.info(f"Outputting to {output}")
        # Problem: Create the file if it ∂oesn't exist
        if not self.paths.summary.exists:
            self.paths.summary.mkdir()
        with open(output, "w+") as outfile:
            json.dump(by_hour_summary, outfile)
