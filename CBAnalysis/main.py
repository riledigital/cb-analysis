__version__ = "0.1.0"
import logging
import os
import pickle
from datetime import date, timedelta
from pathlib import Path
from time import strptime

import pandas as pd

from .data_prep import Prepper
from .reports import (
    export_groups_by_stations,
    export_hourly_sql,
    export_json,
    export_msgpack,
)
from .summarize import Summarizer
from .utils import WorkingPaths

logging.info(f"Version: {__version__}")


class Main:
    def __init__(self, start_dir: Path = None):
        self.paths = WorkingPaths(start_dir, touch=True)
        self.dp = Prepper(self.paths)
        self.summarizer = Summarizer(self.paths)

    def run(self, start_date="2022-01-01", end_date="2022-02-01"):
        # Downlaod data
        logging.info(f"{os.getenv('START_DATE')}-{os.getenv('END_DATE')}")
        fetched = self.fetch(start_date, end_date)
        # Process summaries
        summarized = self.summarize(fetched["stations"], fetched["rides"])
        # Export
        self.export(summarized["hourly"], fetched["stations"], summarized["ranking"])

    def fetch(self, start_date, end_date):
        logging.info(f"Downloading ZIPs from Citi Bike from {start_date} to {end_date}")

        # download for range
        # months_in_range("2021-12-01", "2022-05-01")
        def months_in_range(start, end):
            start = date.fromisoformat(start)
            end = date.fromisoformat(end)
            months = list()
            current = start
            while current <= end:
                months.append(current)
                if current.month == 12:
                    current = current.replace(year=current.year + 1, month=1)
                else:
                    current = current.replace(month=current.month + 1)
            return months

        for target in months_in_range(start_date, end_date):
            logging.info(f"Fetching: {target.year}, {target.month}")
            self.dp.download_ride_zip(
                year=target.year, month=target.month, use_jc=False
            )

        logging.info("Combining CSVs...")
        all_months = self.dp.concat_csvs()

        logging.info("Preparing data...")
        df_rides = self.dp.load_rename_rides(input_merged_rides=all_months)

        logging.info("Loading NTAs...")
        ntas = self.dp.load_ntas()

        logging.info("Fetching station info...")
        stations = self.dp.fetch_station_info()

        df_station_geo = self.dp.sjoin_ntas_stations(ntas, stations)
        return {"ntas": ntas, "rides": df_rides, "stations": df_station_geo}

    def summarize(self, df_station_geo, df_rides):

        logging.info("Aggregating ride data...")
        # In >2021, change id to short_name
        # Brings short_name into df_hourly summaries, which are needed for lookup

        # Generate short_name for start
        df_stations_renamed: pd.DataFrame = df_station_geo.loc[
            :, ["station_id", "short_name"]
        ].rename(
            columns={
                "station_id": "start_station_id",
                "short_name": "start_short_name",
            }
        )
        # Create new col for stop also
        df_stations_renamed["stop_short_name"] = df_stations_renamed["start_short_name"]

        df_rides = df_rides.merge(
            df_stations_renamed,
            how="left",
            left_on="start_station_id",
            right_on="start_short_name",
        )
        df_rides = df_rides.merge(
            df_stations_renamed,
            how="left",
            left_on="stop_station_id",
            right_on="stop_short_name",
        )

        df_rides = df_rides.loc[
            :,
            [
                "ride_id",
                "rideable_type",
                "start_time",
                "stop_time",
                "start_lat",
                "start_lng",
                "end_lat",
                "end_lng",
                "member_casual",
                "uuid",
                "start_hour",
                "start_day",
                "start_weekday",
                "stop_hour",
                "stop_day",
                "stop_weekday",
                "start_station_name",
                # "start_station_id_y",
                "start_short_name_x",
                "stop_short_name_x",
                "start_station_id_x",
                "stop_station_name",
                # "start_short_name_y",
                # "stop_short_name_y",
                # to be filled:
                # "start_station_id",
                # "stop_station_id",
            ],
        ].rename(
            columns={
                # Rewrite erroneous name
                "start_station_id_x": "start_station_id",
                "stop_station_id_x": "stop_station_id",
                # Remove extraneous naming
                "start_short_name_x": "start_short_name",
                "stop_short_name_x": "stop_short_name",
            }
        )

        df_hourly = self.summarizer.agg_by_hour(df_rides)

        # compute rankings
        df_stations_per_nta: pd.DataFrame = self.summarizer.count_stations_per_nta(
            df_station_geo
        )
        logging.info("Computing rankings...")
        df_rankings = self.summarizer.rank_stations_by_nta(
            df_rides, df_station_geo, df_stations_per_nta
        )
        return {"hourly": df_hourly, "ranking": df_rankings}

    def export(self, df_hourly, df_station_geo, df_rankings):
        logging.info("Compiling report...")

        report = {
            "df_hourly": df_hourly,
            "df_station_geo": df_station_geo,
            "df_rankings": df_rankings,
        }

        # # Save a pickle
        # with open("report.pickle", "wb") as handle:
        #     pickle.dump(report, handle, protocol=pickle.HIGHEST_PROTOCOL)

        # Join the rankings onto the station_geo for mapbox
        df_station_geo_ranked = self.summarizer.join_rankings(
            df_rankings=df_rankings, df_station_geo=df_station_geo
        )

        # make a dict with dictionary-form, don't use dataframes
        report_export = dict(
            {
                "df_summary_hourly": export_groups_by_stations(df_hourly),
                "df_station_ranking": df_rankings.to_dict(orient="records"),
            }
        )

        # Save individual report files
        # Save the geojson with special option
        with open(self.paths.out / "station_geo_ranked.geojson", "wb") as file:
            df_station_geo_ranked.to_file(file, driver="GeoJSON")

        for key, data in report_export.items():
            # write the json
            export_json(data, self.paths.out / f"{key}.json")
            export_msgpack(data, self.paths.out / f"{key}.msgpack")

        # Save a packed report file
        # path_report = self.paths.out / "full-report.json"
        # logging.info(f"Saving report to: {path_report}")
        # export_json(json_report, path_report)
        # export_msgpack(json_report, path_report)
        if os.getenv("SQLALCHEMY_CONN") is not None:
            export_hourly_sql(df_hourly)


if __name__ == "__main__":
    logging.info("See cli.py for a runnable mode")
    pass
