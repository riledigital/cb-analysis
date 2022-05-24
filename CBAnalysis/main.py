__version__ = "0.1.0"
from datetime import date, timedelta
from time import strptime
import logging
import os
import pickle

from data_prep import Prepper
from summarize import Summarizer
from utils import WorkingPaths
from reports import make_report, export_json

from pathlib import Path

logging.info(f"Version: {__version__}")


class Main:
    def __init__(self, start_dir: Path = None):
        self.paths = WorkingPaths(start_dir, touch=True)
        self.dp = Prepper(self.paths)
        self.summarizer = Summarizer(self.paths)

    def run(self, start_date, end_date):
        # Downlaod data
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
            while (current <= end):
                months.append(current)
                if (current.month == 12):
                    current = current.replace(year = current.year + 1, month = 1)
                else:
                    current = current.replace(month = current.month + 1)
            return months
        for target in months_in_range(start_date, end_date):
            logging.info(f"Fetching: {target.year}, {target.month}")
            # self.dp.download_ride_zip(year=target.year, month=target.month, use_jc=False)


        # logging.info("Combining CSVs...")
        # all_months = self.dp.concat_csvs()

        logging.info("Preparing data...")
        df_rides = self.dp.load_rename_rides()

        logging.info("Loading NTAs...")
        ntas = self.dp.load_ntas()

        logging.info("Fetching station info...")
        stations = self.dp.fetch_station_info()
        
        df_station_geo = self.dp.sjoin_ntas_stations(ntas, stations)
        return {"ntas": ntas, "rides": df_rides, "stations": df_station_geo}

    def summarize(self, df_station_geo, df_rides):
        df_stations_per_nta = self.summarizer.count_stations_per_nta(df_station_geo)

        logging.info("Aggregating ride data...")
        df_hourly = self.summarizer.agg_by_hour(df_rides)
        logging.info("Exporting report...")
        self.summarizer.export_by_hour_json(df_hourly)

        # compute rankings
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
        with open("report.pickle", "wb") as handle:
            pickle.dump(report, handle, protocol=pickle.HIGHEST_PROTOCOL)

        # json_report = make_report(
        #     df_hourly.to_json(),
        #     df_station_geo.to_json(),
        #     df_rankings.to_json(),
        # )

        # path_report = self.paths.out / "report.json"
        # logging.info(f"Saving report to: {path_report}")
        # export_json(json_report, path_report)


if __name__ == "__main__":
    logging.info("See cli.py for a runnable mode")
    pass
