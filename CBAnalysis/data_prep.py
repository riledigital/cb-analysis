from tqdm import tqdm
from tempfile import TemporaryDirectory
from pathlib import Path
import logging
import pandas as pd
import geopandas as gpd

# modifies pandas
import janitor

# import matplotlib.pyplot as plt
import requests
import zipfile
import uuid
import glob
import os
from .utils import touchdir

logging.basicConfig(level=logging.INFO)


class Prepper:
    """CBAnalysis can download and analyze
    data for Citi Bike planner. It is modular for use with Airflow or simple scripts"""

    URL_STATION_FEED = "https://gbfs.citibikenyc.com/gbfs/en/station_information.json"
    URL_NYCNTAS_JSON = "https://data.cityofnewyork.us/api/geospatial/d3qk-pfyz?method=export&format=GeoJSON"

    def __init__(self, paths):
        """Initialize a CBAnalysis instance with the cwd

        Args:
            start_cwd ([type], optional): [description]. Defaults to Path("./..").

        Returns:
            [type]: [description]
        """
        self.paths = paths

    def download_ride_zip(self, output=Path("csv/"), year=2020, month=8, use_jc=False):
        """Downloads ZIP files and unzips them to output

        Args:
            output ([type], optional): [description]. Defaults to Path("csv/").
            year (int, optional): [description]. Defaults to 2020.
            month (int, optional): [description]. Defaults to 8.
            use_jc (bool, optional): Download Jersey City files. Defaults to False.

        Returns:
            [type]: [description]
        """
        base = "https://s3.amazonaws.com/tripdata/"
        touchdir("zip")

        def make_url(year, month):
            return (
                f"{'JC-' if use_jc else ''}{year}{month:0>2}-citibike-tripdata.csv.zip"
            )

        filename = make_url(year, month)
        zipfile_path = self.paths.zip / Path(filename)
        if zipfile_path.exists():
            logging.warn("Already downloaded some zips... skipping early.")
            return None

        logging.info(f"Downloading zip: {base + filename}")
        resp = requests.get(base + filename, stream=True)
        total_size_in_bytes = int(resp.headers.get("content-length", 0))
        path_zipfile = self.paths.zip / Path(filename)
        block_size = 128  # 1 Kibibyte
        progress_bar = tqdm(total=total_size_in_bytes, unit="iB", unit_scale=True)
        with open(path_zipfile, "wb") as fd:
            for chunk in resp.iter_content(chunk_size=128):
                fd.write(chunk)
                progress_bar.update(len(chunk))
        progress_bar.close()
        if total_size_in_bytes != 0 and progress_bar.n != total_size_in_bytes:
            print("ERROR, something went wrong")

        logging.info(f"Extract csv: {self.paths.csv}/{base + filename}")
        zf = zipfile.ZipFile(path_zipfile)
        zf.extractall(self.paths.csv)

    def concat_csvs(self, glob_string="csv/*.csv", output="merged", save_temp=False):
        """glob csvs and merge them, assuming same columns
        -- note that schema changed in 2021
        """
        logging.info(f"Concatenating CSVs in {glob_string}...")
        if len(glob.glob(glob_string)) < 1:
            raise Exception("No CSVs to concatenate.")
        if not os.path.exists("csv"):
            logging.info("CSVs not already found, creating directory")
            os.makedirs("csv")
        dfs = map(
            lambda f: pd.read_csv(f, parse_dates=["started_at", "ended_at"]),
            glob.glob(glob_string),
        )
        all_months = pd.concat(dfs)
        all_months["uuid"] = uuid.uuid1()
        if save_temp:
            logging.info(f"Saving to {output}")
            all_months.to_pickle(path=f"{output}.pickle")
        return all_months

    def load_rename_rides(
        self,
        input_merged_rides: pd.DataFrame = None,
        input_merged_rides_path="./merged.pickle",
        prepped_rides="./merged_prepped.pickle",
        save_temp=False,
    ):
        """Loads concatted rides ride files

        Args:
            input_merged_rides ([type], optional): [description]. Defaults to Path("merged.pickle").
            prepped_rides ([type], optional): [description]. Defaults to Path("./merged_prepped.pickle").
            save_temp (bool, optional): [description]. Defaults to True.
        """

        def create_date_columns(df, orientation="start"):
            df = df.copy()
            df[f"{orientation}_hour"] = df[f"{orientation}_time"].dt.hour
            df[f"{orientation}_day"] = df[f"{orientation}_time"].dt.day
            df[f"{orientation}_weekday"] = df[f"{orientation}_time"].dt.dayofweek
            return df

        # Guard: load from disk if we are debugging
        if input_merged_rides_path is None:
            logging.info(f"Loading input_merged_rides_path...")
            if Path(self.paths.start_cwd / Path(prepped_rides)).exists():
                logging.info(f"Merged rides already exists; loading existing...")
                df = pd.read_pickle(prepped_rides)
            else:
                df = pd.read_pickle(input_merged_rides_path)

        logging.info(f"Using in-memory input_merged_rides")
        df = input_merged_rides
        df.rename(
            # FROM : TO
            {
                "started_at": "start_time",
                "ended_at": "stop_time",
                "end_station_id": "stop_station_id",
                "end_station_name": "stop_station_name",
                "end_station_latitude": "stop_station_latitude",
                "end_station_longitude": "stop_station_longitude",
            },
            axis=1,
            inplace=True,
        )

        df = create_date_columns(df, orientation="start")
        df = create_date_columns(df, orientation="stop")
        df = df.replace({0: "unknown", 1: "male", 2: "female"}).clean_names()
        if save_temp:
            df.to_pickle(prepped_rides)
        return df

    def fetch_station_info(self, save_temp=False):
        """Get data on all Citi Bike stations from the Citi Bike feed API

        Returns:
            stations_geo: a DataFrame with the station geographies
        """
        url_station_info = self.URL_STATION_FEED
        resp = requests.get(url_station_info)
        resp_data = resp.json()
        stations = pd.DataFrame(resp_data["data"]["stations"])
        stations_geo = gpd.GeoDataFrame(
            stations, geometry=gpd.points_from_xy(stations.lon, stations.lat)
        )
        stations_geo.set_crs(epsg=4326, inplace=True)
        # stations_geo = stations_geo.to_crs(epsg=2263)
        cols_to_keep = [
            "name",
            "legacy_id",
            "geometry",
            "station_id",
            "short_name",
        ]
        stations_geo: pd.DataFrame = stations_geo[cols_to_keep]

        if save_temp:
            stations_geo.to_pickle("stations_original.pickle")
        return stations_geo

    def load_ntas(self, remote_url=URL_NYCNTAS_JSON, save_temp=False):
        """Loads and converts NTA data from NYC remote URL.

        Args:
            remote_url ([type], optional): Set the URL to fetch GeoJSON from. Defaults to self.URL_NYCNTAS_JSON.
            save_temp (bool, optional): Saves intermediate files. Defaults to True.

        Returns:
            [type]: [description]
        """
        ntas = gpd.read_file(
            remote_url,
            driver="GeoJSON",
        ).clean_names()

        if save_temp:
            ntas.to_pickle(Path("ntas_orig.pickle"))
            ntas.to_file(Path("ntas_orig.geojson"), driver="GeoJSON")
        return ntas

    def sjoin_ntas_stations(self, ntas, stations, save_temp=False):
        """Joins NTA context data to CB station locations.

        Args:
            ntas ([type]): GeoDataFrame containing NYC NTAs
            stations ([type]): GeoDataFrame containing citibike stations
            save_temp (bool, optional): Outputs to the temp directory. Defaults to True.

        Returns:
            [type]: [description]
        """
        gdf = gpd.sjoin(stations, ntas, op="intersects", how="left").loc[
            :,
            [
                "station_id",
                "short_name",
                "name",
                "boroname",
                "ntaname",
                "ntacode",
                "geometry",
            ],
        ]
        # projected = gdf.to_crs(epsg=4326)
        projected = gdf.copy()
        if save_temp:
            output = Path(self.paths.out) / "stations-with-nta.geojson"
            logging.info(f"Saving stations with NTAs: {output}")
            projected.to_file(output, driver="GeoJSON")
            projected.to_pickle("./stations-with-nta.pickle")
        return projected
