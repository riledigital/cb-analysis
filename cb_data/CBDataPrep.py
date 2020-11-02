from pathlib import Path
import logging
import pandas as pd
import geopandas as gpd
import janitor

# import matplotlib.pyplot as plt
import requests
import zipfile
import uuid
import glob
import os

logging.basicConfig(level=logging.INFO)


class DataPrep:
    """CBAnalysis can download and analyze
    data for Citi Bike planner. It is modular for use with Airflow or simple scripts"""

    def __init__(self, start_cwd="./temp/"):
        """Initialize a CBAnalysis instance with the cwd

        Args:
            start_cwd ([type], optional): [description]. Defaults to Path("./..").

        Returns:
            [type]: [description]
        """

        self.start_cwd = start_cwd
        self.path_input = ""
        self.path_tmp = ""
        self.path_output = ""
        logging.info("Downloading data")
        logging.info(f"CWD IS {os.getcwd()}")
        os.chdir(start_cwd)
        logging.info(f"CWD changed to {os.getcwd()}")

    def download_ride_zip(
        self, output=Path("temp/csv/"), year=2020, month=8, use_jc=False
    ):
        """Downloads ZIP files and unzips them to output

        Args:
            output ([type], optional): [description]. Defaults to Path("temp/csv/").
            year (int, optional): [description]. Defaults to 2020.
            month (int, optional): [description]. Defaults to 8.
            use_jc (bool, optional): Download Jersey City files. Defaults to False.

        Returns:
            [type]: [description]
        """
        base = "https://s3.amazonaws.com/tripdata/"
        if not os.path.exists("temp/zip"):
            logging.warn("Created zip folder")
            os.makedirs("temp/zip")

        def make_url(year, month):
            return (
                f"{'JC-' if use_jc else ''}{year}{month:0>2}-citibike-tripdata.csv.zip"
            )

        if Path(make_url(year, month)).exists():
            logging.warn("Already downloaded some zips... skipping early.")
            return None

        filename = make_url(year, month)

        resp = requests.get(base + filename, stream=True)
        path_zipfile = f"./temp/zip/{filename}"
        with open(path_zipfile, "wb") as fd:
            for chunk in resp.iter_content(chunk_size=128):
                fd.write(chunk)

        zf = zipfile.ZipFile(path_zipfile)
        zf.extractall(output)

    def concat_csvs(
        self, glob_string="temp/csv/*.csv", output="temp/merged", save_temp=True
    ):
        """glob csvs and merge them, assuming same columns"""
        if not os.path.exists("temp/csv"):
            logging.info("CSVs not already found, creating directory")
            os.makedirs("temp/csv")
        dfs = map(
            lambda f: pd.read_csv(f, parse_dates=["starttime", "stoptime"]),
            glob.glob(glob_string),
        )
        all_months = pd.concat(dfs)
        all_months["uuid"] = uuid.uuid1()
        logging.info(f"Saving to {output}")
        if save_temp:
            all_months.to_pickle(path=f"{output}.pickle")
        return all_months

    def load_rename_rides(
        self,
        input_merged_rides=Path("temp/merged.pickle"),
        prepped_rides=Path("./temp/merged_prepped.pickle"),
        save_temp=True,
    ):
        """Loads concatted rides ride files

        Args:
            input_merged_rides ([type], optional): [description]. Defaults to Path("temp/merged.pickle").
            prepped_rides ([type], optional): [description]. Defaults to Path("./temp/merged_prepped.pickle").
            save_temp (bool, optional): [description]. Defaults to True.
        """

        def create_date_columns(df, orientation="start"):
            df = df.copy()
            df[f"{orientation}_hour"] = df[f"{orientation}time"].dt.hour
            df[f"{orientation}_day"] = df[f"{orientation}time"].dt.day
            df[f"{orientation}_weekday"] = df[f"{orientation}time"].dt.dayofweek
            return df

        if Path(prepped_rides).exists():
            logging.info(f"Merged rides already exists; loading existing...")
            df = pd.read_pickle(prepped_rides)
        else:
            df = pd.read_pickle(input_merged_rides)
            df = create_date_columns(df, orientation="start")
            df = create_date_columns(df, orientation="stop")
            df = df.replace({0: "unknown", 1: "male", 2: "female"}).clean_names()

            df.rename(
                {
                    "end_station_id": "stop_station_id",
                    "end_station_name": "stop_station_name",
                    "end_station_latitude": "stop_station_latitude",
                    "end_station_longitude": "stop_station_longitude",
                },
                axis=1,
                inplace=True,
            )
            if save_temp:
                df.to_pickle(prepped_rides)
        return df

    def fetch_station_info(self, save_temp=True):
        """Get data on all Citi Bike stations from the Citi Bike feed API

        Returns:
            stations_geo: a DataFrame with the station geographies
        """
        url_station_info = (
            "https://gbfs.citibikenyc.com/gbfs/en/station_information.json"
        )
        r = requests.get(url_station_info)
        stations = pd.DataFrame(r.json()["data"]["stations"])
        stations_geo = gpd.GeoDataFrame(
            stations, geometry=gpd.points_from_xy(stations.lon, stations.lat)
        )
        stations_geo.set_crs(epsg=4326, inplace=True)
        # stations_geo = stations_geo.to_crs(epsg=2263)
        cols_to_keep = [
            "name",
            "rental_url",
            "legacy_id",
            "station_id",
            "geometry",
            "short_name",
        ]
        stations_geo = stations_geo[cols_to_keep]
        if save_temp:
            stations_geo.to_pickle(Path("./temp/stations_original.pickle"))
        return stations_geo

    def load_ntas(self, save_temp=True):
        """
        Load NTA data from a NYC source
        """
        # ntas = gpd.read_file(filein, driver="ESRI Shapefile")
        ntas = gpd.read_file(
            "https://data.cityofnewyork.us/api/geospatial/d3qk-pfyz?method=export&format=GeoJSON",
            driver="GeoJSON",
        ).clean_names()

        if save_temp:
            ntas.to_pickle(Path("temp/ntas_orig.pickle"))
            ntas.to_file(Path("temp/ntas_orig.geojson"), driver="GeoJSON")
        return ntas

    def sjoin_ntas_stations(self, ntas, stations, save_temp=True):
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
                "short_name",
                "rental_url",
                "station_id",
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
            projected.to_file("./temp/sjoinedrides.geojson", driver="GeoJSON")
            projected.to_pickle("./temp/stations_with_ntas.pickle")
        return projected