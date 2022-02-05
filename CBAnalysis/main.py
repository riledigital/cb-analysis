__version__ = "0.1.0"
import logging
import os

from .data_prep import Prepper
from .summarize import Summarizer
from .utils import touchdir

from tempfile import TemporaryDirectory
from pathlib import Path

logging.info(f"Version: {__version__}")


def make_folders(start_dir=None):
    """
    Create folders for project.
    """
    tempdir = TemporaryDirectory()
    # start_cwd = start_dir or tempdir.name

    logging.info(f"CWD IS {os.getcwd()}")
    os.chdir(tempdir.name)
    logging.info(f"CWD changed to {os.getcwd()}")

    paths = {
        "dir_zip": Path("./zip"),
        "dir_csv": Path("./csv"),
        "dir_out": Path("./out"),
        "dir_summary": Path("./summary"),
    }

    # `touch` all the paths we're using
    for dir in paths.values():
        touchdir(dir)
    # Assign AFTER the touch
    paths["start_cwd"] = tempdir

    return paths


class Main:
    def __init__(self, start_dir: Path = None):
        paths = make_folders(start_dir)
        dp = Prepper(
            dir_cwd=paths["start_cwd"],
            dir_zip=paths["dir_zip"],
            dir_csv=paths["dir_csv"],
            dir_out=paths["dir_out"],
        )
        summarizer = Summarizer(
            dir_cwd=paths["start_cwd"],
            dir_zip=paths["dir_zip"],
            dir_csv=paths["dir_csv"],
            dir_out=paths["dir_out"],
            dir_summary=paths["dir_summary"],
        )
        logging.info("Downloading ZIPs from Citi Bike")
        dp.download_ride_zip()
        logging.info("Fetching station info...")
        stations = dp.fetch_station_info()
        dp.concat_csvs()
        logging.info("Preparing data...")
        df = dp.load_rename_rides()
        logging.info("Loading NTAs...")
        ntas = dp.load_ntas()
        stations_with_nta = dp.sjoin_ntas_stations(ntas, stations)

        logging.info("Aggregating ride data...")
        aggregated_rides = summarizer.agg_by_hour(df)
        logging.info("Exporting report...")
        summarizer.export_by_hour_json(aggregated_rides)


if __name__ == "__main__":
    logging.info("See cli.py for a runnable mode")
    pass
