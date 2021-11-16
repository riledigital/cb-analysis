__version__ = "0.1.0"

from CBAnalysis.CBDataPrep import Prepper
from CBAnalysis.CBSummarize import Summarizer

from tempfile import TemporaryDirectory
from pathlib import Path

def touchdir(dir: str):
    if not os.path.exists(dir):
            logging.warn(f"Created: {dir}")
            os.makedirs(dir)
    else:
        logging.warn(f"Skipped mkdir: {dir}")
        
import os
import logging


class CBAnalysis:
    
    def __init__(self, start_dir: Path):
        
        self.tempdir = TemporaryDirectory()
        self.start_cwd = start_dir or self.tempdir
        
        logging.info(f"CWD IS {os.getcwd()}")
        os.chdir(self.start_cwd)
        logging.info(f"CWD changed to {os.getcwd()}")
        
        self.dir_zip = Path('./zip')
        touchdir(self.dir_zip)
        self.dir_csv = Path('./csv')
        touchdir(self.dir_csv)
        self.dir_out = Path('./out')
        touchdir(self.dir_out)
        self.dir_summary = Path('./summary')
        touchdir(self.dir_summary)
        
                
        dp = Prepper(self.start_cwd, self.dir_zip, self.dir_csv, self.dir_out)
        summarizer = Summarizer(self.start_cwd, self.dir_zip, self.dir_csv, self.dir_out, self.dir_summary)
        
        # dp.download_ride_zip()
        stations = dp.fetch_station_info()
        # dp.concat_csvs()
        df = dp.load_rename_rides()
        ntas = dp.load_ntas()
        stations_with_nta = dp.sjoin_ntas_stations(ntas, stations)

        # aggregated_rides = summarizer.agg_by_hour(df)
        # summarizer.export_by_hour_json(aggregated_rides)
        
if __name__ == "__main__":
    # Script mode
    CBAnalysis(start_dir='temp')