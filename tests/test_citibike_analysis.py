from citibike_analysis import __version__
from pathlib import Path
import citibike_analysis.CBDataPrep as dp
import os

cb = dp.Prepper(start_cwd=Path("./.."))


def test_version():
    assert __version__ == "0.1.0"


class TestDownloading:
    def test_download(self):
        """download file and check if it exists in the temp directory"""
        cb.download_ride_zip()
        assert Path("temp/csv/").exists()
