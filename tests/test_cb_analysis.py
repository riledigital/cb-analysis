from cb_data import __version__
from pathlib import Path
import cb_data.CBDataPrep as dp
import os

cb = dp.DataPrep(start_cwd=Path("./.."))


def test_version():
    assert __version__ == "0.1.0"


class TestDownloading:
    def test_download(self):
        """download file and check if it exists in the temp directory"""
        cb.download_ride_zip()
        assert Path("temp/csv/").exists()
