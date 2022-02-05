from CBAnalysis import __version__
from pathlib import Path
import CBAnalysis.data_prep as dp
import os

cb = dp.Prepper(start_cwd=Path("./.."))


def test_version():
    assert __version__ == "0.1.0"


class TestDownloading:
    def test_download(self):
        """download file and check if it exists in the temp directory"""
        cb.download_ride_zip()
        assert Path("temp/csv/").exists()
