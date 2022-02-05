from cbanalysis import __version__
from pathlib import Path
from cbanalysis import Prepper
import os


def test_version():
    assert __version__ == "0.1.0"


class TestDownloading:
    def test_download(self):
        # prepper = Prepper(start_cwd=Path("./.."))
        """download file and check if it exists in the temp directory"""
        # prepper.download_ride_zip()
        assert Path("temp/csv/").exists()
