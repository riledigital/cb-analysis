import pytest
import os
from pathlib import Path

from cbanalysis import __version__
import cbanalysis
from cbanalysis.data_prep import Prepper
from cbanalysis.main import Main
from cbanalysis.reports import export_json
from cbanalysis.utils import WorkingPaths


def test_version():
    assert __version__ == "0.1.0"


# Setup fixtures to be reused


@pytest.fixture
def working_paths():
    return WorkingPaths()


@pytest.fixture
def main_job():
    job = Main()
    return job


def test_station_info(main_job):
    """Download station info"""
    instance = main_job
    stations = instance.dp.fetch_station_info()
    assert stations.shape[0] > 10


# class TestDownloading:
#     def test_download(self):
#         """download file and check if it exists in the temp directory"""
#         prepper = Prepper(start_cwd=Path("./.."))
#         prepper.download_ride_zip()
#         assert Path("temp/csv/").exists()


# class TestReports:
#     def test_make_report(self):
#         """Create a report given files"""
#         export_json()
