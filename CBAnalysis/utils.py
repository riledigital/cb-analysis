import os
from datetime import date, timedelta
import logging
from pathlib import Path
from tempfile import TemporaryDirectory


def touchdir(dir: str):
    if not os.path.exists(dir):
        logging.warn(f"Created: {dir}")
        os.makedirs(dir)
    else:
        logging.warn(f"Skipped mkdir: {dir}")


class WorkingPaths:
    """A dictionary of working paths, with the option of creating a TemporaryDirectory."""

    def __init__(self, start_dir, touch=True) -> None:
        self.start_cwd = None
        self.tempdir = None
        self.paths = self.make_folders(start_dir)

        if touch:
            logging.debug("Touch-ing folders...")
            self.touch()
            # Assign AFTER the touch
            if self.tempdir:
                self.paths["cwd"] = self.tempdir
        # Assign dict to class attrs
        for k, v in self.paths.items():
            setattr(self, k, v)
        pass

    def make_folders(self, start_dir=None):
        """
        Create folders for project.
        """
        logging.debug(f"CWD IS {os.getcwd()}")
        if start_dir:
            logging.debug(f"Using existing directory: {start_dir}")
            self.start_cwd = start_dir
            os.chdir(self.start_cwd)
        else:
            self.tempdir = TemporaryDirectory()
            logging.debug(f"Using TemporaryDirectory: {start_dir}")
            self.start_cwd = self.tempdir.name
            os.chdir(self.tempdir.name)

        logging.debug(f"CWD changed to {os.getcwd()}")
        paths = {x: Path(x) for x in ["zip", "csv", "out", "summary"]}
        # paths = {
        #     "zip": Path("./zip"),
        #     "csv": Path("./csv"),
        #     "out": Path("./out"),
        #     "summary": Path("./summary"),
        # }
        return paths

    def touch(self):
        # `touch` all the paths we're using
        for dir in self.paths.values():
            logging.debug(f"Created {dir}")
            touchdir(dir)


def get_months(start_date=date(2020, 8, 1), end_date=date(2020, 12, 1)):
    delta = end_date - start_date  # returns timedelta
    months = []
    for i in range(delta.months + 1):
        month = start_date + timedelta(months=1)
        print(month)
        months.append(month)
    return months
