# from .main import Main
from main import Main
from dotenv import load_dotenv
from pathlib import Path
import click
import logging
import os

load_dotenv()


@click.command()
def run():
    app = Main(start_dir=Path("temp"))
    app.run(os.getenv("START_DATE"), os.getenv("END_DATE"))


if __name__ == "__main__":
    logging.info("Running CBAnalysis CLI!")
    run()
