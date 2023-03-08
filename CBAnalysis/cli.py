from dotenv import load_dotenv
from pathlib import Path
import click
import logging
import os
from cbanalysis.main import Main

load_dotenv()


@click.command()
@click.argument("start")
@click.argument("end")
def run(start, end):
    """Run the CBAnalysis program

    Args:
        start (string): Start date, YYYY-MM-01
        end (string): End date, YYYY-MM-01
    """
    logging.basicConfig(encoding="utf-8", force=True, level=logging.DEBUG)
    app = Main(start_dir=Path("temp"))
    app.run(start, end)


if __name__ == "__main__":
    logging.info("Running CBAnalysis CLI!")
    run()
