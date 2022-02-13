from pathlib import Path
import click
import logging
from .main import Main


@click.command()
def run():
    main = Main(start_dir=Path("temp"))
    main.run()


if __name__ == "__main__":
    logging.info("Running CBAnalysis CLI!")
    run()
