import click
import logging
import CBAnalysis


@click.command()
def run():
    CBAnalysis(start_dir="temp")


if __name__ == "__main__":
    logging.info("Running CBAnalysis CLI!")
    run()
