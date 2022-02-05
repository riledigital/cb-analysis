import os
import logging


def touchdir(dir: str):
    if not os.path.exists(dir):
        logging.warn(f"Created: {dir}")
        os.makedirs(dir)
    else:
        logging.warn(f"Skipped mkdir: {dir}")
