# # Generate JSON report data
import json
import logging
import os
import msgpack
import pickle
from pathlib import Path

import pandas as pd
from sqlalchemy import create_engine

from dotenv import load_dotenv

load_dotenv()


def load_pickle():
    with open("./temp/report.pickle", "rb") as f:
        data = pickle.load(f)
        print("Loaded:")


def export_msgpack(input_dict, path_report) -> Path:
    """Export a MsgPack file for the client

    Args:
        input_dict (_type_): _description_

    Returns:
        Path: _description_ Output path
    """
    with open(path_report, "wb") as f:
        logging.info(f"Saving MsgPack to {path_report}")
        msgpack.dump(input_dict, f)

    pass


def export_json(json_data, path_report):
    """Save JSON to file.

    Args:
        json_data ([type]): [description]
        path_report ([type]): [description]
    """
    with open(path_report, "w") as f:
        logging.info(f"Saving to {path_report}")
        json_string = json.dumps(json_data)
        f.write(json_string)


from pathlib import Path


def export_groups_by_stations(df) -> dict:
    """Given a df of aggregated data, group by station id and
    export a dictionary of all

    Args:
        df (_type_): _description_
        export_path (Path): _description_

    Returns:
        dict: _description_
    """
    grouped: dict = dict()
    for station_id, data in df.groupby("short_name"):
        # logging.info(f"Exporting station: {station_id}")
        grouped[station_id] = data.to_dict(orient="records")
    return grouped


def export_hourly_sql(df: pd.DataFrame) -> None or int:
    logging.info(os.getenv("SQLALCHEMY_CONN"))
    engine = create_engine(os.getenv("SQLALCHEMY_CONN"))
    logging.info(
        f"Exporting to SQL database using connection string: SQLALCHEMY_CONN..."
    )
    with engine.connect() as connection:
        result = df.to_sql(
            name="summary_hourly",
            con=connection,
            if_exists="replace",
            chunksize=5000,
            method="multi",
        )
    logging.info(f"{result} rows affected")


# if __name__ == "__main__":
#     # load_pickle()
