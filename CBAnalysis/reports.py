# # Generate JSON report data
import json
import logging


def make_report(df_station_geo, df_hourly, df_station_ranking):
    """
    Generate a JSON file with the report
    """
    # Keys of each dict should be station_id

    report = dict(
        {
            "geo": df_station_geo,
            "hourly_breakdown": df_hourly,
            "ranking": df_station_ranking,
        }
    )
    return report


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
