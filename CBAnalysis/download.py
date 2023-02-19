from pprint import pprint

import datetime
from bs4 import BeautifulSoup
from urllib.request import urlopen
import re


def download_file_list():
    URL = "https://s3.amazonaws.com/tripdata"
    with urlopen(URL) as response:
        html_doc = response.read()
        soup = BeautifulSoup(html_doc, "html.parser")
        tags = soup.find_all("key")
        filenames = [tag.string for tag in tags]
        return filenames
        # print(soup.prettify())


def parse_date_from_filename(filename):
    match = re.search("\d{6}", filename)
    if not match:
        return
    date = match[0]
    year = date[:4]
    month = date[4:6]
    return {"year": year, "month": month}


def parse_filename_to_metadata(filename):
    if not filename.endswith("zip"):
        return
    record = {"date": parse_date_from_filename(filename), "filename": filename}
    return record


def parse_filenames_to_metadata(filenames):
    result = []
    for filename in filenames:
        record = parse_filename_to_metadata(filename)
        result.append(record)
    return result


def parse_meta_date(meta):
    return datetime.datetime(int(meta["year"]), int(meta["month"]), 1)


def filter_range(files, start_date, end_date):
    start = parse_meta_date(start_date)
    end = parse_meta_date(end_date)

    def compare(file: dict):
        if file is None:
            return False
        date = parse_meta_date(file["date"])
        return (start <= date) and (end >= date)

    return list(filter(compare, files))


def download_date_range(
    start={"year": "2020", "month": "01"}, end={"year": "2020", "month": "02"}
):
    filenames = download_file_list()
    all_downloads = parse_filenames_to_metadata(filenames)
    filtered_downloads = filter_range(all_downloads, start, end)
    # pprint(filtered_downloads)
    return filtered_downloads



