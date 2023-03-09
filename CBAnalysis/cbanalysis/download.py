import logging
import datetime
from zipfile import ZipFile
from bs4 import BeautifulSoup
import requests
import re
from tqdm import tqdm
from pathlib import Path


def download_file_list():
    URL = "https://s3.amazonaws.com/tripdata"
    r = requests.get(URL)
    html_doc = r.text
    soup = BeautifulSoup(html_doc, "html.parser")
    tags = soup.find_all("key")
    filenames = [Path(tag.string) for tag in tags]
    return filenames


def parse_date_from_filename(filename: Path):
    match = re.search("\d{6}", str(filename))
    if not match:
        return
    date = match[0]
    year = date[:4]
    month = date[4:6]
    return {"year": year, "month": month}


def parse_filename_to_metadata(filename: Path):
    if filename.suffix != ".zip":
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


def get_files_for_date_range(
    start={"year": "2019", "month": "01"}, end={"year": "2019", "month": "02"}
):
    filenames = download_file_list()
    all_downloads = parse_filenames_to_metadata(filenames)
    filtered_downloads = filter_range(all_downloads, start, end)
    # pprint(filtered_downloads)
    return filtered_downloads


def download_single_zip(filename: Path):
    base = "https://s3.amazonaws.com/tripdata/"
    target = base + str(filename)
    logging.info(f"Downloading zip: {target}")
    res = requests.get(target, stream=True)
    if res.status_code != 200:
        raise requests.RequestException("Nonfile response")
    return res


def save_zipfile_to_disk(resp, file: Path):
    logging.info(f"Saving zip: {file}")
    total_size_in_bytes = int(resp.headers.get("content-length", 0))
    progress_bar = tqdm(total=total_size_in_bytes, unit="iB", unit_scale=True)
    with open(file, "wb") as fd:
        for chunk in resp.iter_content(chunk_size=128):
            fd.write(chunk)
            progress_bar.update(len(chunk))
        progress_bar.close()
        if total_size_in_bytes != 0 and progress_bar.n != total_size_in_bytes:
            print("ERROR, something went wrong")


def download_and_save(
    start: datetime.date, end: datetime.date, path_to_zips: Path, extract_to: Path
):
    parsed_date_dict_start = {"year": start.year, "month": start.month}
    parsed_date_dict_end = {"year": end.year, "month": end.month}
    files = get_files_for_date_range(parsed_date_dict_start, parsed_date_dict_end)
    for file in files:
        print(file)
        resp = download_single_zip(file["filename"])
        outpath_zip = path_to_zips / file["filename"]
        save_zipfile_to_disk(resp, outpath_zip)
        extract_zip(outpath_zip, extract_to)


def extract_zip(zipfile_path: Path, to: Path):

    zf = ZipFile(zipfile_path)
    zf.extractall(to)


if __name__ == "__main__":
    download_and_save()
