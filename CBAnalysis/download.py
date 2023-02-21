import logging
from pprint import pprint

import datetime
from bs4 import BeautifulSoup
import requests
import re
from tqdm import tqdm
    
  
def download_file_list():
    URL = "https://s3.amazonaws.com/tripdata"
    r = requests.get(URL)
    html_doc = r.text
    soup = BeautifulSoup(html_doc, "html.parser")
    tags = soup.find_all("key")
    filenames = [tag.string for tag in tags]
    return filenames


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


def get_files_for_date_range(
    start={"year": "2019", "month": "01"}, end={"year": "2019", "month": "02"}
):
    filenames = download_file_list()
    all_downloads = parse_filenames_to_metadata(filenames)
    filtered_downloads = filter_range(all_downloads, start, end)
    # pprint(filtered_downloads)
    return filtered_downloads


def download_single_zip(filename):
    base = "https://s3.amazonaws.com/tripdata"
    logging.info(f"Downloading zip: {base + filename}")
    return requests.get(base + filename, stream=True)


def save_zipfile_to_disk(resp, file):
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

def main():
    files = get_files_for_date_range()
    for file in files:
        resp = download_single_zip(file["filename"])
        save_zipfile_to_disk(resp, "./temp/zip/" + file['filename'])
        
        
if __name__ == "__main__":
    main()