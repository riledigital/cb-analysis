from .download import parse_date_from_filename, compare_date

def test_parse_date_from_filename():
  assert parse_date_from_filename("citibike-tripdata.csv.zip") is None
  assert parse_date_from_filename("201912-citibike-tripdata.csv.zip") == { "year" : "2019", "month": "12"}  
  
def parse_filename_to_metadata():
  test_filename = "201912-citibike-tripdata.csv.zip"
  result = parse_filename_to_metadata(test_filename)
  expected ={"date": {"year": "2019", "month": "12"}, "filename": test_filename}
  assert result is expected
  