from cb_data import CBDataPrep

dp = CBDataPrep.DataPrep

dp.download_ride_zip()
dp.fetch_station_info()