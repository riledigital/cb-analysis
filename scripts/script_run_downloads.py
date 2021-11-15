from citibike_analysis import CBDataPrep
from citibike_analysis import CBSummarize

dp = CBDataPrep.Prepper(start_cwd="temp")
summarizer = CBSummarize.Summarizer()

# dp.download_ride_zip()
stations = dp.fetch_station_info()
# dp.concat_csvs()
df = dp.load_rename_rides()
ntas = dp.load_ntas()
stations_with_nta = dp.sjoin_ntas_stations(ntas, stations)

aggregated_rides = summarizer.agg_by_hour(df)
summarizer.export_by_hour_json(aggregated_rides)