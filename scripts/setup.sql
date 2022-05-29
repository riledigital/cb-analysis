-- hourly summaries can be selected by short_names and hours

CREATE TABLE summary_hourly (
  short_name text,
  start_hour integer,
  counts integer
)