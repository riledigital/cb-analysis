# CBAnalysis

This is a companion REST API server + analysis module for [Citi Bike Planner](https://github.com/riledigital/citibike-planner).

# Installation

Use conda to create the environment. You may use the optional Docker Compose setup to start the cbanalysis API server which provides a `/api/hourly/` endpoint, allowing queries by `?short_name=XXXX` to retrieve the hourly summary for an individual station. 

## Dev environment warning

Due to system-level dependencies and general Poetry + pandas + scipy weirdness, it's safest to install dependencies for now through conda. `conda env create -f environment.yml` 
- Fill parameters in `.env` file, copy from `.env.sample`
- `conda activate cbanalysis`
- `python3 ./cbanalysis/cli.py`
## Makefile

See the `Makefile` for tasks to run.

## Environment Variables

Use env variables to configure the scripts. See `.env.sample` 
