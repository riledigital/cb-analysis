# CBAnalysis

This is a companion REST API server + analysis module for [Citi Bike Planner](https://github.com/riledigital/citibike-planner).

# Installation

Install `pyenv` and `poetry`. Install GDAL with your system package manager. Then run `make install` and `make run-analysis-job`. 


See the `Makefile` for tasks to run.

A Docker image is available which runs `scripts/main.py`.
## Environment Variables

Use env variables to configure the scripts. See `.env.sample` 
