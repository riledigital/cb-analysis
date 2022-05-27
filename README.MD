# CBAnalysis

These scripts are used to automatically download and create summary files for the [Citi Bike Planner](https://github.com/rl2999/citibike-planner).

# Installation

## Dev environment warning

Due to system-level dependencies and general Poetry + pandas + scipy weirdness, it's safest to install dependencies for now through conda. `conda env create -f environment.yml` 
- Fill parameters in `.env` file, copy from `.env.sample`
- `conda activate cbanalysis`
- `python3 ./cbanalysis/cli.py`
## Makefile

See the `Makefile` for tasks to run.

## Environment Variables

Use env variables to configure the scripts. See `.env.sample` 