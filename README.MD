# CBAnalysis

These scripts are used to automatically download and create summary files for the [Citi Bike Planner](https://github.com/rl2999/citibike-planner).



# Installation

This project uses [Poetry](https://python-poetry.org/docs/basic-usage/#installing-dependencies) for dependencies and package management.

- Run `poetry install` to create a virtualenv + install dependencies.
- Tests run with `pytest`

# Notes on Airflow

- Issue: there is no data validation going on in between steps of the script; tests should be able to correct this issue
- If one step fails, the rest will fail (Airflow can fix this issue)
- Returning the data in these functions enables use with Airflow XCOMs?
- Hardcoding remote URLs into the script