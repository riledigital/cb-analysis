# CBAnalysis

These scripts are used to automatically download and create summary files for the [Citi Bike Planner](https://github.com/rl2999/citibike-planner).

# Installation

This project uses [Poetry](https://python-poetry.org/docs/basic-usage/#installing-dependencies) for dependencies and package management.

- Use `pyenv` to install and switch to Python 3.8.12
- Note: `rtree` or `geopandas` might fail without `libspatialindex` installed. Use a system package manager to install. `brew install spatialindex`.
- Run `poetry install` to create a virtualenv + install dependencies.
- Set VSCode Python environment to the output of `poetry env info` under Virtualenv path to enable VSCode debugger.
- Tests run with `pytest`

## Makefile

See the `Makefile` for tasks to run.

## Dev environment warning

Due to system-level dependencies and general Poetry + pandas + scipy weirdness, it's safest to install dependencies for now through conda. `conda env create -f environment.yml`