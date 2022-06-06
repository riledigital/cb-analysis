.PHONY: lint format

lint:
	poetry run pylint cbanalysis

format:
	poetry run black cbanalysis scripts tests

run:
	poetry run python3 ./cbanalysis/cli.py
