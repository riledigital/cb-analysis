.PHONY: lint format

lint:
	poetry run pylint cbanalysis

format:
	poetry run black cbanalysis scripts tests

run-server:
	gunicorn --workers=8 -b 0.0.0.0:80 'cbserver:create_app()'

install:
	poetry install

run-analysis-job:
	python3 ./cbanalysis/cli.py