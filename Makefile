.PHONY: lint format

lint:
	poetry run pylint cbanalysis

format:
	poetry run black cbanalysis scripts tests

run:
	gunicorn --workers=8 -b 0.0.0.0:80 'cbserver:create_app()'
