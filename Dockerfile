FROM python:3.11-bullseye
WORKDIR /app
COPY cbanalysis ./cbanalysis
COPY scripts ./scripts
COPY tests ./tests
COPY pyproject.toml .
COPY poetry.lock .
ENV POETRY_HOME=/opt/poetry
# Install Poetry
RUN curl -sSL https://install.python-poetry.org | python3 -
ENV PATH="${PATH}:${POETRY_HOME}/bin"
# Need GDAL for Fiona
RUN apt-get update && apt-get install -y libgdal-dev
RUN gdal-config --version
# Install all Python deps
RUN poetry install --no-root --no-dev
CMD ["poetry", "run", "python3", "scripts/run.py"]