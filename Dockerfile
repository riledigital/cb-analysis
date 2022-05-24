FROM continuumio/miniconda3
WORKDIR /app
COPY cbanalysis .
COPY scripts .
COPY tests .
COPY environment.yml .
RUN conda env create -f environment.yml
CMD ["conda", "run", "-n", "cbanalysis", "python3", "cbanalysis/cli.py"]