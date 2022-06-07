# syntax=docker/dockerfile:1
FROM python:3.10.4
RUN apt-get update && apt-get -y install libpq-dev
WORKDIR /app
COPY requirements.txt requirements.txt
RUN pip3 install -r requirements.txt
COPY . /app/cbserver

EXPOSE 80

CMD [ "gunicorn", "--workers=4", "-b", "0.0.0.0:80", "cbserver:create_app()"]
# CMD [ "python3", "-m" , "flask", "run", "--host=0.0.0.0", "--port=3000"]