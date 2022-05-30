# https://flask-sqlalchemy.palletsprojects.com/en/2.x/quickstart/#a-minimal-application
import logging
import os

from dotenv import load_dotenv
from flask import Flask, request
from flask_sqlalchemy import SQLAlchemy

load_dotenv()

app = Flask(__name__)
app.config["SQLALCHEMY_DATABASE_URI"] = os.getenv("SQLALCHEMY_CONN")
db = SQLAlchemy(app)

# Models
class SummaryHourly(db.Model):
    index = db.Column(db.Integer, primary_key=True)
    short_name = db.Column(db.Text)
    start_hour = db.Column(db.Integer)
    counts = db.Column(db.Integer)

    def __repr__(self):
        return f"HourCount({self.short_name}, {self.start_hour}, {self.counts})"


# Server
@app.route("/")
def index():
    return "Server is live!"


@app.route("/hourly")
def hourly():
    json_data = request.get_json(force=True)
    short_name = json_data["short_name"]
    result_proxy = SummaryHourly.query.filter(
        SummaryHourly.short_name == short_name
    ).all()
    results_list = []
    for row in result_proxy:
        print(row)
        row_as_dict = {
            column: str(getattr(row, column)) for column in row.__table__.c.keys()
        }
        results_list.append(row_as_dict)
    return {"data": results_list, "short_name": json_data["short_name"]}


if __name__ == "__main__":
    logging.info("Starting server...")
    app.run(debug=True)
