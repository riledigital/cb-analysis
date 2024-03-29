# https://flask-sqlalchemy.palletsprojects.com/en/2.x/quickstart/#a-minimal-application
import logging
import os

from dotenv import load_dotenv
from flask import Flask, request
from flask_cors import CORS
from flask_sqlalchemy import SQLAlchemy

load_dotenv()


def create_app(test_config=None):
    app = Flask(__name__, instance_relative_config=True)

    if test_config is None:
        # load the instance config, if it exists, when not testing
        app.config.from_pyfile("config.py", silent=True)
    else:
        # load the test config if passed in
        app.config.from_mapping(test_config)

    # ensure the instance folder exists
    try:
        os.makedirs(app.instance_path)
    except OSError:
        pass

    # Add CORS origins here
    CORS(app, resources={r"/api/*": {"origins": "*"}})
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

    @app.route("/api/hourly")
    def hourly():
        short_name = request.args.get("short_name")
        if short_name is None:
            return "No short_name specified", 204
        result_proxy = SummaryHourly.query.filter(
            SummaryHourly.short_name == short_name
        ).all()
        results_list = []
        for row in result_proxy:
            # row_as_dict = {
            #     column: str(getattr(row, column)) for column in row.__table__.c.keys()
            # }
            row_as_dict = {"start_hour": row.start_hour, "counts": row.counts}
            results_list.append(row_as_dict)
        return {"data": results_list}

    # End of app factory pattern
    return app


if __name__ == "__main__":
    logging.info("Starting server...")
    app = create_app()
    app.run(debug=True)
