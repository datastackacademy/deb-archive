# import sqlite3
from datetime import datetime
from logging import Logger, DEBUG

from flask import Flask, request, current_app
from flask_cors import CORS
# from flask_httpauth import HTTPTokenAuth

api_version = "v1.0"

# setup app
app = Flask(__name__)
app.config.from_pyfile('config.py')
CORS(app)

# setup logger
logger: Logger = app.logger
logger.setLevel(DEBUG)

# setup database
# db_uri = "flights.db"
# app.config["db_uri"] = db_uri


def init():
    from .views.query import bp as bp_query

    # setup views
    app.register_blueprint(bp_query)


def get_headers() -> dict:
    return {
        "api-version": api_version,
        "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f"),
        "Content-Type": "application/json",
        "Access-Control-Allow-Origin": "*"
    }


@app.route('/version')
def version():
    return {"version": api_version}, 200, get_headers()



init()


if __name__ == '__main__':
    app.run()
