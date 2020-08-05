import logging
from logging import Logger

from flask import Flask, current_app

from ..config import LOG_LEVEL


def get_logger() -> Logger:
    # todo: gotta fix logger. This gets called before app is always initiated
    # todo: loog into https://flask.palletsprojects.com/en/1.1.x/logging/#basic-configuration

    logging.basicConfig(format='[%(levelname)-7s] %(asctime)s [%(module)-20s][%(lineno)04d] : %(message)s', level=LOG_LEVEL)
    try:
        app: Flask = current_app
        return app.logger
    except RuntimeError:
        return logging


logger: Logger = get_logger()

