import json
import datetime
from flask import Blueprint, request, current_app
from google.cloud import bigquery

from .. import get_headers
from ..utils.query_factory import QueryFactory, get_factory


bp = Blueprint('query', __name__, url_prefix='/query')


@bp.route('/preload')
def preload():
    # get bigquery request params
    factory = get_factory()
    res = {}
    res["airports"] = factory.getAllAirports()
    res["min_date"] = factory.getMinDate()
    res["max_date"] = factory.getMaxDate()

    return res, 200, get_headers()

@bp.route('/airports')
def airports():
    # get flight date and today request params
    src = request.args.get("src", default=None, type=str)
    dest = request.args.get("dest", default=None, type=str)
    # get bigquery request params
    factory = get_factory()
    res = {}
    res["src"] = factory.getAirport(src)
    res["dest"] = factory.getAirport(dest)

    return res, 200, get_headers()

@bp.route('/airlines')
def airlines():
    # get airline info
    iata = request.args.get("iata", default=None, type=str)
    # get bigquery request params
    factory = get_factory()
    output =  factory.getAirlines(iata)
    

    return json.dumps(output), 200, get_headers()


@bp.route('/aircraft')
def aircraft():
    # get aircraft info
    tailnum = request.args.get("tailnum", default=None, type=str)
    # get bigquery request params
    factory = get_factory()
    output = factory.getAircraft(tailnum)

    def datetime_handler(x):
        if isinstance(x, datetime.date):
            return f"{x.year}-{x.month}-{x.day}"

    return json.dumps(output, default=datetime_handler), 200, get_headers()

@bp.route('/flights')
def flights():
    # get flight date and today request params
    src = request.args.get("src", default=None, type=str)
    dest = request.args.get("dest", default=None, type=str)
    start = request.args.get("start", default=None, type=str)
    end = request.args.get("end", default=None, type=str)
    # get bigquery request params
    factory = get_factory()
    res = {}
    res["flights"] = factory.getFlights(src, dest, start, end)

    return json.dumps(res, default=str), 200, get_headers()
