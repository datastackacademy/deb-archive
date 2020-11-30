import json
from datetime import datetime

from flask import current_app, Flask
from google.cloud import bigquery


# default bigquery names for project, dataset, and table
_project_ = "deb"
_dataset_ = "deb"
_flights_ = "flights"
_airports_ = "airports"
_airlines_ = "airlines"


class QueryFactory:

    def __init__(self, project: str = None, dataset: str = None, flights: str = None, airports: str=None, airlines: str=None):
        super(QueryFactory, self).__init__()
        # create google bigquery client
        self.client = bigquery.Client()
        self.project = project if project is not None else _project_
        self.dataset = dataset if dataset is not None else _dataset_
        self.flights = flights if flights is not None else _flights_
        self.airports = airports if airports is not None else _airports_
        self.airlines = airlines if airlines is not None else _airlines_
        self.flights_ref = f"{self.project}.{self.dataset}.{self.flights}"
        self.airports_ref = f"{self.project}.{self.dataset}.{self.airports}"
        self.airlines_ref = f"{self.project}.{self.dataset}.{self.airlines}"

    def getAllAirports(self, **kwargs):
        client = self.client
        table = self.airports_ref

        QUERY = f"[YOUR ALL AIRPORTS QUERY GOES HERE]"
        query_job = client.query(QUERY)
        r = query_job.result()
        output = [dict(row) for row in r]
        return json.dumps(output)

    def getMinDate(self):
        client = self.client
        table = self.flights_ref

        QUERY = f"[YOUR MIN DATE QUERY GOES HERE]`"
        query_job = client.query(QUERY)
        r = query_job.result()
        all_results = [row for row in r]
        output = all_results[0].values()[0]
        return output

    def getMaxDate(self):
        client = self.client
        table = self.flights_ref

        QUERY = f"[YOUR MAX DATE QUERY GOES HERE]"
        query_job = client.query(QUERY)
        r = query_job.result()
        all_results = [row for row in r]
        output = all_results[0].values()[0]
        return output

    def getAirport(self, iata):
        """query a single airport by its IATA code"""
        client = self.client
        table = self.airports_ref

        QUERY = f"[YOUR PARAMETERIZED AIRPORT QUERY GOES HERE]"
        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter('airport', 'STRING', iata),
            ]
        )
        query_job = client.query(QUERY, job_config=job_config)
        r = query_job.result()
        all_results = [dict(row) for row in r]
        output = all_results[0]
        return output

    def getAircraft(self, tailnum):
        """query a single aircraft by its tailnumber"""
        client = self.client
        table = self.aircrafts_ref

        QUERY = f"[YOUR PARAMETERIZED AIRCRAFT QUERY GOES HERE]"
        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter('tailnum', 'STRING', tailnum),
            ]
        )
        query_job = client.query(QUERY, job_config=job_config)
        r = query_job.result()
        all_results = [dict(row) for row in r]
        output = all_results[0]
        print (output)
        return output

    def getAirlines(self, iata_list: str):
        """query and return a list of comma separated airline codes"""
        client = self.client
        table = self.airlines_ref

        QUERY = f"[YOUR PARAMETERIZED QUERY USING SQL IN CLAUSE]"
        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                # bigquery.ArrayQueryParameter(...),
            ]
        )
        query_job = client.query(QUERY, job_config=job_config)
        r = query_job.result()
        all_results = [dict(row) for row in r]
        return all_results

    def getAirlines(self, iata_list: str):
        """query and return a list of comma separated airline codes"""
        # todo: finish writing the entire method
        pass


# model level factory
_query_factory_ : QueryFactory = None


def get_factory() -> QueryFactory:
    global _query_factory_
    if _query_factory_ is None:
        try:
            app: Flask = current_app
            _query_factory_ = QueryFactory()
        except RuntimeError:
            print("WARNING: working outside of flask application context")
            _query_factory_ = QueryFactory()
    return _query_factory_


def test():
    factory = get_factory()
    # airports = factory.getAllAirports()
    # PDX = factory.getAirport("PDX")
    # min_date = factory.getMinDate()
    # max_date = factory.getMaxDate()
    # flights = factory.getFlights("PDX","SJC","2018-01-01", "2018-01-01")
    # getAirlines = factory.getAirlines("WN,AS,NK")


if __name__ == '__main__':
    print("WARNING: this module should not be called by itself. continuing with tests...")
    test()
