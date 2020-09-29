import json
from datetime import datetime

from flask import current_app, Flask
from google.cloud import bigquery


# default bigquery names for project, dataset, and table
_project_ = "deb-airliner"
_dataset_ = "airline_data"
_flights_ = "flights"
_airports_ = "airports"
_airlines_ = "airlines"


class QueryFactory:

    def __init__(self, project: str = None, dataset: str = None, flights: str = None, airports: str = None, airlines: str = None):
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
        """returns dictionary of flights with iata, city, and state information"""
        client = self.client
        table = self.airports_ref

        QUERY = f"SELECT DISTINCT(iata),city,state FROM `{table}`"
        query_job = client.query(QUERY)
        r = query_job.result()
        output = [dict(row) for row in r]
        return json.dumps(output)

    def getAirport(self, iata):
        """query a single airport by its IATA code"""
        client = self.client
        table = self.airports_ref

        QUERY = f"SELECT * FROM `{table}` WHERE iata = @airport"
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

    def getAirlines(self, iata_list: str):
        """query and return a list of comma separated airline codes"""
        client = self.client
        table = self.airlines_ref

        QUERY = f"SELECT * FROM `{table}` WHERE iata IN UNNEST(@airlines)"
        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ArrayQueryParameter('airlines', 'STRING', [x.strip() for x in iata_list.split(',')])
            ]
        )
        query_job = client.query(QUERY, job_config=job_config)
        r = query_job.result()
        all_results = [dict(row) for row in r]
        return all_results

    def getMinDate(self):
        client = self.client
        table = self.flights_ref

        QUERY = f"SELECT MIN(flight_date) FROM `{table}`"
        query_job = client.query(QUERY)
        r = query_job.result()
        all_results = [row for row in r]
        output = all_results[0].values()[0]
        return output

    def getMaxDate(self):
        client = self.client
        table = self.flights_ref

        QUERY = f"SELECT MAX(flight_date) FROM `{table}`"
        query_job = client.query(QUERY)
        r = query_job.result()
        all_results = [row for row in r]
        output = all_results[0].values()[0]
        return output

    def getFlights(self, src, dest, start, end):
        client = self.client
        table = self.flights_ref

        QUERY = f"""SELECT * 
                    FROM `{table}` 
                    WHERE flight_date BETWEEN @start_date AND @end_date 
                    AND src = @src 
                    AND dest = @dest 
                    ORDER BY flight_date"""
        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter('start_date', 'DATE', datetime.strptime(start, "%Y-%m-%d").date()),
                bigquery.ScalarQueryParameter('end_date', 'DATE', datetime.strptime(end, "%Y-%m-%d").date()),
                bigquery.ScalarQueryParameter('src', 'STRING', src),
                bigquery.ScalarQueryParameter('dest', 'STRING', dest),
            ]
        )
        query_job = client.query(QUERY, job_config=job_config)
        r = query_job.result()
        output = [dict(row) for row in r]
        return output


# model level factory
_query_factory_: QueryFactory = None


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
    airports = factory.getAllAirports()
    PDX = factory.getAirport("PDX")
    min_date = factory.getMinDate()
    max_date = factory.getMaxDate()
    flights = factory.getFlights("PDX", "SJC", "2018-01-01", "2018-01-01")
    airlines = factory.getAirlines("WN,AS,NK")

    print(f"we have {len(airports)} airports")
    print(f"PDX: {PDX}")
    print(f"min flight_date: {min_date}")
    print(f"max flight_date: {max_date}")
    print(f"PDX-SJC flights: {len(flights)}")
    print(f"airlines: {airlines}")


if __name__ == '__main__':
    print("WARNING: this module should not be called by itself. continuing with tests...")
    test()
