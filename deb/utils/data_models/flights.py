"""
Helper functions and datamodels for Chapter 2 flight records.


"""


from google.cloud import bigquery
import pyarrow


# historical flights schemas
FLIGHTS_CSV_COLUMNS = ["day_of_week", "flight_date", "airline", "tailnumber", "flight_number",
            "src", "src_city", "src_state", "dest", "dest_city", "dest_state",
            "departure_time", "actual_departure_time", "departure_delay",
            "taxi_out", "wheels_off", "wheels_on", "taxi_in",
            "arrival_time", "actual_arrival_time", "arrival_delay",
            "cancelled", "cancellation_code", "flight_time", "actual_flight_time", "air_time", "flights", "distance",
            "airline_delay", "weather_delay", "nas_delay", "security_delay", "late_aircraft_delay"]

FLIGHTS_CSV_SCHEMA = {
    'day_of_week': {"type": int, "bq_type": "INT64", "parquet_type": pyarrow.int8(), "description": "Day of the week. A number between 1-7 starting with Monday as 1."},
    'flight_date': {"type": str, "bq_type": "DATE", "parquet_type": pyarrow.date32(), "description": "Flight date in YYYY-MM-DD format."},
    'airline': {"type": str, "bq_type": "STRING", "parquet_type": pyarrow.string(), "description": "Airline IATA src."},
    'tailnumber': {"type": str, "bq_type": "STRING", "parquet_type": pyarrow.string(), "description": "Aircraft tail number unique identifier."},
    'flight_number': {"type": str, "bq_type": "STRING", "parquet_type": pyarrow.string(), "description": "Airline flight number. This is a unique number combined with airline, flight_date, src, and dest."},
    'src': {"type": str, "bq_type": "STRING", "parquet_type": pyarrow.string(), "description": "Originating airport IATA src."},
    'src_city': {"type": str, "bq_type": "STRING", "parquet_type": pyarrow.string(), "description": "Originating airport city/state name."},
    'src_state': {"type": str, "bq_type": "STRING", "parquet_type": pyarrow.string(), "description": "Originating state name (United States)."},
    'dest': {"type": str, "bq_type": "STRING", "parquet_type": pyarrow.string(), "description": "Destination airport IATA src."},
    'dest_city': {"type": str, "bq_type": "STRING", "parquet_type": pyarrow.string(), "description": "Destination airport city/state name."},
    'dest_state': {"type": str, "bq_type": "STRING", "parquet_type": pyarrow.string(), "description": "Destination state name (United States)."},
    'departure_time': {"type": str, "bq_type": "TIME", "parquet_type": pyarrow.string(), "description": "Scheduled flight departure time in military format (ie: '1725' as 05:25pm)."},
    'actual_departure_time': {"type": str, "bq_type": "TIME", "parquet_type": pyarrow.string(), "description": "Actual flight departure time in military format (ie: '1725' as 05:25pm)."},
    'departure_delay': {"type": str, "bq_type": "FLOAT", "parquet_type": pyarrow.float32(), "description": "Flight delay in minutes as a decimal number. Negative numbers represent early flight departure. (ie: -3.5 for 3 minutes and 30 seconds early departure)."},
    'taxi_out': {"type": str, "bq_type": "FLOAT", "parquet_type": pyarrow.float32(), "description": "Flight take-off taxi time in minutes as a decimal (ie: 3.5 as 3 minutes and 30 seconds)."},
    'wheels_off': {"type": str, "bq_type": "TIME", "parquet_type": pyarrow.string(), "description": "Flight wheels off the ground take-off time in military format (ie: '1725' as 05:25pm)."},
    'wheels_on': {"type": str, "bq_type": "TIME", "parquet_type": pyarrow.string(), "description": " Flight wheels on on the ground landing time in military format (ie: '1725' as 05:25pm)."},
    'taxi_in': {"type": str, "bq_type": "FLOAT", "parquet_type": pyarrow.float32(), "description": "Flight landing taxi time in minutes as a decimal (ie: 3.5 as 3 minutes and 30 seconds)."},
    'arrival_time': {"type": str, "bq_type": "TIME", "parquet_type": pyarrow.string(), "description": "Flight scheduled gate arrival time in military format (ie: '1725' as 05:25pm)."},
    'actual_arrival_time': {"type": str, "bq_type": "TIME", "parquet_type": pyarrow.string(), "description": "Flight actual gate arrival time in military format (ie: '1725' as 05:25pm)."},
    'arrival_delay': {"type": str, "bq_type": "FLOAT", "parquet_type": pyarrow.float32(), "description": "Flight arrival delay in minutes as a decimal. Negative numbers represent early arrival (ie: -3.5 as 3 minutes and 30 seconds early arrival)."},
    'cancelled': {"type": str, "bq_type": "BOOL", "parquet_type": pyarrow.bool_(), "description": "Flight cancellation indicator with 1 indicating a cancelled flight."},
    'cancellation_code': {"type": str, "bq_type": "STRING", "parquet_type": pyarrow.string(), "description": "Flight cancellation src. A: Carrier, B: Weather, C: National Air System, D: Security, Empty: Not Cancelled."},
    'flight_time': {"type": str, "bq_type": "FLOAT", "parquet_type": pyarrow.float32(), "description": "Scheduled flight time in minutes as a decimal (ie: 120.5 as 2 hours and 30 seconds)."},
    'actual_flight_time': {"type": str, "bq_type": "FLOAT", "parquet_type": pyarrow.float32(), "description": "Actual flight time in minutes as a decimal (ie: 120.5 as 2 hours and 30 seconds)."},
    'air_time': {"type": str, "bq_type": "FLOAT", "parquet_type": pyarrow.float32(), "description": "Flight in-air time in minutes as a decimal (ie: 120.5 as 2 hours and 30 seconds)."},
    'flights': {"type": str, "bq_type": "INT64", "parquet_type": pyarrow.int8(), "description": "Number of flight legs. This number is typically 1 as a single route."},
    'distance': {"type": str, "bq_type": "FLOAT", "parquet_type": pyarrow.float32(), "description": "Distance between airports in miles as a decimal (ie: 1,250.5 miles)."},
    'airline_delay': {"type": str, "bq_type": "FLOAT", "parquet_type": pyarrow.float32(), "description": "Flight delay in minutes due to airline issues."},
    'weather_delay': {"type": str, "bq_type": "FLOAT", "parquet_type": pyarrow.float32(), "description": "Flight delay in minutes due to weather issues."},
    'nas_delay': {"type": str, "bq_type": "FLOAT", "parquet_type": pyarrow.float32(), "description": "Flight delay in minutes due to National Air System (NAS) issues."},
    'security_delay': {"type": str, "bq_type": "FLOAT", "parquet_type": pyarrow.float32(), "description": "Flight delay in minutes due to security issues."},
    'late_aircraft_delay': {"type": float, "bq_type": "FLOAT", "parquet_type": pyarrow.float32(), "description": "Flight delay in minutes due to late arriving aircraft."},
}

FUTURE_FLIGHTS_BIGQUERY_SCHEMA = [bigquery.SchemaField('flight_date', 'DATE', mode='REQUIRED'),
                                  bigquery.SchemaField('airline', 'STRING', mode='REQUIRED'),
                                  bigquery.SchemaField('flight_number', 'STRING', mode='REQUIRED'),
                                  bigquery.SchemaField('tailnumber', 'STRING', mode='NULLABLE'),
                                  bigquery.SchemaField('src', 'STRING', mode='REQUIRED'),
                                  bigquery.SchemaField('dest', 'STRING', mode='REQUIRED'),
                                  bigquery.SchemaField('departure_time', 'TIME', mode='REQUIRED'),
                                  bigquery.SchemaField('arrival_time', 'TIME', mode='REQUIRED'),
                                  bigquery.SchemaField('flight_time', 'FLOAT64', mode='NULLABLE'),
                                  bigquery.SchemaField('distance', 'FLOAT64', mode='NULLABLE'),
                                  bigquery.SchemaField('day_of_week', 'INT64', mode='NULLABLE'),
                                  ]


def datamodel_flights_column_names():
    """
    Get FLIGHTS_CSV_SCHEMA column names (keys)

    :return: list
    """
    return list(FLIGHTS_CSV_SCHEMA.keys())


def datamodel_flights_bigquery_schema():
    """
    Get FLIGHTS_CSV_SCHEMA as BigQuery schema (using bigquery.SchemaField).

    :return: list[bigquery.SchemaField]
    """
    return [bigquery.SchemaField(k, field_type=v['bq_type'], mode='NULLABLE', description=v['description'])
            for k, v in FLIGHTS_CSV_SCHEMA.items()]


def datamodel_flights_parquet_schema():
    fields = [(k, v['parquet_type']) for k, v in FLIGHTS_CSV_SCHEMA.items()]
    return pyarrow.schema(fields)

