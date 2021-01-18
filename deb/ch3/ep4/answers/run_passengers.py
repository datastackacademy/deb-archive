from deb.utils.config import config
from deb.utils.config import logger

from passenger_loading import PassengerUtils

logger.info("Loading configuration")
bucket = config['defaults']['ch3']['ep4']['input_bucket'].get(str)
passenger_filename = config['defaults']['ch3']['ep4']['input_passengers'].get(
    str)
passenger_output = config['defaults']['ch3']['ep4']['bq_passengers'].get(str)
cards_filepath = config['defaults']['ch3']['ep4']['input_addrs'].get(str)
cards_bq = config['defaults']['ch3']['ep4']['bq_cards'].get(str)
bucket = config['defaults']['ch3']['ep4']['input_bucket'].get(str)
addrs_filepath = config['defaults']['ch3']['ep4']['input_addrs'].get(str)
addrs_bq = config['defaults']['ch3']['ep4']['bq_addrs'].get(str)

loader = PassengerUtils()
loader.load_passengers(passenger_filename, passenger_output)
loader.load_subtable(cards_filepath, 'card_uid', ["street_address",
                                                  "city",
                                                  "state_code",
                                                  "from_date",
                                                  "to_date"], cards_bq)
loader.load_subtable(addrs_filepath, 'addr_uid', ["street_address",
                                                  "city",
                                                  "state_code",
                                                  "from_date",
                                                  "to_date"], addrs_bq)