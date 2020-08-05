
import sys
import os
import logging
import argparse
import glob
import json
from datetime import datetime

import apache_beam as beam
import apache_beam.transforms.combiners as combine
from apache_beam.options.pipeline_options import PipelineOptions


# setup python logging
logging.basicConfig(format='[%(levelname)-8s] [%(asctime)s] [%(module)-35s][%(lineno)04d] : %(message)s', level=logging.INFO)
logger = logging

TOLLBOOTH_HEADERS = 'date,tollbooth,license_plate,cornsilk,slate_gray,navajo_white'


def parse_csv(line):
    # breakout csv values into a list and strip out space, ", and carriage return
    values = [v.strip(' "\n') for v in str(line).split(',')]
    keys = TOLLBOOTH_HEADERS.split(',')
    # pack row in {key: value} dict with column values
    return dict(zip(keys, values))


def run():
    print("Town of Squirreliwink Bureau Of Tolls and Nuts Affair")

    # parse command line args:
    #   - parse both beam args and known script args
    parser = argparse.ArgumentParser(description="Town of Squirreliwink Bureau Of Tolls and Nuts Affair")
    parser.add_argument('-i', '--input', type=str,
                        default='./data/input',
                        help='Input folder')
    parser.add_argument('-o', '--output', type=str,
                        default='./data/output',
                        help='Output folder')
    known_args, beam_args = parser.parse_known_args(sys.argv)

    # construct pipeline and run
    options = PipelineOptions(beam_args)
    with beam.Pipeline(options=options) as pipeline:
        # create a pcollection of nut prices
        logger.info("GOTTA FINISH WRITING THIS CODE!")


if __name__ == '__main__':
    run()
