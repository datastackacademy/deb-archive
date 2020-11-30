
import sys
import os
import logging
import argparse
import glob
from datetime import datetime

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions

# setup python logging
logging.basicConfig(format='[%(levelname)8s] [%(asctime)s] [%(module)20s:%(lineno)04d] : %(message)s', level=logging.INFO)
logger = logging

TOLLBOOTH_HEADERS = 'date,tollbooth,license_plate,cornsilk,slate_gray,navajo_white'


# ╔═══════════════════════════════════════════════════════════════════════╗
# ║═════╬═════ HELPER FUNCTIONS                                      ═════║
# ╚═══════════════════════════════════════════════════════════════════════╝

def delete_files(filepath_pattern):
    logger.info(f"deleteing {filepath_pattern}...")
    for f in glob.glob(filepath_pattern):
        os.remove(f)


# ╔═══════════════════════════════════════════════════════════════════════╗
# ║═════╬═════ BEAM PIPELINE FUNCTIONS                               ═════║
# ╚═══════════════════════════════════════════════════════════════════════╝


def parse_csv(line):
    # breakout csv values into a list and strip out space, ", and carriage return
    values = [v.strip(' "\n') for v in str(line).split(',')]
    keys = TOLLBOOTH_HEADERS.split(',')
    # pack row in {key: value} dict with column values
    return dict(zip(keys, values))


class PrepareAndAddTotalsWithSideInput(beam.DoFn):

    def process(self, element, nut_prices, *args, **kwargs):
        # check to see if side_input contains all nut proices we care about!
        assert all(nut in nut_prices for nut in ['cornsilk', 'slate_gray', 'navajo_white']), "missing some nuts!"

        # convert values into correct data types
        record_date = datetime.strptime(element['date'], '%Y.%m.%d')        # parse date
        element['date'] = element['date']
        element['tollbooth'] = int(element['tollbooth'])
        element['cornsilk'] = int(element['cornsilk'])
        element['slate_gray'] = int(element['slate_gray'])
        element['navajo_white'] = int(element['navajo_white'])

        # add calculated columns: total toll, week of year, and month
        # read nut prices from the side_input as a dict
        element['total'] = ((nut_prices['cornsilk'] * element['cornsilk']) +
                            (nut_prices['slate_gray'] * element['slate_gray']) +
                            (nut_prices['navajo_white'] * element['navajo_white']))
        element['week'] = record_date.isocalendar()[1]      # week number in year
        element['month'] = record_date.strftime("%Y.%m")

        yield element


def run():
    print("Town of Squirreliwink Bureau Of Tolls and Nuts Affair\n\n[PART-2]")

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

    # delete previous run files
    delete_files(os.path.join(known_args.output, "output*"))

    # construct pipeline and run
    options = PipelineOptions(beam_args)
    with beam.Pipeline(options=options) as pipeline:
        # create a pcollection of nut prices
        logger.info("creating nut prices side input")
        nut_prices = (pipeline
                      | beam.Create([('cornsilk', 2.0),
                                     ('slate_gray', 3.5),
                                     ('navajo_white', 7.0)])
                      )

        # read toll records and pass in nut prices as a side_input
        # you can convert a (k, v) tuple pcollection into a {k: v} with beam.pvalue.AsDict()
        logger.info("reading toll records")
        records = (pipeline
                   | beam.io.ReadFromText(os.path.join(known_args.input, 'tollbooth_logs.csv'),
                                          skip_header_lines=1)
                   | beam.Map(parse_csv)
                   | beam.ParDo(PrepareAndAddTotalsWithSideInput(),
                                nut_prices=beam.pvalue.AsDict(nut_prices))      # calling ParDo with side_inputs
                   )

        # output to a txt file
        logger.info("output record into text file")
        (records
         | beam.io.WriteToText(os.path.join(known_args.output, "output"),
                               file_name_suffix='.txt')
         )


if __name__ == '__main__':
    run()
