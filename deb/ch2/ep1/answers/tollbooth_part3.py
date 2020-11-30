
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


def key_by_tollbooth_month(element):
    """
    Most Beam combiners are designed to work on a (key, value) tuple row.

    Input rows are normalized into a tuple where the first element contains the keys
    and second elements contains the values for combiners

    Beam allows for multi keys and values by accepting a tuple of tuples:
    ((k1, k2, ...), (v1, v2, v3, ...))
    """
    # create a multiple key (tollbooth, month) key with single value (total) tuple
    return (element['tollbooth'], element['month']), element['total']


def run():
    print("Town of Squirreliwink Bureau Of Tolls and Nuts Affair\n\n[PART-3]")

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
        logger.info("reading toll records")
        records = (pipeline
                   | beam.io.ReadFromText(os.path.join(known_args.input, 'tollbooth_logs.csv'),
                                          skip_header_lines=1)
                   | beam.Map(parse_csv)
                   | beam.ParDo(PrepareAndAddTotalsWithSideInput(),
                                nut_prices=beam.pvalue.AsDict(nut_prices))      # calling ParDo with side_inputs
                   )

        # prepare records for aggregation by forming into a ((keys), (values)) tuple first
        # then apply beam.CombineByKey() aggregator
        logger.info("summing by tollbooth and month...")
        records = (records
                   | beam.Map(key_by_tollbooth_month)       # multi-key by (tollbooth, month)
                   | beam.CombinePerKey(sum)                # sum by key
                   )

        # output to a txt file
        logger.info("output record into csv file")
        (records
         | beam.Map(lambda e: e[0] + (e[1],))  # combine your key,value tuple into a single tuple
         | beam.Map(lambda e: ','.join([str(k) for k in e]))  # write tuple as csv
         | beam.io.WriteToText(os.path.join(known_args.output, "output"),
                               file_name_suffix='.csv',
                               header='tollbooth,month,total')
         )
        # to find run the culpable tollbooth attendant run:
        # diff -yBZd tollbooth_check_file.csv output-00000-of-00001.csv


if __name__ == '__main__':
    run()
