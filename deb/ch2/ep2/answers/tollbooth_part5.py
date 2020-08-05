
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


def key_by_license_plate_month(element):
    """
    here we will construct a multi ((keys), (values)) tuple row

    we're preparing to aggregate by license plate and month and we would aggregated fields:
    count(1), sum(total), sum(cornsilk), sum(slate_gray), sum(navajo_white), mean(total)

    multi-aggregation on a tuple of values is accomplished by
    beam.CombineByKey and beam.combiners.TupleCombineFn
    """
    return (
        (element['license_plate'], element['month']),
        (1, element['total'], element['cornsilk'], element['slate_gray'], element['navajo_white'], element['total'])
    )


def run():
    print("Town of Squirreliwink Bureau Of Tolls and Nuts Affair\n\n[PART-4]")

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
    delete_files(os.path.join(known_args.output, "report*"))

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
                                nut_prices=beam.pvalue.AsDict(nut_prices))
                   )

        # multi-keys multi-values combiner by using beam.combiners.TupleCombineFn()
        # first normalize rows into ((license_plate, month), (1, total, cornsilk, slate gray, navajo white, total)) tuple
        # then apply a tuple of combiners over values
        records = (records
                   | beam.Map(key_by_license_plate_month)
                   | beam.CombinePerKey(
                        beam.combiners.TupleCombineFn(
                            combine.CountCombineFn(), sum, sum, sum, sum, combine.MeanCombineFn()
                        )
                     )
                   )

        # read squirreliwink population file
        # file consist of newline delimited json rows. read each json row as dict
        logger.info("reading Squirreliwink's residents file")
        residents = (pipeline
                     | "residents" >> beam.io.ReadFromText(os.path.join(known_args.input, 'squirreliwink_population.json'))
                     | beam.Map(lambda line: json.loads(line))
                     )

        # key residents by their license plate
        logger.info("key residents by license_plate")
        residents_by_plate = (residents | beam.Map(lambda element: (element['car'], element)))

        # lookup residents by their license plate using SideInputs
        records = (records
                   | beam.Map(lambda e, lookup:
                              (
                                  # add family_name and address from resident lookup to the keys tuple.
                                  # Remember e[0][0] (first value in the keys tuple) should contain our license_plate info
                                  (e[0] + tuple(v for k, v in lookup[e[0][0]].items() if k in ('family_name', 'address'))),
                                  e[1]
                              ), lookup=beam.pvalue.AsDict(residents_by_plate))     # pass in residents info as a SideInput
                   )

        # (records | beam.Map(print))

        # output to a newline delimited json file
        logger.info("output record into csv file")
        (records
         | beam.Map(lambda e: e[0] + e[1])          # flatten ((keys), (values)) tuple into a single tuple (keys + values)
         | beam.Map(lambda t: dict(zip(             # stitch up the results as a dict, adding back column names
                        ('license_plate', 'month', 'family_name', 'address',
                         'visit_count', 'total', 'cornsilk', 'slate_gray', 'navajo_white', 'avg_total'),
                    t)
                ))
         | beam.Map(lambda d: json.dumps(d, ensure_ascii=False))    # json output the results
         | beam.io.WriteToText(os.path.join(known_args.output, "report"),
                               file_name_suffix='.json')
         )


if __name__ == '__main__':
    run()
