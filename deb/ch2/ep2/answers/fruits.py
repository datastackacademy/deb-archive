
import apache_beam as beam


print("\nSimple Create and Map example...")
with beam.Pipeline() as p:
    rows = (
        p
        | beam.Create([('apple', 1),
                       ('banana', 3),
                       ('cherry', 5),
                       ])
        | beam.Map(print)
    )

print("\nMap with lambda function example...")
with beam.Pipeline() as p:
    rows = (
        p
        | beam.Create([('apple', 1),
                       ('banana', 3),
                       ('cherry', 5),
                       ])
        | beam.Map(lambda x: (x[0], x[1] ** 2))
        | beam.Map(print)
    )


print("\nParDo Example...")
class ExplodeByValue(beam.DoFn):

    def process(self, element, *args, **kwargs):
        for _ in range(element[1]):
            yield dict(zip(['name', 'value'], element))


with beam.Pipeline() as p:
    rows = (
        p
        | beam.Create([('apple', 1),
                       ('banana', 3),
                       ('cherry', 5),
                       ])
        | beam.ParDo(ExplodeByValue())
        | beam.Map(print)
    )


print("\nParDo with SideInput Example...")
class PowerUpValue(beam.DoFn):

    def process(self, element, power, *args, **kwargs):
        yield element[0], element[1] ** power


with beam.Pipeline() as p:
    rows = (
        p
        | beam.Create([('apple', 1),
                       ('banana', 3),
                       ('cherry', 5),
                       ])
        | beam.ParDo(PowerUpValue(), power=3)
        | beam.Map(print)
    )


print("\nMap with lambda function and SideInput Example...")
with beam.Pipeline() as p:
    rows = (
        p
        | beam.Create([('apple', 1),
                       ('banana', 3),
                       ('cherry', 5),
                       ])
        | beam.Map(lambda e, power: (e[0], e[1] ** power), power=3)
        | beam.Map(print)
    )


print("\nGroupByKey Example...")
with beam.Pipeline() as p:
    rows = (
        p
        | beam.Create([('apple', 1),
                       ('banana', 3),
                       ('cherry', 5),
                       ('apple', 6),
                       ('banana', 4),
                       ('apple', 4),
                       ])
        | beam.GroupByKey()
        | beam.Map(print)
    )


print("\nGroupByKey + CombineValues Example...")
with beam.Pipeline() as p:
    rows = (
        p
        | beam.Create([('apple', 1),
                       ('banana', 3),
                       ('cherry', 5),
                       ('apple', 6),
                       ('banana', 4),
                       ('apple', 4),
                       ])
        | beam.GroupByKey()
        | beam.CombineValues(sum)
        | beam.Map(print)
    )

# same output as above
print("\nCombinePerKey Example...")
with beam.Pipeline() as p:
    rows = (
        p
        | beam.Create([('apple', 1),
                       ('banana', 3),
                       ('cherry', 5),
                       ('apple', 6),
                       ('banana', 4),
                       ('apple', 4),
                       ])
        | beam.CombinePerKey(sum)
        | beam.Map(print)
    )


print("\nTupleCombineFn Example...")
with beam.Pipeline() as p:
    rows = (
            p
            | beam.Create([('apple', (1, 10)),
                           ('banana', (3, 30)),
                           ('cherry', (5, 50)),
                           ('apple', (6, 60)),
                           ('banana', (4, 40)),
                           ('apple', (4, 40)),
                           ])
            | beam.CombinePerKey(beam.combiners.TupleCombineFn(sum, max))
            | beam.Map(print)
    )

print("\nI/O with TupleCombineFn example...")
with beam.Pipeline() as p:
    fruits = (p
              | beam.io.ReadFromText('<CHANGE THE PATH>/input/fruits.csv')
              | beam.Map(lambda line: str(line).strip().split(','))             # split apart csv columns
              | beam.Map(lambda row: row[:2] + [int(row[2])])                   # convert 3rd column into int
              | beam.Map(lambda row: (tuple(row[:2]), tuple(row[2:] * 3)))      # create a multi ((keys),(values)) tuple.
                                                                                # replicate count column 3 times for multiple aggregations
              )
    # p = (fruits | "print fruits" >> beam.Map(print))

    totals = (fruits
              | beam.CombinePerKey(
                beam.combiners.TupleCombineFn(beam.combiners.CountCombineFn(),
                                              sum,
                                              beam.combiners.MeanCombineFn())
            )
              )
    # print to console
    p = (totals | "print totals" >> beam.Map(print))

    # output to file
    # flatten (k,v) tuple first and convert to csv
    p = (totals
         | beam.Map(lambda kv_tuple: kv_tuple[0] + kv_tuple[1])                     # flatten ((keys),(values)) tuple into (keys + values)
         | beam.Map(lambda flat_tuple: ','.join([str(_) for _ in flat_tuple]))      # join with ',' into a csv
         | beam.io.WriteToText('fruit_totals', file_name_suffix='.txt'))
