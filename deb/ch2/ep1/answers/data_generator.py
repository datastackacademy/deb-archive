"""
Apache Beam Intro Tutorial - Squirreliwink nuts log data generator

Author: Par (turalabs.com)
Contact:
"""

import argparse
import json
import math
import os
import random
import string
import sys
from datetime import datetime, timedelta

from faker import Faker
from faker.providers import automotive, address, date_time, job, ssn

args = object()
families = []
fake = Faker('de_DE')
fake.add_provider(automotive)
fake.add_provider(address)
fake.add_provider(date_time)
fake.add_provider(job)
fake.add_provider(ssn)


class Family(object):

    __families = []
    __cars = []
    __ids = []

    def __init__(self):
        super(Family, self).__init__()
        self.family_id = self.generate_id()
        self.family_name = fake.last_name()
        self.address = self.generate_address()
        self.resident_since = fake.date_between(start_date='-30y', end_date='-3y').strftime('%Y-%m-%d')
        self.car = self.generate_car()
        self.members = [fake.first_name() for _ in range(random.randint(1, 5))]
        self.__families.append(self)

    def __repr__(self):
        return f"Family({str(self.__dict__)})"

    def to_json(self):
        return json.dumps(self.__dict__, ensure_ascii=False)

    @classmethod
    def _gen_id(cls, length=6, sep='-', sep_index=3):
        assert sep is None or length > sep_index, "length must be greater than sep_index"
        newid = ''.join(random.choice(string.ascii_uppercase + string.digits) for _ in range(length))
        return newid if sep is None else f"{newid[:sep_index]}{sep}{newid[sep_index:]}"

    @classmethod
    def generate_id(cls, unique=True, length=6):
        if not unique:
            return cls._gen_id()
        else:
            while True:
                newid = cls._gen_id()
                if newid not in cls.__ids:
                    cls.__ids.append(newid)
                    return newid

    @classmethod
    def generate_address(cls):
        return fake.street_address()

    @classmethod
    def generate_car(cls, unique=True):
        if not unique:
            return fake.license_plate()
        else:
            while True:
                plate = fake.license_plate()
                if plate not in cls.__cars:
                    cls.__cars.append(plate)
                    return plate

    @classmethod
    def generate_person(cls, sex=None, child=False):
        min_age, max_age = (3, 15) if child else (30, 75)
        sex = random.choice(['F', 'M', 'N']) if sex is None else sex
        return {'name': fake.first_name_female() if sex == 'F' else fake.first_name_male() if sex == 'M' else fake.first_name(),
                'birth_date': fake.date_of_birth(minimum_age=min_age, maximum_age=max_age),
                'ssn': fake.ssn(),  # social squirrel number
                'job': (random.choice(['student', 'undecided', 'hell-maker', 'nice kid']) if child else fake.job()),
                }

    @classmethod
    def generate(cls):
        return cls()

    @classmethod
    def ids(cls):
        return cls.__ids

    @classmethod
    def cars(cls):
        return cls.__cars

    @classmethod
    def families(cls):
        return cls.__families

    @classmethod
    def random_family(cls):
        return random.choice(cls.__families)

    @classmethod
    def random_car(cls):
        return random.choice(cls.__cars)

    @classmethod
    def random_id(cls):
        return random.choice(cls.__ids)

    @classmethod
    def save(cls, file_name):
        with open(file_name, 'w', encoding='utf-8') as f:
            for family in cls.__families:
                f.write(family.to_json() + "\n")


def parse_arguments(print_out=True):
    global args

    p = argparse.ArgumentParser(description='Squirreliwink nuts log data generator')
    p.add_argument('--family_count', default=835, type=int, help='Number of families to generate')
    p.add_argument('--output', default='./data/input', type=str, help='Output folder')
    p.add_argument('--start_date', default=datetime(2020, 3, 1).date(),
                   type=(lambda x: datetime.strptime(x, '%Y-%m-%d').date()), help='starting date, ie: "2020-03-01"')
    p.add_argument('--end_date', default=datetime(2020, 3, 31).date(),
                   type=(lambda x: datetime.strptime(x, '%Y-%m-%d').date()), help='ending date, ie: "2020-03-31')
    p.add_argument('--min_per_day', default=0.1, type=float, help='min records per day (% of family_count)')
    p.add_argument('--max_per_day', default=0.2, type=float, help='max records per day (% of family_count)')
    args, other_args = p.parse_known_args(sys.argv)
    # print args
    if print_out:
        print(f"command line args: {sys.argv}")
        for k, v in args.__dict__.items():
            print(f"\t{k:15s}: {v}")
    return args


def generate_tollbooth_logs():
    days = [args.start_date + timedelta(days=d) for d in range((args.end_date - args.start_date).days + 1)]
    for d in days:
        print(f"generating {d}")
        for _ in range(random.randint(math.ceil(args.min_per_day * args.family_count), math.ceil(args.max_per_day * args.family_count))):
            family = Family.random_family()
            tollbooth = random.randint(1, 3)
            nuts = random.choices(list(range(11)), weights=[int(x ** 1.5) for x in range(10, -1, -1)], k=3)
            yield d, tollbooth, family, nuts


def generate_tollbooth_logs_random_dates():
    num_days = (args.end_date - args.start_date).days + 1
    num_record = sum([random.randint(math.ceil(args.min_per_day * args.family_count), math.ceil(args.max_per_day * args.family_count))
                      for _ in range(num_days)])
    for _ in range(num_record):
        d = fake.date_this_year(before_today=True, after_today=True)
        family = Family.random_family()
        tollbooth = random.randint(1, 3)
        nuts = random.choices(list(range(11)), weights=[int(x ** 1.5) for x in range(10, -1, -1)], k=3)
        yield d, tollbooth, family, nuts


def generate():
    # get command line args
    parse_arguments()

    # generate families
    [Family() for _ in range(args.family_count)]
    # save families into a file
    with open(os.path.join(args.output, 'squirreliwink_population.json'), 'w', encoding='utf-8') as f:
        for family in Family.families():
            f.write(family.to_json() + "\n")

    # generate and write tollbooth records
    with open(os.path.join(args.output, 'tollbooth_logs.csv'), mode='w', encoding='utf-8') as f:
        f.write('date,tollbooth,license_plate,cornsilk,slate_gray,navajo_white\n')
        for r in generate_tollbooth_logs():
            f.write(f'{r[0].strftime("%Y.%m.%d")},{r[1]},"{r[2].car}",{",".join([str(_) for _ in r[3]])}\n')


if __name__ == '__main__':
    generate()
