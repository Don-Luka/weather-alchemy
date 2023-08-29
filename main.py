import sys
import csv
import datetime

import logging
logging.basicConfig(level=logging.DEBUG)

from functions_alchemy import *

db_filename = 'air.db'

engine = create_engine(f'sqlite:///{db_filename}', echo = False)
# engine = create_engine(f'sqlite:///:memory:', echo = False)
meta = MetaData()

stations = Table(
    'stations', meta,
    Column('station', String, primary_key = True),
    Column('latitude', Float),
    Column('longitude', Float),
    Column('elevation', Float),
    Column('name', String),
    Column('country', String),
    Column('state', String)
        )

measure = Table(
    'measure', meta,
    Column('station', ForeignKey("stations.station")),
    Column('date', String),
    # Column('date', DATE),
    Column('precip', Float),
    Column('tobs', Integer)
        )

meta.drop_all(engine)
meta.create_all(engine)

stations_data = download_csv("clean_stations.csv")
measure_data = download_csv("clean_measure.csv")

# CREATE
insert_data(engine, stations, stations_data)
insert_data(engine, measure, measure_data)

newdata = [
    {'station': 'A',
                'latitude': 120,
                'longitude': 150,
                'elevation': 10,
                'name': 'Honolulu_1',
                'country':'usa',
                'state': 'hawaii'},
                    {'station': 'B',
                'latitude': 125,
                'longitude': 110,
                'elevation': 3,
                'name': 'Honolulu_2',
                'country':'usa',
                'state': 'hawaii'},
]

insert_data(engine, stations, newdata)
select_all(engine, stations)

# READ
select_all(engine, measure)

# UPDATE
stmt = stations.update()\
    .where(stations.c.station == 'A')\
        .values({"station" : 'USC00000000'})\
            .values({"country" : 'USA'})
with engine.connect() as conn:
    conn.execute(stmt)
    conn.commit()
select_all(engine, stations)

# DELETE
# Example of deleting measures by date and precipitation
delete_where_value_between(engine, measure, measure.c.date,
                           '2010-01-01',
                           '2017-01-01')
select_all(engine, measure)
delete_where_value_between(engine, measure, measure.c.precip,
                           0.0,
                           0.42)
select_all(engine, measure)

select_first_rows(engine, measure, 3)

# JOIN
def joint():
    j = join(stations, measure, stations.c.station == measure.c.station)
    print(j)
    stmt = select(stations.c.name, measure.c.precip, measure.c.tobs)\
        .select_from(j).where(measure.c.tobs > 70)\
            .where(measure.c.precip > 1)\
                .order_by(measure.c.precip.desc())\
                    .order_by(measure.c.tobs)
    
    with engine.connect() as conn:
        for row in conn.execute(stmt).all():
            print(row)
joint()

# SUM PRECIPITATION
stmt = func.sum(measure.c.precip)
with engine.connect() as conn:
    sum_precip  = conn.execute(stmt).scalar()
print(f"{sum_precip = }")
    # print(result.all())

# AVERAGE PRECIPITATION
stmt = func.count(measure.c.precip)
with engine.connect() as conn:
    count_precip  = conn.execute(stmt).scalar()
print(f"{count_precip = }")

print(f"Average precipitation: {sum_precip/count_precip}")

stmt = func.max(measure.c.precip)
with engine.connect() as conn:
    max_precip  = conn.execute(stmt).scalar()
print(f"{max_precip = }")

sys.exit()
