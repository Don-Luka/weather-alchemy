import datetime
import sqlalchemy


import csv

import sqlalchemy
from sqlalchemy import create_engine, MetaData, inspect
from sqlalchemy import Table, Column, ForeignKey
from sqlalchemy import Integer, String, Float, ForeignKey, DATE, text
from sqlalchemy import select, and_, cast, join, func, delete, update

from sqlalchemy.sql.functions import sum, count, max, min


def download_csv(filename: str): 
    data = []
    with open(filename, newline="") as csv_file:
        reader = csv.DictReader(csv_file)
        for row in reader:
            data.append(row)    
    return data

def convert_string_to_date(string, format = "%Y-%m-%d"):
    d = datetime.datetime.strptime(string, format).date()
    return d

def select_by_keys(engine, table, **query):
    '''
    **query is in format key1 = value1, key2 = value2, ...
    where key1, key2 are the names of the columns, such as name, date etc. (NOT table.c.name or table.c.date)
    * example of application:
    select_by_keys(engine, measure, date = '2017-01-01')
    '''
    stmt = select(table).filter_by(**query)
    with engine.connect() as conn:
        print('\n---table---:')
        for row in conn.execute(stmt):
            print(row)
        print('---end of the table---')

def select_all(engine, table):
    stmt = select(table)
    with engine.connect() as conn:
        print('\n---table---:')
        for row in conn.execute(stmt).all():
            print(row)
        print('---end of the table---')

def select_first_rows(engine, table, number_of_rows):
    stmt = select(table)
    with engine.connect() as conn:
        print('\n---table---:')
        for row in conn.execute(stmt).fetchmany(number_of_rows):
            print(row)
        print('---end of the table---')

def delete_where_value_between(engine, table, param, bottom_value, top_value):
    stmt = delete(table)\
        .where(param >= bottom_value)\
        .where(param <= top_value)
    with engine.connect() as conn:
        conn.execute(stmt)
        conn.commit()

def insert_data(engine, table, data):
    with engine.connect() as conn:
        conn.execute(table.insert(), data)
        conn.commit()

