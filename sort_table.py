import datetime
import os
import pandas as pd
import locale
import psycopg2
from sqlalchemy import create_engine
from sqlalchemy.engine import URL
from sqlalchemy import Integer, String, Column, DateTime, Float
import sqlalchemy

def get_connection():
    return psycopg2.connect(
        host='localhost',
        port='5432',
        database='postgres',
        user='postgres',
        password='password'
    )

def get_sqlalchemy_connection():
    url = URL.create(
        drivername='postgresql',
        username='postgres',
        host='localhost',
        database='postgres',
        port='5432',
        password='password'
    )
    engine = create_engine(url, isolation_level="AUTOCOMMIT")
    connection = engine.connect()
    return connection

con = get_connection()
# cluster reviews on price
cur = con.cursor()
cur.execute("CREATE INDEX product_price ON products (price);")
cur.execute("CLUSTER products USING product_price")
con.close()