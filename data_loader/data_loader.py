'''
This file loads the YELP reviews table into a
SQL server database, however I would suggest not using it.
the datset has 4.7 million records and this load took 2 full days
because of the transaction lock and slowness of the code 
'''
import pyodbc
import os
DATA_PATH = 'path_to_yelp_reviews.json'
datafiles = os.listdir(DATA_PATH)
datafiles = [os.path.join(DATA_PATH, x) for x in datafiles]
conn = pyodbc.connect("DSN=YELP")
cursor = conn.cursor()
from sqlalchemy import create_engine
engine=create_engine('mssql+pyodbc://username@YELP')


def create_table():
    cursor.execute('''IF OBJECT_ID('YELP.dbo.reviews', 'U') IS NULL
       BEGIN CREATE TABLE [YELP].[dbo].[reviews]
    (review_id varchar(100),
    user_id varchar(100),
    business_id varchar(100),
    stars int,
    date datetime,
    text TEXT,
    useful int,
    funny int,
    cool int,
    PRIMARY KEY(review_id)) END''')
    cursor.commit()

create_table()


from sqlalchemy import MetaData, Table, insert, delete
metadata = MetaData(engine)
conn = engine.connect()
reviews = Table('reviews', metadata, autoload=True, autoload_with=engine)

def insert_yelp(values):
    i = insert(reviews)
    i = i.values(values)
    conn.execute(i)
    
# inserting the data into the table

import json
with open(datafiles[3], 'r', encoding='utf8') as fh:
    for line in fh:
        data = json.loads(line)
        insert_yelp(data)

conn.commit()
conn.close()
cursor.close()



# https://www.dataquest.io/blog/python-json-tutorial/