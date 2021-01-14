import pymongo
import pandas as pd
import pickle
import datetime
import time
import gzip
import lzma
import pytz
import numpy as np
import TSLPy3

def DB1(host, db_name, user, passwd):
    auth_db = db_name if user not in ('admin', 'root') else 'admin'
    url = 'mongodb://%s:%s@%s/?authSource=%s' % (user, passwd, host, auth_db)
    client = pymongo.MongoClient(url, maxPoolSize=None)
    db = client[db_name]
    return db

def build_query(start_date=None, end_date=None, index_id=None):
    query = {}

    def parse_date(x):
        if type(x) == int:
            return x
        elif type(x) == str:
            if len(x) != 8:
                raise Exception("`date` must be YYYYMMDD format")
            return int(x)
        elif type(x) == datetime.datetime or type(x) == datetime.date:
            return x.strftime("%Y%m%d").astype(int)
        else:
            raise Exception("invalid `date` type: " + str(type(x)))

    if start_date is not None or end_date is not None:
        query['date'] = {}
        if start_date is not None:
            query['date']['$gte'] = parse_date(start_date)
        if end_date is not None:
            query['date']['$lte'] = parse_date(end_date)

    def parse_symbol(x):
        if type(x) == int:
            return x
        else:
            return int(x)

    if index_id:
        if type(index_id) == list or type(index_id) == tuple:
            query['index_id'] = {'$in': [parse_symbol(x) for x in index_id]}
        else:
            query['index_id'] = parse_symbol(index_id)
    
    return query

def build_filter_query(start_date=None, end_date=None, skey=None):
    query = {}

    def parse_date(x):
        if type(x) == int:
            return x
        elif type(x) == str:
            if len(x) != 8:
                raise Exception("`date` must be YYYYMMDD format")
            return int(x)
        elif type(x) == datetime.datetime or type(x) == datetime.date:
            return x.strftime("%Y%m%d").astype(int)
        else:
            raise Exception("invalid `date` type: " + str(type(x)))

    if start_date is not None or end_date is not None:
        query['date'] = {}
        if start_date is not None:
            query['date']['$gte'] = parse_date(start_date)
        if end_date is not None:
            query['date']['$lte'] = parse_date(end_date)

    def parse_symbol(x):
        if type(x) == int:
            return x
        else:
            return int(x)

    if skey:
        if type(skey) == list or type(skey) == tuple:
            query['skey'] = {'$in': [parse_symbol(x) for x in skey]}
        else:
            query['skey'] = parse_symbol(skey)
    
    return query

def read_filter_daily(db, name, start_date=None, end_date=None, skey=None, interval=None, col=None, return_sdi=True):
    collection = db[name]
    # Build projection
    prj = {'_id': 0}
    if col is not None:
        if return_sdi:
            col = ['skey', 'date', 'interval'] + col
        for col_name in col:
            prj[col_name] = 1

    # Build query
    query = {}
    if skey is not None:
        query['skey'] = {'$in': skey}
    if interval is not None:
        query['interval'] = {'$in': interval}
    if start_date is not None:
        if end_date is not None:
            query['date'] = {'$gte': start_date, '$lte': end_date}
        else:
            query['date'] = {'$gte': start_date}
    elif end_date is not None:
        query['date'] = {'$lte': end_date}

    # Load data
    cur = collection.find(query, prj)
    df = pd.DataFrame.from_records(cur)
    if df.empty:
        df = pd.DataFrame()
    else:
        df = df.sort_values(by=['date','skey'])
    return df  


database_name = 'com_md_eq_cn'
user = 
password = 

pd.set_option('max_columns', 200)
db1 = DB1("192.168.10.178", database_name, user, password)

read_filter_daily(db1, 'md_stock_sizefilter', skey=[2000001])

