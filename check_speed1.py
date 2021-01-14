#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Dec 30 01:54:46 2020

@author: zhenyu
"""
import pymongo
import io
import pandas as pd
import pickle
import datetime
import time
import gzip
import lzma
import pytz
import pyarrow as pa
import pyarrow.parquet as pq
import numpy as np
from random import randint
from multiprocessing import Pool
import glob
import os
os.environ['OMP_NUM_THREADS'] = '1'
os.environ['OMP_THREAD_LIMIT'] = '1'


def DB(host, db_name, user, passwd):
    auth_db = db_name if user not in ('admin', 'root') else 'admin'
    uri = 'mongodb://%s:%s@%s/?authSource=%s' % (user, passwd, host, auth_db)
    return DBObj(uri, db_name=db_name)

class DBObj(object):
    def __init__(self, uri, symbol_column='skey', db_name='white_db', version=3): 
        self.db_name = db_name 
        self.uri = uri 
        self.client = pymongo.MongoClient(self.uri) 
        self.db = self.client[self.db_name] 
        self.chunk_size = 20000 
        self.symbol_column = symbol_column 
        self.date_column = 'date' 
        self.version = version

    def parse_uri(self, uri): 
        # mongodb://user:password@example.com 
        return uri.strip().replace('mongodb://', '').strip('/').replace(':', ' ').replace('@', ' ').split(' ')

    def build_query(self, start_date=None, end_date=None, symbol=None):
        query = {}
        def parse_date(x):
            if type(x) == str:
                if len(x) != 8:
                    raise Exception("date must be YYYYMMDD format")
                return x
            elif type(x) == datetime.datetime or type(x) == datetime.date:
                return x.strftime("%Y%m%d")
            elif type(x) == int:
                return parse_date(str(x))
            else:
                raise Exception("invalid date type: " + str(type(x)))
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
        if symbol:
            if type(symbol) == list or type(symbol) == tuple:
                query['symbol'] = {'$in': [parse_symbol(x) for x in symbol]}
            else:
                query['symbol'] = parse_symbol(symbol)
        return query

    def read_tick(self, table_name, start_date=None, end_date=None, symbol=None):
        collection = self.db[table_name] 
        query = self.build_query(start_date, end_date, symbol) 
        if not query: 
            print('cannot read the whole table') 
            return None  
        segs = [] 
        for x in collection.find(query): 
            x['data'] = self.deser(x['data'], x['ver']) 
            segs.append(x) 
        segs.sort(key=lambda x: (x['symbol'], x['date'], x['start'])) 
        return pd.concat([x['data'] for x in segs], ignore_index=True) if segs else None

    def read_tick1(self, table_name, start_date=None, end_date=None, symbol=None):
        collection = self.db[table_name] 
        query = self.build_query(start_date, end_date, symbol) 
        if not query: 
            print('cannot read the whole table') 
            return None  
        segs = [] 
        start_time = time.time()
        for x in collection.find(query): 
            x['data'] = self.deser(x['data'], x['ver']) 
            segs.append(x) 
        segs.sort(key=lambda x: (x['symbol'], x['date'], x['start'])) 
        time1 = time.time() - start_time
        start_time = time.time()
        data = pd.concat([x['data'] for x in segs], ignore_index=True) if segs else None
        time2 = time.time() - start_time
        print(str(time1) + ',' + str(time2))
        return time1, time2
    
        
    def read_daily(self, table_name, start_date=None, end_date=None, skey=None, index_id=None, interval=None, index_name=None, col=None, return_sdi=True): 
        collection = self.db[table_name]
        # Build projection 
        prj = {'_id': 0} 
        if col is not None: 
            if return_sdi: 
                col = ['skey', 'date', 'index_id'] + col 
            for col_name in col: 
                prj[col_name] = 1 
        # Build query 
        query = {} 
        if skey is not None: 
            query['skey'] = {'$in': skey} 
        if interval is not None: 
            query['interval'] = {'$in': interval} 
        if index_id is not None: 
            query['index_id'] = {'$in': index_id}    
        if index_name is not None:
            n = '' 
            for name in index_name: 
                try: 
                    name = re.compile('[\u4e00-\u9fff]+').findall(name)[0] 
                    if len(n) == 0: 
                        n = n = "|".join(name) 
                    else: 
                        n = n + '|' + "|".join(name) 
                except: 
                    if len(n) == 0: 
                        n = name 
                    else: 
                        n = n + '|' + name 
            query['index_name'] = {'$regex': n}
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
            if 'index_id' in df.columns:
                df = df.sort_values(by=['date', 'index_id', 'skey']).reset_index(drop=True)
            else:
                df = df.sort_values(by=['date','skey']).reset_index(drop=True)
        return df 
 
    def list_tables(self):
        return self.db.collection_names()

    def list_dates(self, table_name, start_date=None, end_date=None, symbol=None):
        collection = self.db[table_name]
        dates = set()
        if start_date is None:
            start_date = '00000000'
        if end_date is None:
            end_date = '99999999'
        for x in collection.find(self.build_query(start_date, end_date, symbol), {"date": 1, '_id': 0}):
            dates.add(x['date'])
        return sorted(list(dates))

    def deser(self, s, version): 
        def unpickle(s): 
            return pickle.loads(s) 
        if version == 1: 
            return unpickle(gzip.decompress(s)) 
        elif version == 2: 
            return unpickle(lzma.decompress(s)) 
        elif version == 3: 
            f = io.BytesIO() 
            f.write(s) 
            f.seek(0) 
            return pq.read_table(f, use_threads=False).to_pandas() 
        else: 
            raise Exception('unknown version')

def patch_pandas_pickle():
    if pd.__version__ < '0.24':
        import sys
        from types import ModuleType
        from pandas.core.internals import BlockManager
        pkg_name = 'pandas.core.internals.managers'
        if pkg_name not in sys.modules:
            m = ModuleType(pkg_name)
            m.BlockManager = BlockManager
            sys.modules[pkg_name] = m
patch_pandas_pickle()

import random
random.seed(1)

# for i in range(20):
#   db1.read_tick1('md_trade', 20201211, 20201211, symbol=sl)


def f(x):
    database_name = 'com_md_eq_cn'
    user = 'zhenyuy'
    password = 'bnONBrzSMGoE'
    pd.set_option('max_columns', 200)
    random.seed(1)
    sl = [1600681, 2002117, 2300277, 2300087, 2002946, 1600312, 1603050, 1600598, 2000726, 2002932, 2000408, 2000596, 2002460, 1605336, 2300030, 1601211, 1600486, 2000686, 1600145, 2300469, 2300229, 1688055, 2000010, 2002278, 2002939, 2002961, 1600073, 2002646, 2000151, 1603123, 2002758, 2300085, 1601702, 2002208, 2300687, 1600525, 2300502, 1603609, 1600156, 1600063, 1600079, 2002452, 2001979, 1600025, 2300662, 2300418, 1605399, 2002607, 1601369, 2300795, 1688521, 2002777, 1600148, 2000928, 1601598, 2300912, 2000037, 2300869, 2000727, 2002048, 1601827, 1603825, 1601789, 2002567, 1601512, 2300907, 2000528, 2300718, 1603323, 2300608, 1600123, 1688369, 2300236, 2300565, 2002063, 2300590, 2002421, 1600515, 1600958, 2002368, 2002768, 2300328, 1603368, 1600612, 2002853, 1603712, 2300478, 2300884, 2300813, 2002713, 2000762, 2300651, 2300786, 2300863, 2000807, 2300204, 2300539, 2002540, 1600977, 1603486, 1603288, 2002193, 2300423, 2000755, 2300272, 2300669, 2000795, 1688088, 2002199, 2300301, 1600173, 2000636, 1601989, 2002857, 2300067, 1688196, 1688357, 2002516, 1600856, 1603987, 2002031, 2300424, 2002675, 2002999, 2002557, 2002831, 1605128, 1600436, 2000042, 2002512, 2000811, 1600555, 2003010, 1600812, 2000893, 2300246, 1688086, 1605006, 2000697, 2002808, 1600151, 2000581, 1600218, 1603535, 2002677, 2300280, 2002308, 2002216, 2002155, 1688089, 2002441, 1600843, 1600834, 2000778, 1601678, 1600104, 2002977, 1601038, 2000998, 2300581, 2300329, 2002029, 1601808, 1688198, 2000839, 1603815, 2300719, 2300915, 2002153, 1603888, 2000525, 2300537, 1603157, 2002493, 2002028, 2002284, 2002792, 1600017, 1688010, 2300008, 2300316, 2300164, 2300439, 2300659, 2002842, 2000833, 2300117, 1600653, 2000882, 2003006, 2002085, 1601138, 1688571, 1600279, 2000639, 2300371, 1603970, 2002120, 2002054, 1601058, 2300670, 2000791, 1688336, 2000671, 2300134, 1603909, 2300791, 1603828, 1600072, 2000989, 2001696, 2002343, 2300020, 2002298, 1603703, 2000520, 2002245, 1600141, 2300097, 1601727, 2002393, 1600873, 2002039, 2002180, 1600892, 2300332, 1600475, 2300071, 2002041, 2300065, 2300292, 2300147, 2300628, 1603053, 1600163, 2300253, 2002552, 1600350, 1600415, 2300363, 1600113, 2000426, 1600038, 2002901, 2002908, 1603258, 1603021, 1603155, 1600561, 2300066, 2002349, 1600929, 1603817, 1603328, 1600343, 1600828, 1600793, 2300665, 2000927, 1600831, 2002482, 1603183, 2002446, 2002714, 1603357, 2000502, 2002673, 1603639, 2000731, 2000603, 1600582, 1600128, 1603577, 1688023, 1603811, 1688516, 2300062, 1600968, 1603078, 1600558, 1603039, 2300495, 2002796, 2000820, 1601199, 2002270, 2000006, 2300149, 1600060, 1601636, 1600054, 1688123, 1600733, 1600179, 2002746, 1600796, 2000153, 2002683, 2000799, 2002572, 1688578, 2002014, 2300213, 1601568, 2002374, 2300068, 2002642, 2000863, 2000415, 1601608, 2000909, 2002447, 2300876, 1688098, 2002559, 2002145, 2300092, 1603636, 2002495, 2002375, 1688577, 1600291, 2002828, 1603387, 1600637, 1601231, 2300395, 1600235, 1603516, 1600351, 2300322, 1600378, 1603559, 1603383, 2300864, 1600787, 2300840, 2002099, 1603033, 1600660, 1600095, 2002082, 2300408, 2300289, 1600191, 2002206, 2300160, 1601390, 2300499, 2002121, 2000533, 1600848, 2300196, 2300360, 2300703, 2003017, 2002679, 2002341, 2000813, 1600189, 1605255, 1601068, 1603839, 1600510, 1601139, 2002134, 2002556, 2300482, 2000011, 2002209, 1600997, 2000711, 1600535, 2002521, 1688056, 1603366, 2000788, 2000757, 1600114, 1603667, 2002297, 2300379, 1688178, 2300616, 1603259, 1600116, 1600780, 1601077, 2300318, 1603683, 2300125, 2002093, 2300004, 1600684, 1603777, 1688599, 1601258, 1603126, 2300782, 1600498, 2300237, 1605333, 2002027, 2300737, 2300807, 2300492, 2002610, 2000967, 2000668, 2002965, 2000959, 1601866, 1600322, 2002775, 1600202, 1600422, 1600673, 1600839, 1600825, 2000988, 2300496, 1603136, 2002925, 1603709, 2002246, 2000798, 2300251, 2300640, 1603991, 1603776, 1603788, 1600581, 1603331, 1601872, 2300358, 2002263, 2003015, 2002730, 2300444, 2000691, 1600685, 2002162, 2300698, 2002975, 2300520, 1603630, 1600197, 1688222, 1600362, 1605366, 2300353, 2300027, 1600737, 2300198, 1600635, 1603797, 1600584, 2002309, 2300808, 2300001, 2300533, 2300563, 2002123, 2002037, 1601611, 2002104, 1600403, 1603127, 1603976, 1603363, 2002097, 2300485, 1600583, 2000519, 1603218, 1600552, 2300022, 1600228, 2300195, 1603365, 2300748, 2002303, 2002541, 2300656, 2300672, 2300696, 1600586, 2300188, 2300035, 1600200, 1600967, 1601929, 2300016, 2002190, 2300622, 1600804, 1600588, 2300587, 2300642, 2002583]
    db1 = DB("192.168.10.178", database_name, user, password)
    return db1.read_tick1('md_trade', 20201211, 20201211, symbol=sl)


def f1(x):
    database_name = 'com_md_eq_cn'
    user = 'zhenyuy'
    password = 'bnONBrzSMGoE'
    pd.set_option('max_columns', 200)
    start_date = 20181122
    end_date = 20201211
    db1 = DB("192.168.10.178", database_name, user, password)
    return db1.read_tick1('md_trade', start_date, end_date, symbol=2000001)
  
def f2(x):
    database_name = 'com_md_eq_cn'
    user = 'zhenyuy'
    password = 'bnONBrzSMGoE'
    pd.set_option('max_columns', 200)
    start_date = 20201211
    end_date = 20201211
    db1 = DB("192.168.10.178", database_name, user, password)
    return db1.read_tick1('md_trade', start_date, end_date, symbol=2000001)
    
if __name__ == '__main__':
    n = [randint(0,72) for i in range(72)]
    with Pool(72) as p:
        re = p.map(f, n)
        re = pd.DataFrame(re)
        re.columns=['time1', 'time2']
        re['case'] = 'case1'
        re['mode'] = 'local load db'
        re['core'] = 72
        re.to_csv('/mnt/e/md_trade/local_db/core_72/case1.csv')   
        
    with Pool(72) as p:
        re = p.map(f1, n)
        re = pd.DataFrame(re)
        re.columns=['time1', 'time2']
        re['case'] = 'case2'
        re['mode'] = 'local load db'
        re['core'] = 72
        re.to_csv('/mnt/e/md_trade/local_db/core_72/case2.csv')          
        
    with Pool(72) as p:
        re = p.map(f2, n)
        re = pd.DataFrame(re)
        re.columns=['time1', 'time2']
        re['case'] = 'case3'
        re['mode'] = 'local load db'
        re['core'] = 72
        re.to_csv('/mnt/e/md_trade/local_db/core_72/case3.csv')     
# t1 = []
# t2 = []
# case = []
# mode = []
# core = []
# for i in range(20):
#     re = f(i)
#     t1.append(re[0])
#     t2.append(re[1])
#     case.append('case1')
#     mode.append('local load db')
#     core.append(1)
# for i in range(20):
#     re = f1(i)
#     t1.append(re[0])
#     t2.append(re[1])
#     case.append('case2')
#     mode.append('local load db')
#     core.append(1)
# for i in range(20):
#     re = f2(i)
#     t1.append(re[0])
#     t2.append(re[1])
#     case.append('case3')
#     mode.append('local load db')
#     core.append(1)

# df = pd.DataFrame()
# df['time1'] = t1
# df['time2'] = t2
# df['case'] = case
# df['mode'] = mode
# df['core'] = core
# df.to_csv('/mnt/e/md_trade/local_db/core_1/' + '0' + '.csv')