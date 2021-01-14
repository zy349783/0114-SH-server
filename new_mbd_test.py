#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Sep 29 15:27:04 2020

@author: work516
"""
#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Sep 29 11:38:50 2020

@author: work11
"""
import os
import sys
import glob
import datetime
import numpy as np
import pandas as pd
from multiprocessing import Pool

funcPath = r'/mnt/d/work516/Downloads'
sys.path.append(funcPath)
from frozen_order import generateMBD

import os

os.environ['OMP_NUM_THREADS'] = '1'
import glob
import pymongo
import numpy as np
import pandas as pd
import pickle
import time
import gzip
import lzma
import pytz
import warnings
import glob
import datetime
from collections import defaultdict, OrderedDict

warnings.filterwarnings(action='ignore')


def DB(host, db_name, user, passwd):
    auth_db = db_name if user not in ('admin', 'root') else 'admin'
    uri = 'mongodb://%s:%s@%s/?authSource=%s' % (user, passwd, host, auth_db)
    return DBObj(uri, db_name=db_name)


class DBObj(object):
    def __init__(self, uri, symbol_column='skey', db_name='white_db'):
        self.db_name = db_name
        self.uri = uri
        self.client = pymongo.MongoClient(self.uri)
        self.db = self.client[self.db_name]
        self.symbol_column = symbol_column
        self.date_column = 'date'

    def parse_uri(self, uri):
        # mongodb://user:password@example.com
        return uri.strip().replace('mongodb://', '').strip('/').replace(':', ' ').replace('@', ' ').split(' ')

    def drop_table(self, table_name):
        self.db.drop_collection(table_name)

    def rename_table(self, old_table, new_table):
        self.db[old_table].rename(new_table)

    def write(self, table_name, df, chunk_size=20000):
        if len(df) == 0: return

        multi_date = False

        if self.date_column in df.columns:
            date = str(df.head(1)[self.date_column].iloc[0])
            multi_date = len(df[self.date_column].unique()) > 1
        else:
            raise Exception('DataFrame should contain date column')

        collection = self.db[table_name]
        collection.create_index([('date', pymongo.ASCENDING), ('symbol', pymongo.ASCENDING)], background=True)
        collection.create_index([('symbol', pymongo.ASCENDING), ('date', pymongo.ASCENDING)], background=True)

        if multi_date:
            for (date, symbol), sub_df in df.groupby([self.date_column, self.symbol_column]):
                date = str(date)
                symbol = int(symbol)
                collection.delete_many({'date': date, 'symbol': symbol})
                self.write_single(collection, date, symbol, sub_df, chunk_size)
        else:
            for symbol, sub_df in df.groupby([self.symbol_column]):
                collection.delete_many({'date': date, 'symbol': symbol})
                self.write_single(collection, date, symbol, sub_df, chunk_size)

    def write_single(self, collection, date, symbol, df, chunk_size):
        for start in range(0, len(df), chunk_size):
            end = min(start + chunk_size, len(df))
            df_seg = df[start:end]
            version = 1
            seg = {'ver': version, 'data': self.ser(df_seg, version), 'date': date, 'symbol': symbol, 'start': start}
            collection.insert_one(seg)

    def build_query(self, start_date=None, end_date=None, symbol=None):
        query = {}

        def parse_date(x):
            if type(x) == str:
                if len(x) != 8:
                    raise Exception("`date` must be YYYYMMDD format")
                return x
            elif type(x) == datetime.datetime or type(x) == datetime.date:
                return x.strftime("%Y%m%d")
            elif type(x) == int:
                return parse_date(str(x))
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

        if symbol:
            if type(symbol) == list or type(symbol) == tuple:
                query['symbol'] = {'$in': [parse_symbol(x) for x in symbol]}
            else:
                query['symbol'] = parse_symbol(symbol)

        return query

    def delete(self, table_name, start_date=None, end_date=None, symbol=None):
        collection = self.db[table_name]

        query = self.build_query(start_date, end_date, symbol)
        if not query:
            print('cannot delete the whole table')
            return None

        collection.delete_many(query)

    def read(self, table_name, start_date=None, end_date=None, symbol=None):
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

    def ser(self, s, version):
        if version == 1:
            return gzip.compress(pickle.dumps(s), compresslevel=2)
        elif version == 2:
            return lzma.compress(pickle.dumps(s), preset=1)
        else:
            raise Exception('unknown version')

    def deser(self, s, version):
        def unpickle(s):
            return pickle.loads(s)

        if version == 1:
            return unpickle(gzip.decompress(s))
        elif version == 2:
            return unpickle(lzma.decompress(s))
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
            
            
def mbdGene(stockData):

    thisDateStr = str(stockData['date'].values[0])
    thisStock = stockData['skey'].values[0]
    stockData['time'] = stockData['time'] / 1000
    stockData['order_price'] = (stockData['order_price'] * 10000).round(0)
    stockData['trade_price'] = (stockData['trade_price'] * 10000).round(0)
    try:
        stockData['isAuction'] = np.where(stockData['time'] < 92900000, True, False)
        stockData = stockData[stockData['time'] < 145655000].reset_index(drop=True)
        hasAuction = True if stockData[stockData['isAuction'] == True].shape[0] > 0 else False
        simMarket = generateMBD(skey=thisStock, date=int(thisDateStr), hasAuction=hasAuction)
        stockDataNP = stockData.to_records()
        for rowEntry in stockDataNP:
            if rowEntry.isAuction:
                if rowEntry.status == 'order':
                    simMarket.insertAuctionOrder(rowEntry.clockAtArrival, rowEntry.time, rowEntry.ApplSeqNum,
                                                 rowEntry.order_side, rowEntry.order_type, rowEntry.order_price, rowEntry.order_qty)
        
                elif rowEntry.status == 'cancel':
                    simMarket.removeOrderByAuctionCancel(rowEntry.clockAtArrival, rowEntry.time, rowEntry.ApplSeqNum,
                                                         rowEntry.trade_qty, rowEntry.BidApplSeqNum, rowEntry.OfferApplSeqNum)
        
                elif rowEntry.status == 'trade':
                    simMarket.removeOrderByAuctionTrade(rowEntry.clockAtArrival, rowEntry.time, rowEntry.ApplSeqNum,
                                                        rowEntry.trade_price, rowEntry.trade_qty, rowEntry.BidApplSeqNum, rowEntry.OfferApplSeqNum)
            else:      
                if rowEntry.ApplSeqNum == 33643:
                    print('llll')
                if rowEntry.ApplSeqNum == 30254:
                    print('llll')
                if rowEntry.status == 'order':
                    simMarket.insertOrder(rowEntry.clockAtArrival, rowEntry.time, rowEntry.ApplSeqNum, rowEntry.order_side,
                                          rowEntry.order_type, rowEntry.order_price, rowEntry.order_qty)
        
                elif rowEntry.status == 'cancel':
                    simMarket.removeOrderByCancel(rowEntry.clockAtArrival, rowEntry.time, rowEntry.ApplSeqNum,
                                                  rowEntry.trade_qty, rowEntry.BidApplSeqNum, rowEntry.OfferApplSeqNum)
        
                elif rowEntry.status == 'trade':
                    simMarket.removeOrderByTrade(rowEntry.clockAtArrival, rowEntry.time, rowEntry.ApplSeqNum,
                                                 rowEntry.trade_price, rowEntry.trade_qty, rowEntry.BidApplSeqNum,
                                                 rowEntry.OfferApplSeqNum)
        
        simData = simMarket.getSimMktInfo()
        print(simData)
        
    except Exception as e:
        print(thisStock)
        print(e)
        
        
        
thisDate = datetime.date(2020, 10, 26)
while thisDate <= datetime.date(2020, 10, 26):
    thisDate_str = str(thisDate).replace('-', '')
    db = DB("192.168.10.178", 'com_md_eq_cn', 'zhenyuy', 'bnONBrzSMGoE')

    mdOrderLog = pd.read_csv('/mnt/dailyRawData/20201026/logs_20201026_zs_96_03_day_pcap/mdOrderPcap_SZ_20201026_0900.csv')
    mdOrderLog = mdOrderLog[mdOrderLog['ID'] == 2123015]
    
    if mdOrderLog is None:
        thisDate = thisDate + datetime.timedelta(days=1)
        continue
    print(thisDate)
            
    mdOrderLog["OrderType"] = np.where(mdOrderLog["OrderType"] == 'U', 3, mdOrderLog["OrderType"])
    mdOrderLog = mdOrderLog.rename(columns={'ID':'skey', 'time':'TransactTime'})
    mdOrderLog['date'] = 20201026
    mdOrderLog['time1'] = 20201026 * 1000000000 + mdOrderLog['TransactTime']
    mdOrderLog["TransactTime"] = mdOrderLog['TransactTime'].astype('int64') * 1000
    mdOrderLog["clockAtArrival"] = mdOrderLog["time1"].astype(str).apply(
        lambda x: np.int64(datetime.datetime.strptime(x, '%Y%m%d%H%M%S%f').timestamp() * 1e6))
    mdOrderLog.drop("time1", axis=1, inplace=True)
    mdOrderLog['datetime'] = mdOrderLog["clockAtArrival"].apply(lambda x: datetime.datetime.fromtimestamp(x / 1e6))
    mdOrderLog = mdOrderLog.rename(columns={"TransactTime":"time"})
    
    for col in ["skey", "date", "ApplSeqNum", "OrderQty", "Side", "OrderType"]:
        mdOrderLog[col] = mdOrderLog[col].astype('int32')
    for cols in ["Price"]:
        mdOrderLog[cols] = (mdOrderLog[cols]/10000).round(2)
    
    mdOrderLog = mdOrderLog.rename(columns={"Side":"order_side", "OrderType":"order_type", "Price":"order_price", "OrderQty":"order_qty"})
    mdOrderLog = mdOrderLog[["skey", "date", "time", "clockAtArrival", "datetime", "ApplSeqNum", "order_side", "order_type", "order_price", "order_qty"]]
    
    mdTradeLog = pd.read_csv('/mnt/dailyRawData/20201026/logs_20201026_zs_96_03_day_pcap/mdTradePcap_SZ_20201026_0900.csv')
    mdTradeLog = mdTradeLog[mdTradeLog['ID'] == 2123015]
    
    mdTradeLog = mdTradeLog.rename(columns={'ID':'skey', 'time':'TransactTime'})
    mdTradeLog["TradeBSFlag"] = 'N'
    mdTradeLog['date'] = 20201026
    mdTradeLog['time1'] = 20201026 * 1000000000 + mdTradeLog['TransactTime']
    mdTradeLog["TransactTime"] = mdTradeLog['TransactTime'].astype('int64') * 1000
    mdTradeLog["clockAtArrival"] = mdTradeLog["time1"].astype(str).apply(
        lambda x: np.int64(datetime.datetime.strptime(x, '%Y%m%d%H%M%S%f').timestamp() * 1e6))
    mdTradeLog.drop("time1", axis=1, inplace=True)
    mdTradeLog['datetime'] = mdTradeLog["clockAtArrival"].apply(lambda x: datetime.datetime.fromtimestamp(x / 1e6))
    mdTradeLog['TradePrice'] = (mdTradeLog['TradePrice'] / 10000).round(2)
    mdTradeLog = mdTradeLog.rename(columns={"TradeQty":"trade_qty", "TradePrice":"trade_price", "ExecType":"trade_type", 'TransactTime':'time'})
    mdTradeLog["trade_flag"] = 0
    mdTradeLog["trade_type"] = np.where(mdTradeLog["trade_type"] == 'F', 1, mdTradeLog["trade_type"])
    for col in ["skey", "date", "ApplSeqNum", "BidApplSeqNum", "OfferApplSeqNum", "trade_qty", "trade_type", "trade_flag"]:
        mdTradeLog[col] = mdTradeLog[col].astype('int32')
     
    mdTradeLog = mdTradeLog[["skey", "date", "time", "clockAtArrival", "datetime", "ApplSeqNum", "trade_type", "trade_flag",
                                                 "trade_price", "trade_qty", "BidApplSeqNum", "OfferApplSeqNum"]]

    
    mdOrderLog['status'] = 'order'

    assert(mdTradeLog['trade_type'].nunique() == 2)
    mdTradeLog['status'] = np.where(mdTradeLog['trade_type'] == 1, 'trade', 'cancel')
    
    msgData = pd.concat([mdOrderLog, mdTradeLog], sort=False)
    del mdOrderLog
    del mdTradeLog
    
    msgData = msgData.sort_values(by=['skey', 'ApplSeqNum']).reset_index(drop=True)
    
    start = time.time()
    mbdGene(msgData)
    print(time.time() - start)
    
    print('finished ' + thisDate_str)
    thisDate = thisDate + datetime.timedelta(days=1)
    
    
    
    
