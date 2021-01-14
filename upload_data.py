#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri Aug 14 11:08:10 2020

@author: work516
"""

import numpy as np
import pandas as pd
import pymongo
import pandas as pd
import pickle
import datetime
import time
import gzip
import lzma
import pytz


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
        self.chunk_size = 20000
        self.symbol_column = symbol_column
        self.date_column = 'date'

    def parse_uri(self, uri):
        # mongodb://user:password@example.com
        return uri.strip().replace('mongodb://', '').strip('/').replace(':', ' ').replace('@', ' ').split(' ')

    def drop_table(self, table_name):
        self.db.drop_collection(table_name)

    def rename_table(self, old_table, new_table):
        self.db[old_table].rename(new_table)

    def write(self, table_name, df):
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
                self.write_single(collection, date, symbol, sub_df)
        else:
            for symbol, sub_df in df.groupby([self.symbol_column]):
                collection.delete_many({'date': date, 'symbol': symbol})
                self.write_single(collection, date, symbol, sub_df)

    def write_single(self, collection, date, symbol, df):
        for start in range(0, len(df), self.chunk_size):
            end = min(start + self.chunk_size, len(df))
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
        pickle_protocol = 4
        if version == 1:
            return gzip.compress(pickle.dumps(s, protocol=pickle_protocol), compresslevel=2)
        elif version == 2:
            return lzma.compress(pickle.dumps(s, protocol=pickle_protocol), preset=1)
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
patch_pandas_pickle()




import pickle
from matplotlib import pyplot as plt
from matplotlib.ticker import Formatter
import collections
import glob
import os
import datetime


y = '20200818'
print('----------------------------------------------------------------')
print(y)



print('~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~SH snapshot data~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~')

readPath = '/mnt/dailyRawData/' + y + '/logs_' + y + '_zt_88_03_day_pcap/mdL2Pcap_SH_***'
dataPathLs = np.array(glob.glob(readPath))
startTm = datetime.datetime.now()
logSH = pd.read_csv(dataPathLs[0])
print(datetime.datetime.now() - startTm)

startTm = datetime.datetime.now()
logSH = logSH.rename(columns={'ID': 'skey'})
logSH = logSH[(logSH['skey'] >= 1600000) & (logSH['skey'] < 1700000)]
logSH = logSH[["skey", "time", "cum_volume", "cum_amount", "cum_tradesCnt", "prevClose",
                  "open", "high", "low", "close", 'bid10p', 'bid9p', 'bid8p', 'bid7p', 'bid6p', 'bid5p', 'bid4p',
                  'bid3p', 'bid2p', 'bid1p',
                  'ask1p', 'ask2p', 'ask3p', 'ask4p', 'ask5p', 'ask6p', 'ask7p', 'ask8p', 'ask9p', 'ask10p', 'bid10q',
                  'bid9q', 'bid8q',
                  'bid7q', 'bid6q', 'bid5q', 'bid4q', 'bid3q', 'bid2q', 'bid1q', 'ask1q', 'ask2q', 'ask3q', 'ask4q',
                  'ask5q', 'ask6q',
                  'ask7q', 'ask8q', 'ask9q', 'ask10q', 'bid10n', 'bid9n', 'bid8n', 'bid7n', 'bid6n', 'bid5n', 'bid4n',
                  'bid3n', 'bid2n', 'bid1n',
                  'ask1n', 'ask2n', 'ask3n', 'ask4n', 'ask5n', 'ask6n', 'ask7n', 'ask8n', 'ask9n', 'ask10n', 'bid1Top1q',
                  'bid1Top2q', 'bid1Top3q', 'bid1Top4q', 'bid1Top5q', 'bid1Top6q',
                  'bid1Top7q', 'bid1Top8q', 'bid1Top9q', 'bid1Top10q', 'bid1Top11q', 'bid1Top12q', 'bid1Top13q',
                  'bid1Top14q', 'bid1Top15q', 'bid1Top16q', 'bid1Top17q', 'bid1Top18q',
                  'bid1Top19q', 'bid1Top20q', 'bid1Top21q', 'bid1Top22q', 'bid1Top23q', 'bid1Top24q', 'bid1Top25q',
                  'bid1Top26q', 'bid1Top27q', 'bid1Top28q', 'bid1Top29q',
                  'bid1Top30q', 'bid1Top31q', 'bid1Top32q', 'bid1Top33q', 'bid1Top34q', 'bid1Top35q', 'bid1Top36q',
                  'bid1Top37q', 'bid1Top38q', 'bid1Top39q', 'bid1Top40q',
                  'bid1Top41q', 'bid1Top42q', 'bid1Top43q', 'bid1Top44q', 'bid1Top45q', 'bid1Top46q', 'bid1Top47q',
                  'bid1Top48q', 'bid1Top49q', 'bid1Top50q', 'ask1Top1q',
                  'ask1Top2q', 'ask1Top3q', 'ask1Top4q', 'ask1Top5q', 'ask1Top6q', 'ask1Top7q', 'ask1Top8q', 'ask1Top9q',
                  'ask1Top10q', 'ask1Top11q', 'ask1Top12q', 'ask1Top13q',
                  'ask1Top14q', 'ask1Top15q', 'ask1Top16q', 'ask1Top17q', 'ask1Top18q', 'ask1Top19q', 'ask1Top20q',
                  'ask1Top21q', 'ask1Top22q', 'ask1Top23q',
                  'ask1Top24q', 'ask1Top25q', 'ask1Top26q', 'ask1Top27q', 'ask1Top28q', 'ask1Top29q', 'ask1Top30q',
                  'ask1Top31q', 'ask1Top32q', 'ask1Top33q',
                  'ask1Top34q', 'ask1Top35q', 'ask1Top36q', 'ask1Top37q', 'ask1Top38q', 'ask1Top39q', 'ask1Top40q',
                  'ask1Top41q', 'ask1Top42q', 'ask1Top43q',
                  'ask1Top44q', 'ask1Top45q', 'ask1Top46q', 'ask1Top47q', 'ask1Top48q', 'ask1Top49q', 'ask1Top50q',
                  "totalBidQuantity", "totalAskQuantity",
                  "vwapBid", "vwapAsk", "totalBidOrders", "totalAskOrders", "totalBidLevels", "totalAskLevels",
                  "bidTradeMaxDuration", "askTradeMaxDuration",
                  "cum_canceledBuyOrders", "cum_canceledBuyVolume", "cum_canceledBuyAmount", "cum_canceledSellOrders",
                  "cum_canceledSellVolume", "cum_canceledSellAmount"]]
logSH.columns = ["skey", "time", "cum_volume", "cum_amount", "cum_trades_cnt", "prev_close",
                  "open", "high", "low", "close", 'bid10p', 'bid9p', 'bid8p', 'bid7p', 'bid6p', 'bid5p', 'bid4p',
                  'bid3p', 'bid2p', 'bid1p',
                  'ask1p', 'ask2p', 'ask3p', 'ask4p', 'ask5p', 'ask6p', 'ask7p', 'ask8p', 'ask9p', 'ask10p', 'bid10q',
                  'bid9q', 'bid8q',
                  'bid7q', 'bid6q', 'bid5q', 'bid4q', 'bid3q', 'bid2q', 'bid1q', 'ask1q', 'ask2q', 'ask3q', 'ask4q',
                  'ask5q', 'ask6q',
                  'ask7q', 'ask8q', 'ask9q', 'ask10q', 'bid10n', 'bid9n', 'bid8n', 'bid7n', 'bid6n', 'bid5n', 'bid4n',
                  'bid3n', 'bid2n', 'bid1n',
                  'ask1n', 'ask2n', 'ask3n', 'ask4n', 'ask5n', 'ask6n', 'ask7n', 'ask8n', 'ask9n', 'ask10n', 'bid1Top1q',
                  'bid1Top2q', 'bid1Top3q', 'bid1Top4q', 'bid1Top5q', 'bid1Top6q',
                  'bid1Top7q', 'bid1Top8q', 'bid1Top9q', 'bid1Top10q', 'bid1Top11q', 'bid1Top12q', 'bid1Top13q',
                  'bid1Top14q', 'bid1Top15q', 'bid1Top16q', 'bid1Top17q', 'bid1Top18q',
                  'bid1Top19q', 'bid1Top20q', 'bid1Top21q', 'bid1Top22q', 'bid1Top23q', 'bid1Top24q', 'bid1Top25q',
                  'bid1Top26q', 'bid1Top27q', 'bid1Top28q', 'bid1Top29q',
                  'bid1Top30q', 'bid1Top31q', 'bid1Top32q', 'bid1Top33q', 'bid1Top34q', 'bid1Top35q', 'bid1Top36q',
                  'bid1Top37q', 'bid1Top38q', 'bid1Top39q', 'bid1Top40q',
                  'bid1Top41q', 'bid1Top42q', 'bid1Top43q', 'bid1Top44q', 'bid1Top45q', 'bid1Top46q', 'bid1Top47q',
                  'bid1Top48q', 'bid1Top49q', 'bid1Top50q', 'ask1Top1q',
                  'ask1Top2q', 'ask1Top3q', 'ask1Top4q', 'ask1Top5q', 'ask1Top6q', 'ask1Top7q', 'ask1Top8q', 'ask1Top9q',
                  'ask1Top10q', 'ask1Top11q', 'ask1Top12q', 'ask1Top13q',
                  'ask1Top14q', 'ask1Top15q', 'ask1Top16q', 'ask1Top17q', 'ask1Top18q', 'ask1Top19q', 'ask1Top20q',
                  'ask1Top21q', 'ask1Top22q', 'ask1Top23q',
                  'ask1Top24q', 'ask1Top25q', 'ask1Top26q', 'ask1Top27q', 'ask1Top28q', 'ask1Top29q', 'ask1Top30q',
                  'ask1Top31q', 'ask1Top32q', 'ask1Top33q',
                  'ask1Top34q', 'ask1Top35q', 'ask1Top36q', 'ask1Top37q', 'ask1Top38q', 'ask1Top39q', 'ask1Top40q',
                  'ask1Top41q', 'ask1Top42q', 'ask1Top43q',
                  'ask1Top44q', 'ask1Top45q', 'ask1Top46q', 'ask1Top47q', 'ask1Top48q', 'ask1Top49q', 'ask1Top50q',
                  "total_bid_quantity", "total_ask_quantity",
                  "total_bid_vwap", "total_ask_vwap", "total_bid_orders", "total_ask_orders", "total_bid_levels", "total_ask_levels",
                  "bid_trade_max_duration", "ask_trade_max_duration",
                  "cum_canceled_buy_orders", "cum_canceled_buy_volume", "cum_canceled_buy_amount", "cum_canceled_sell_orders",
                  "cum_canceled_sell_volume", "cum_canceled_sell_amount"]
logSH['date'] = int(y)
logSH['time1'] = int(y) * 1000000000 + logSH['time']
logSH["time"] = logSH['time'].astype('int64') * 1000
logSH["clockAtArrival"] = logSH["time1"].astype(str).apply(
    lambda x: np.int64(datetime.datetime.strptime(x, '%Y%m%d%H%M%S%f').timestamp() * 1e6))
logSH.drop("time1", axis=1, inplace=True)
logSH['datetime'] = logSH["clockAtArrival"].apply(lambda x: datetime.datetime.fromtimestamp(x / 1e6))
logSH = logSH.fillna(0)
logSH["ordering"] = logSH.groupby("skey").cumcount() + 1

logSH["has_missing"] = 0
for col in ["skey", "date", "cum_trades_cnt", "total_bid_orders",
            'total_ask_orders', 'total_bid_levels', 'total_ask_levels', 'cum_canceled_buy_orders',
            'cum_canceled_sell_orders',
            "ordering", 'bid_trade_max_duration', 'ask_trade_max_duration', 'has_missing', 'bid10n', 'bid9n', 'bid8n', 'bid7n', 'bid6n', 'bid5n', 'bid4n',
                  'bid3n', 'bid2n', 'bid1n','ask1n', 'ask2n', 'ask3n', 'ask4n', 'ask5n', 'ask6n', 'ask7n', 'ask8n', 'ask9n', 'ask10n',  'bid1Top1q',
                  'bid1Top2q', 'bid1Top3q', 'bid1Top4q', 'bid1Top5q', 'bid1Top6q',
                  'bid1Top7q', 'bid1Top8q', 'bid1Top9q', 'bid1Top10q', 'bid1Top11q', 'bid1Top12q', 'bid1Top13q',
                  'bid1Top14q', 'bid1Top15q', 'bid1Top16q', 'bid1Top17q', 'bid1Top18q',
                  'bid1Top19q', 'bid1Top20q', 'bid1Top21q', 'bid1Top22q', 'bid1Top23q', 'bid1Top24q', 'bid1Top25q',
                  'bid1Top26q', 'bid1Top27q', 'bid1Top28q', 'bid1Top29q',
                  'bid1Top30q', 'bid1Top31q', 'bid1Top32q', 'bid1Top33q', 'bid1Top34q', 'bid1Top35q', 'bid1Top36q',
                  'bid1Top37q', 'bid1Top38q', 'bid1Top39q', 'bid1Top40q',
                  'bid1Top41q', 'bid1Top42q', 'bid1Top43q', 'bid1Top44q', 'bid1Top45q', 'bid1Top46q', 'bid1Top47q',
                  'bid1Top48q', 'bid1Top49q', 'bid1Top50q', 'ask1Top1q',
                  'ask1Top2q', 'ask1Top3q', 'ask1Top4q', 'ask1Top5q', 'ask1Top6q', 'ask1Top7q', 'ask1Top8q', 'ask1Top9q',
                  'ask1Top10q', 'ask1Top11q', 'ask1Top12q', 'ask1Top13q',
                  'ask1Top14q', 'ask1Top15q', 'ask1Top16q', 'ask1Top17q', 'ask1Top18q', 'ask1Top19q', 'ask1Top20q',
                  'ask1Top21q', 'ask1Top22q', 'ask1Top23q',
                  'ask1Top24q', 'ask1Top25q', 'ask1Top26q', 'ask1Top27q', 'ask1Top28q', 'ask1Top29q', 'ask1Top30q',
                  'ask1Top31q', 'ask1Top32q', 'ask1Top33q',
                  'ask1Top34q', 'ask1Top35q', 'ask1Top36q', 'ask1Top37q', 'ask1Top38q', 'ask1Top39q', 'ask1Top40q',
                  'ask1Top41q', 'ask1Top42q', 'ask1Top43q',
                  'ask1Top44q', 'ask1Top45q', 'ask1Top46q', 'ask1Top47q', 'ask1Top48q', 'ask1Top49q', 'ask1Top50q']:
    logSH[col] = logSH[col].astype('int32')

for cols in ["cum_amount", "prev_close","high", "low", "close", 'open','bid10p','bid9p','bid8p','bid7p','bid6p','bid5p','bid4p','bid3p','bid2p','bid1p',
                      'ask1p','ask2p','ask3p','ask4p','ask5p','ask6p','ask7p','ask8p','ask9p','ask10p', "cum_canceled_buy_amount", "cum_canceled_sell_amount"]:
    logSH[cols] = (logSH[cols]/10000).round(2)

for cols in ['total_bid_vwap', "total_ask_vwap"]:
    logSH[cols] = (logSH[cols]/10000).round(3)

assert (sum(logSH[logSH["open"] != 0].groupby("skey")["open"].nunique() != 1) == 0)
assert (sum(logSH[logSH["prev_close"] != 0].groupby("skey")["prev_close"].nunique() != 1) == 0)
logSH["prev_close"] = np.where(logSH["time"] >= 91500000000, logSH.groupby("skey")["prev_close"].transform("max"),
                            logSH["prev_close"])
logSH["open"] = np.where(logSH["cum_volume"] > 0, logSH.groupby("skey")["open"].transform("max"), logSH["open"])
assert (sum(logSH[logSH["open"] != 0].groupby("skey")["open"].nunique() != 1) == 0)
assert (sum(logSH[logSH["prev_close"] != 0].groupby("skey")["prev_close"].nunique() != 1) == 0)
assert (logSH[logSH["cum_volume"] > 0]["open"].min() > 0)

logSH = logSH[
    ["skey", "date", "time", "clockAtArrival", "datetime", "ordering", "has_missing", "cum_trades_cnt", "cum_volume",
      "cum_amount", "prev_close",
      "open", "high", "low", "close", 'bid10p', 'bid9p', 'bid8p', 'bid7p', 'bid6p', 'bid5p', 'bid4p', 'bid3p', 'bid2p',
      'bid1p',
      'ask1p', 'ask2p', 'ask3p', 'ask4p', 'ask5p', 'ask6p', 'ask7p', 'ask8p', 'ask9p', 'ask10p', 'bid10q', 'bid9q',
      'bid8q',
      'bid7q', 'bid6q', 'bid5q', 'bid4q', 'bid3q', 'bid2q', 'bid1q', 'ask1q', 'ask2q', 'ask3q', 'ask4q', 'ask5q',
      'ask6q',
      'ask7q', 'ask8q', 'ask9q', 'ask10q', 'bid10n', 'bid9n', 'bid8n', 'bid7n', 'bid6n', 'bid5n', 'bid4n', 'bid3n',
      'bid2n', 'bid1n',
      'ask1n', 'ask2n', 'ask3n', 'ask4n', 'ask5n', 'ask6n', 'ask7n', 'ask8n', 'ask9n', 'ask10n', 'bid1Top1q',
      'bid1Top2q', 'bid1Top3q', 'bid1Top4q', 'bid1Top5q', 'bid1Top6q',
      'bid1Top7q', 'bid1Top8q', 'bid1Top9q', 'bid1Top10q', 'bid1Top11q', 'bid1Top12q', 'bid1Top13q', 'bid1Top14q',
      'bid1Top15q', 'bid1Top16q', 'bid1Top17q', 'bid1Top18q',
      'bid1Top19q', 'bid1Top20q', 'bid1Top21q', 'bid1Top22q', 'bid1Top23q', 'bid1Top24q', 'bid1Top25q', 'bid1Top26q',
      'bid1Top27q', 'bid1Top28q', 'bid1Top29q',
      'bid1Top30q', 'bid1Top31q', 'bid1Top32q', 'bid1Top33q', 'bid1Top34q', 'bid1Top35q', 'bid1Top36q', 'bid1Top37q',
      'bid1Top38q', 'bid1Top39q', 'bid1Top40q',
      'bid1Top41q', 'bid1Top42q', 'bid1Top43q', 'bid1Top44q', 'bid1Top45q', 'bid1Top46q', 'bid1Top47q', 'bid1Top48q',
      'bid1Top49q', 'bid1Top50q', 'ask1Top1q',
      'ask1Top2q', 'ask1Top3q', 'ask1Top4q', 'ask1Top5q', 'ask1Top6q', 'ask1Top7q', 'ask1Top8q', 'ask1Top9q',
      'ask1Top10q', 'ask1Top11q', 'ask1Top12q', 'ask1Top13q',
      'ask1Top14q', 'ask1Top15q', 'ask1Top16q', 'ask1Top17q', 'ask1Top18q', 'ask1Top19q', 'ask1Top20q', 'ask1Top21q',
      'ask1Top22q', 'ask1Top23q',
      'ask1Top24q', 'ask1Top25q', 'ask1Top26q', 'ask1Top27q', 'ask1Top28q', 'ask1Top29q', 'ask1Top30q', 'ask1Top31q',
      'ask1Top32q', 'ask1Top33q',
      'ask1Top34q', 'ask1Top35q', 'ask1Top36q', 'ask1Top37q', 'ask1Top38q', 'ask1Top39q', 'ask1Top40q', 'ask1Top41q',
      'ask1Top42q', 'ask1Top43q',
      'ask1Top44q', 'ask1Top45q', 'ask1Top46q', 'ask1Top47q', 'ask1Top48q', 'ask1Top49q', 'ask1Top50q',
      "total_bid_quantity", "total_ask_quantity", "total_bid_vwap", "total_ask_vwap",
      "total_bid_orders", 'total_ask_orders', 'total_bid_levels', 'total_ask_levels', 'bid_trade_max_duration',
      'ask_trade_max_duration', 'cum_canceled_buy_orders', 'cum_canceled_buy_volume',
      "cum_canceled_buy_amount", "cum_canceled_sell_orders", 'cum_canceled_sell_volume', "cum_canceled_sell_amount"]]

print(logSH["date"].iloc[0])
print("SH finished")

database_name = 'com_md_eq_cn'
user = "zhenyuy"
password = "bnONBrzSMGoE"

db1 = DB("192.168.10.178", database_name, user, password)
db1.write('md_snapshot_l2', logSH)
del logSH

print(datetime.datetime.now() - startTm)





readPath = '/mnt/dailyRawData/' + y + '/logs_' + y + '_zt_88_03_day_pcap/mdIndexPcap_SH_***'
dataPathLs = np.array(glob.glob(readPath))
startTm = datetime.datetime.now()
SH = pd.read_csv(dataPathLs[0])
print(datetime.datetime.now() - startTm)

startTm = datetime.datetime.now()
SH = SH.rename(columns={"ID":"skey"})
in_dex = [1000016, 1000300, 1000852, 1000905]
SH = SH[SH['skey'].isin(in_dex)]

for cols in ["cum_amount", "close", "open"]:
    SH[cols] = (SH[cols]/10000).round(4)

SH['t'] = SH['time']
SH.loc[SH['t']%1000 != 0, 't'] = SH.loc[SH['t']%1000 != 0, 't'] + 1000
SH['t'] = (SH['t'] // 1000) * 1000
SH['t1'] = SH['time']
SH['time'] = SH['t']

SH['date'] = int(y)
SH['time1'] = int(y) * 1000000000 + SH['time']
SH['time'] = SH['time'].astype('int64') * 1000
SH["clockAtArrival"] = SH["time1"].astype(str).apply(
    lambda x: np.int64(datetime.datetime.strptime(x, '%Y%m%d%H%M%S%f').timestamp() * 1e6))
SH.drop("time1", axis=1, inplace=True)
SH['datetime'] = SH["clockAtArrival"].apply(lambda x: datetime.datetime.fromtimestamp(x / 1e6))

SH = SH.fillna(0)
SH = SH.drop_duplicates(['cum_volume', 'open', 'close', 'cum_amount', 'skey', 
              'date', 'time', 'clockAtArrival', 'datetime'])
assert(SH[SH.duplicated(['skey', 'time'], keep=False)].drop_duplicates(['skey', 't1'], keep=False).shape[0] == 0)
assert(sum(SH['time']%1000000) == 0)
assert(sum(SH[SH['cum_volume'] == 0].groupby('skey')['time'].max() 
           <= SH[SH['cum_volume'] > 0].groupby('skey')['time'].min()))
assert(SH['time'].max() < 150500000000)
SH = SH[(SH['cum_volume'] > 0) & (SH['time'] <= 150500000000)]


k1 = SH.groupby('skey')['datetime'].min().reset_index()
k1 = k1.rename(columns={'datetime':'min'})
k2 = SH.groupby('skey')['datetime'].max().reset_index()
k2 = k2.rename(columns={'datetime':'max'})
k = pd.merge(k1, k2, on='skey')
k['diff'] = (k['max']-k['min']).apply(lambda x: x.seconds)
df = pd.DataFrame()
for i in np.arange(k.shape[0]):
    df1 = pd.DataFrame()
    df1['datetime1'] = [k.loc[i, 'min'] + datetime.timedelta(seconds=int(x)) for x in np.arange(0, k.loc[i, 'diff'] + 1)]
    df1['skey'] = k.loc[i, 'skey']
    assert(df1['datetime1'].min() == k.loc[i, 'min'])
    assert(df1['datetime1'].max() == k.loc[i, 'max'])
    df = pd.concat([df, df1])

SH = pd.merge(SH, df, left_on=['skey', 'datetime'], right_on=['skey', 'datetime1'], how='outer').sort_values(by=['skey', 'datetime1']).reset_index(drop=True)
assert(SH[SH['datetime1'].isnull()].shape[0] == 0)
for cols in ['date', 'cum_volume', 'cum_amount', 'open', 'close']:
    SH[cols] = SH.groupby('skey')[cols].ffill()
SH.drop(["datetime"],axis=1,inplace=True)
SH = SH.rename(columns={'datetime1':'datetime'})
SH['date'] = SH['date'].iloc[0]
SH['date'] = SH['date'].astype('int32')
SH['skey'] = SH['skey'].astype('int32')
SH["time"] = SH['datetime'].astype(str).apply(lambda x: int(x.split(' ')[1].replace(':', ""))).astype(np.int64)
SH['SendingTime'] = SH['date'] * 1000000 + SH['time']
SH["clockAtArrival"] = SH["SendingTime"].astype(str).apply(lambda x: np.int64(datetime.datetime.strptime(x, '%Y%m%d%H%M%S').timestamp()*1e6))
SH.drop(["SendingTime"],axis=1,inplace=True)
SH['time'] = SH['time'] * 1000000

assert(sum(SH[SH["open"] != 0].groupby("skey")["open"].nunique() != 1) == 0)
SH["open"] = np.where(SH["cum_volume"] > 0, SH.groupby("skey")["open"].transform("max"), SH["open"])
assert(sum(SH[SH["open"] != 0].groupby("skey")["open"].nunique() != 1) == 0)
assert(SH[SH["cum_volume"] > 0]["open"].min() > 0)

for cols in ['open', 'close', 'cum_amount']:
    SH[cols] = SH[cols].apply(lambda x: round(x, 4)).astype('float64')
m_in = SH[SH['time'] <= 113500000000].groupby('skey').last()['time'].min()
m_ax = SH[SH['time'] >= 125500000000].groupby('skey').first()['time'].max()
assert((SH[(SH['time'] >= m_in) & (SH['time'] <= m_ax)].drop_duplicates(['cum_volume', 'open', 
                                               'close', 'cum_amount', 'skey', 'date'], keep=False).shape[0] == 0)
          & (sum(SH[(SH['time'] >= m_in) & (SH['time'] <= m_ax)].groupby('skey')['cum_volume'].nunique() != 1) == 0) & 
           (sum(SH[(SH['time'] >= m_in) & (SH['time'] <= m_ax)].groupby('skey')['close'].nunique() != 1) == 0))
SH = pd.concat([SH[SH['time'] <= 113500000000], SH[SH['time'] >= 125500000000]])
    
SH = SH.sort_values(by=['skey', 'time', 'cum_volume'])
SH["ordering"] = SH.groupby("skey").cumcount()
SH["ordering"] = SH["ordering"] + 1
SH['ordering'] = SH['ordering'].astype('int32')
SH['cum_volume'] = SH['cum_volume'].astype('int64')
SH['close'] = np.where(SH['cum_volume'] > 0, SH['close'], 0)

SH = SH[["skey", "date", "time", "clockAtArrival", "datetime", "ordering", "cum_volume", "cum_amount", 
         "open", "close"]]
        
print(SH["date"].iloc[0])
print("index finished")

database_name = 'com_md_eq_cn'
user = "zhenyuy"
password = "bnONBrzSMGoE"

db1 = DB("192.168.10.178", database_name, user, password)
db1.write('md_index', SH)

del SH
print(datetime.datetime.now() - startTm)










readPath = '/mnt/dailyRawData/' + y + '/logs_' + y + '_zs_96_03_day_pcap/mdL2Pcap_SZ_***'
dataPathLs = np.array(glob.glob(readPath))

startTm = datetime.datetime.now()
SZ = pd.read_csv(dataPathLs[0])
print(datetime.datetime.now() - startTm)

startTm = datetime.datetime.now()
SZ = SZ.rename(columns={'ID': 'skey'})
SZ = SZ[["skey", "time", "cum_volume", "cum_amount", "cum_tradesCnt", "prevClose",
                 "open", "high", "low", "close", 'bid10p', 'bid9p', 'bid8p', 'bid7p', 'bid6p', 'bid5p', 'bid4p',
                 'bid3p', 'bid2p', 'bid1p',
                 'ask1p', 'ask2p', 'ask3p', 'ask4p', 'ask5p', 'ask6p', 'ask7p', 'ask8p', 'ask9p', 'ask10p', 'bid10q',
                 'bid9q', 'bid8q',
                 'bid7q', 'bid6q', 'bid5q', 'bid4q', 'bid3q', 'bid2q', 'bid1q', 'ask1q', 'ask2q', 'ask3q', 'ask4q',
                 'ask5q', 'ask6q',
                 'ask7q', 'ask8q', 'ask9q', 'ask10q', 'bid10n', 'bid9n', 'bid8n', 'bid7n', 'bid6n', 'bid5n', 'bid4n',
                 'bid3n', 'bid2n', 'bid1n',
                 'ask1n', 'ask2n', 'ask3n', 'ask4n', 'ask5n', 'ask6n', 'ask7n', 'ask8n', 'ask9n', 'ask10n', 'bid1Top1q',
                 'bid1Top2q', 'bid1Top3q', 'bid1Top4q', 'bid1Top5q', 'bid1Top6q',
                 'bid1Top7q', 'bid1Top8q', 'bid1Top9q', 'bid1Top10q', 'bid1Top11q', 'bid1Top12q', 'bid1Top13q',
                 'bid1Top14q', 'bid1Top15q', 'bid1Top16q', 'bid1Top17q', 'bid1Top18q',
                 'bid1Top19q', 'bid1Top20q', 'bid1Top21q', 'bid1Top22q', 'bid1Top23q', 'bid1Top24q', 'bid1Top25q',
                 'bid1Top26q', 'bid1Top27q', 'bid1Top28q', 'bid1Top29q',
                 'bid1Top30q', 'bid1Top31q', 'bid1Top32q', 'bid1Top33q', 'bid1Top34q', 'bid1Top35q', 'bid1Top36q',
                 'bid1Top37q', 'bid1Top38q', 'bid1Top39q', 'bid1Top40q',
                 'bid1Top41q', 'bid1Top42q', 'bid1Top43q', 'bid1Top44q', 'bid1Top45q', 'bid1Top46q', 'bid1Top47q',
                 'bid1Top48q', 'bid1Top49q', 'bid1Top50q', 'ask1Top1q',
                 'ask1Top2q', 'ask1Top3q', 'ask1Top4q', 'ask1Top5q', 'ask1Top6q', 'ask1Top7q', 'ask1Top8q', 'ask1Top9q',
                 'ask1Top10q', 'ask1Top11q', 'ask1Top12q', 'ask1Top13q',
                 'ask1Top14q', 'ask1Top15q', 'ask1Top16q', 'ask1Top17q', 'ask1Top18q', 'ask1Top19q', 'ask1Top20q',
                 'ask1Top21q', 'ask1Top22q', 'ask1Top23q',
                 'ask1Top24q', 'ask1Top25q', 'ask1Top26q', 'ask1Top27q', 'ask1Top28q', 'ask1Top29q', 'ask1Top30q',
                 'ask1Top31q', 'ask1Top32q', 'ask1Top33q',
                 'ask1Top34q', 'ask1Top35q', 'ask1Top36q', 'ask1Top37q', 'ask1Top38q', 'ask1Top39q', 'ask1Top40q',
                 'ask1Top41q', 'ask1Top42q', 'ask1Top43q',
                 'ask1Top44q', 'ask1Top45q', 'ask1Top46q', 'ask1Top47q', 'ask1Top48q', 'ask1Top49q', 'ask1Top50q',
                 "totalBidQuantity", "totalAskQuantity", "vwapBid", "vwapAsk"]]
SZ.columns = ["skey", "time", "cum_volume", "cum_amount", "cum_trades_cnt", "prev_close",
                 "open", "high", "low", "close", 'bid10p', 'bid9p', 'bid8p', 'bid7p', 'bid6p', 'bid5p', 'bid4p',
                 'bid3p', 'bid2p', 'bid1p',
                 'ask1p', 'ask2p', 'ask3p', 'ask4p', 'ask5p', 'ask6p', 'ask7p', 'ask8p', 'ask9p', 'ask10p', 'bid10q',
                 'bid9q', 'bid8q',
                 'bid7q', 'bid6q', 'bid5q', 'bid4q', 'bid3q', 'bid2q', 'bid1q', 'ask1q', 'ask2q', 'ask3q', 'ask4q',
                 'ask5q', 'ask6q',
                 'ask7q', 'ask8q', 'ask9q', 'ask10q', 'bid10n', 'bid9n', 'bid8n', 'bid7n', 'bid6n', 'bid5n', 'bid4n',
                 'bid3n', 'bid2n', 'bid1n',
                 'ask1n', 'ask2n', 'ask3n', 'ask4n', 'ask5n', 'ask6n', 'ask7n', 'ask8n', 'ask9n', 'ask10n', 'bid1Top1q',
                 'bid1Top2q', 'bid1Top3q', 'bid1Top4q', 'bid1Top5q', 'bid1Top6q',
                 'bid1Top7q', 'bid1Top8q', 'bid1Top9q', 'bid1Top10q', 'bid1Top11q', 'bid1Top12q', 'bid1Top13q',
                 'bid1Top14q', 'bid1Top15q', 'bid1Top16q', 'bid1Top17q', 'bid1Top18q',
                 'bid1Top19q', 'bid1Top20q', 'bid1Top21q', 'bid1Top22q', 'bid1Top23q', 'bid1Top24q', 'bid1Top25q',
                 'bid1Top26q', 'bid1Top27q', 'bid1Top28q', 'bid1Top29q',
                 'bid1Top30q', 'bid1Top31q', 'bid1Top32q', 'bid1Top33q', 'bid1Top34q', 'bid1Top35q', 'bid1Top36q',
                 'bid1Top37q', 'bid1Top38q', 'bid1Top39q', 'bid1Top40q',
                 'bid1Top41q', 'bid1Top42q', 'bid1Top43q', 'bid1Top44q', 'bid1Top45q', 'bid1Top46q', 'bid1Top47q',
                 'bid1Top48q', 'bid1Top49q', 'bid1Top50q', 'ask1Top1q',
                 'ask1Top2q', 'ask1Top3q', 'ask1Top4q', 'ask1Top5q', 'ask1Top6q', 'ask1Top7q', 'ask1Top8q', 'ask1Top9q',
                 'ask1Top10q', 'ask1Top11q', 'ask1Top12q', 'ask1Top13q',
                 'ask1Top14q', 'ask1Top15q', 'ask1Top16q', 'ask1Top17q', 'ask1Top18q', 'ask1Top19q', 'ask1Top20q',
                 'ask1Top21q', 'ask1Top22q', 'ask1Top23q',
                 'ask1Top24q', 'ask1Top25q', 'ask1Top26q', 'ask1Top27q', 'ask1Top28q', 'ask1Top29q', 'ask1Top30q',
                 'ask1Top31q', 'ask1Top32q', 'ask1Top33q',
                 'ask1Top34q', 'ask1Top35q', 'ask1Top36q', 'ask1Top37q', 'ask1Top38q', 'ask1Top39q', 'ask1Top40q',
                 'ask1Top41q', 'ask1Top42q', 'ask1Top43q',
                 'ask1Top44q', 'ask1Top45q', 'ask1Top46q', 'ask1Top47q', 'ask1Top48q', 'ask1Top49q', 'ask1Top50q',
                 "total_bid_quantity", "total_ask_quantity", "total_bid_vwap", "total_ask_vwap"]
SZ['date'] = int(y)
SZ['time1'] = int(y) * 1000000000 + SZ['time']
SZ["time"] = SZ['time'].astype('int64') * 1000
SZ["clockAtArrival"] = SZ["time1"].astype(str).apply(
    lambda x: np.int64(datetime.datetime.strptime(x, '%Y%m%d%H%M%S%f').timestamp() * 1e6))
SZ.drop("time1", axis=1, inplace=True)
SZ['datetime'] = SZ["clockAtArrival"].apply(lambda x: datetime.datetime.fromtimestamp(x / 1e6))
SZ = SZ.fillna(0)
SZ["ordering"] = SZ.groupby("skey").cumcount() + 1

for cols in ["total_bid_orders",'total_ask_orders','total_bid_levels', 'total_ask_levels', 'bid_trade_max_duration',
             'ask_trade_max_duration', 'cum_canceled_buy_orders', 'cum_canceled_buy_volume', "cum_canceled_buy_amount",
             "cum_canceled_sell_orders", 'cum_canceled_sell_volume',"cum_canceled_sell_amount", "has_missing"]:
    SZ[cols] = 0

for col in ["skey", "date", "cum_trades_cnt", "total_bid_orders",
    'total_ask_orders', 'total_bid_levels', 'total_ask_levels', 'cum_canceled_buy_orders','cum_canceled_sell_orders',
        "ordering", 'bid_trade_max_duration', 'ask_trade_max_duration','has_missing', 'bid10n', 'bid9n', 'bid8n', 'bid7n', 'bid6n', 'bid5n', 'bid4n',
                 'bid3n', 'bid2n', 'bid1n','ask1n', 'ask2n', 'ask3n', 'ask4n', 'ask5n', 'ask6n', 'ask7n', 'ask8n', 'ask9n', 'ask10n',  'bid1Top1q',
                 'bid1Top2q', 'bid1Top3q', 'bid1Top4q', 'bid1Top5q', 'bid1Top6q',
                 'bid1Top7q', 'bid1Top8q', 'bid1Top9q', 'bid1Top10q', 'bid1Top11q', 'bid1Top12q', 'bid1Top13q',
                 'bid1Top14q', 'bid1Top15q', 'bid1Top16q', 'bid1Top17q', 'bid1Top18q',
                 'bid1Top19q', 'bid1Top20q', 'bid1Top21q', 'bid1Top22q', 'bid1Top23q', 'bid1Top24q', 'bid1Top25q',
                 'bid1Top26q', 'bid1Top27q', 'bid1Top28q', 'bid1Top29q',
                 'bid1Top30q', 'bid1Top31q', 'bid1Top32q', 'bid1Top33q', 'bid1Top34q', 'bid1Top35q', 'bid1Top36q',
                 'bid1Top37q', 'bid1Top38q', 'bid1Top39q', 'bid1Top40q',
                 'bid1Top41q', 'bid1Top42q', 'bid1Top43q', 'bid1Top44q', 'bid1Top45q', 'bid1Top46q', 'bid1Top47q',
                 'bid1Top48q', 'bid1Top49q', 'bid1Top50q', 'ask1Top1q',
                 'ask1Top2q', 'ask1Top3q', 'ask1Top4q', 'ask1Top5q', 'ask1Top6q', 'ask1Top7q', 'ask1Top8q', 'ask1Top9q',
                 'ask1Top10q', 'ask1Top11q', 'ask1Top12q', 'ask1Top13q',
                 'ask1Top14q', 'ask1Top15q', 'ask1Top16q', 'ask1Top17q', 'ask1Top18q', 'ask1Top19q', 'ask1Top20q',
                 'ask1Top21q', 'ask1Top22q', 'ask1Top23q',
                 'ask1Top24q', 'ask1Top25q', 'ask1Top26q', 'ask1Top27q', 'ask1Top28q', 'ask1Top29q', 'ask1Top30q',
                 'ask1Top31q', 'ask1Top32q', 'ask1Top33q',
                 'ask1Top34q', 'ask1Top35q', 'ask1Top36q', 'ask1Top37q', 'ask1Top38q', 'ask1Top39q', 'ask1Top40q',
                 'ask1Top41q', 'ask1Top42q', 'ask1Top43q',
                 'ask1Top44q', 'ask1Top45q', 'ask1Top46q', 'ask1Top47q', 'ask1Top48q', 'ask1Top49q', 'ask1Top50q']:
    SZ[col] = SZ[col].astype('int32')

for col in ["cum_amount", "prev_close","high", "low", "close", 'open','bid10p','bid9p','bid8p','bid7p','bid6p','bid5p','bid4p','bid3p','bid2p','bid1p',
                      'ask1p','ask2p','ask3p','ask4p','ask5p','ask6p','ask7p','ask8p','ask9p','ask10p',
                      "total_bid_vwap", "total_ask_vwap"]:
    SZ[col] = (SZ[col]/10000).round(2)
    
for cols in ["cum_canceled_sell_amount", "cum_canceled_buy_amount"]:
    SZ[cols] = SZ[cols].astype('float64')

    
assert(sum(SZ[SZ["open"] != 0].groupby("skey")["open"].nunique() != 1) == 0)
assert(sum(SZ[SZ["prev_close"] != 0].groupby("skey")["prev_close"].nunique() != 1) == 0)
SZ["prev_close"] = np.where(SZ["time"] >= 91500000000, SZ.groupby("skey")["prev_close"].transform("max"), SZ["prev_close"]) 
SZ["open"] = np.where(SZ["cum_volume"] > 0, SZ.groupby("skey")["open"].transform("max"), SZ["open"])
assert(sum(SZ[SZ["open"] != 0].groupby("skey")["open"].nunique() != 1) == 0)
assert(sum(SZ[SZ["prev_close"] != 0].groupby("skey")["prev_close"].nunique() != 1) == 0)
assert(SZ[SZ["cum_volume"] > 0]["open"].min() > 0)


startTm = datetime.datetime.now()
SZ = SZ[["skey", "date", "time", "clockAtArrival", "datetime", "ordering", "has_missing", "cum_trades_cnt", "cum_volume", "cum_amount", "prev_close",
                        "open", "high", "low", "close", 'bid10p','bid9p','bid8p','bid7p','bid6p','bid5p','bid4p','bid3p','bid2p','bid1p',
                        'ask1p','ask2p','ask3p','ask4p','ask5p','ask6p','ask7p','ask8p','ask9p','ask10p', 'bid10q','bid9q','bid8q',
                         'bid7q','bid6q','bid5q','bid4q','bid3q','bid2q','bid1q', 'ask1q','ask2q','ask3q','ask4q','ask5q','ask6q',
                         'ask7q','ask8q','ask9q','ask10q', 'bid10n', 'bid9n', 'bid8n', 'bid7n', 'bid6n', 'bid5n', 'bid4n', 'bid3n', 'bid2n', 'bid1n', 
                         'ask1n', 'ask2n', 'ask3n', 'ask4n', 'ask5n', 'ask6n','ask7n', 'ask8n', 'ask9n', 'ask10n','bid1Top1q','bid1Top2q','bid1Top3q','bid1Top4q','bid1Top5q','bid1Top6q',
    'bid1Top7q','bid1Top8q','bid1Top9q','bid1Top10q','bid1Top11q','bid1Top12q','bid1Top13q','bid1Top14q','bid1Top15q','bid1Top16q','bid1Top17q','bid1Top18q',
    'bid1Top19q','bid1Top20q','bid1Top21q','bid1Top22q','bid1Top23q','bid1Top24q','bid1Top25q','bid1Top26q','bid1Top27q','bid1Top28q','bid1Top29q',
    'bid1Top30q','bid1Top31q','bid1Top32q','bid1Top33q','bid1Top34q','bid1Top35q','bid1Top36q','bid1Top37q','bid1Top38q','bid1Top39q','bid1Top40q',
    'bid1Top41q','bid1Top42q','bid1Top43q','bid1Top44q','bid1Top45q','bid1Top46q','bid1Top47q','bid1Top48q','bid1Top49q','bid1Top50q', 'ask1Top1q',
    'ask1Top2q','ask1Top3q','ask1Top4q','ask1Top5q','ask1Top6q','ask1Top7q','ask1Top8q','ask1Top9q','ask1Top10q','ask1Top11q','ask1Top12q','ask1Top13q',
    'ask1Top14q','ask1Top15q','ask1Top16q','ask1Top17q','ask1Top18q','ask1Top19q','ask1Top20q','ask1Top21q','ask1Top22q','ask1Top23q',
    'ask1Top24q','ask1Top25q','ask1Top26q','ask1Top27q','ask1Top28q','ask1Top29q','ask1Top30q','ask1Top31q','ask1Top32q','ask1Top33q',
    'ask1Top34q','ask1Top35q','ask1Top36q','ask1Top37q','ask1Top38q','ask1Top39q','ask1Top40q','ask1Top41q','ask1Top42q','ask1Top43q',
    'ask1Top44q','ask1Top45q','ask1Top46q','ask1Top47q','ask1Top48q','ask1Top49q','ask1Top50q',"total_bid_quantity", "total_ask_quantity","total_bid_vwap", "total_ask_vwap",
    "total_bid_orders",'total_ask_orders','total_bid_levels', 'total_ask_levels', 'bid_trade_max_duration', 'ask_trade_max_duration', 'cum_canceled_buy_orders', 'cum_canceled_buy_volume',
    "cum_canceled_buy_amount", "cum_canceled_sell_orders", 'cum_canceled_sell_volume',"cum_canceled_sell_amount"]]

print(SZ["date"].iloc[0])
print("SZ finished")


database_name = 'com_md_eq_cn'
user = "zhenyuy"
password = "bnONBrzSMGoE"

db1 = DB("192.168.10.178", database_name, user, password)
db1.write('md_snapshot_l2', SZ)

del SZ
print(datetime.datetime.now() - startTm)










readPath = '/mnt/dailyRawData/' + y + '/logs_' + y + '_zs_96_03_day_pcap/mdOrderPcap_SZ_***'
dataPathLs = np.array(glob.glob(readPath))

startTm = datetime.datetime.now()
OrderLogSZ = pd.read_csv(dataPathLs[0])
print(datetime.datetime.now() - startTm)

startTm = datetime.datetime.now()
OrderLogSZ = OrderLogSZ.rename(columns={"ID":"skey", "time": 'TransactTime'})
OrderLogSZ["OrderType"] = np.where(OrderLogSZ["OrderType"] == 'U', 3, OrderLogSZ["OrderType"])
OrderLogSZ = OrderLogSZ[(OrderLogSZ['skey'] < 2004000) | ((OrderLogSZ['skey'] > 2300000)
                                                                & (OrderLogSZ['skey'] < 2310000))]
OrderLogSZ['date'] = int(y)
OrderLogSZ['time1'] = int(y) * 1000000000 + OrderLogSZ['TransactTime']
OrderLogSZ["TransactTime"] = OrderLogSZ['TransactTime'].astype('int64') * 1000
OrderLogSZ["clockAtArrival"] = OrderLogSZ["time1"].astype(str).apply(
    lambda x: np.int64(datetime.datetime.strptime(x, '%Y%m%d%H%M%S%f').timestamp() * 1e6))
OrderLogSZ.drop("time1", axis=1, inplace=True)
OrderLogSZ['datetime'] = OrderLogSZ["clockAtArrival"].apply(lambda x: datetime.datetime.fromtimestamp(x / 1e6))
OrderLogSZ = OrderLogSZ.rename(columns={"TransactTime":"time"})

for col in ["skey", "date", "ApplSeqNum", "OrderQty", "Side", "OrderType"]:
    OrderLogSZ[col] = OrderLogSZ[col].astype('int32')
for cols in ["Price"]:
    OrderLogSZ[cols] = (OrderLogSZ[cols]/10000).round(2)

assert(OrderLogSZ[((OrderLogSZ["Side"] != 1) & (OrderLogSZ["Side"] != 2)) | (OrderLogSZ["OrderType"].isnull())].shape[0] == 0)

OrderLogSZ = OrderLogSZ.rename(columns={"Side":"order_side", "OrderType":"order_type", "Price":"order_price", "OrderQty":"order_qty"})
OrderLogSZ = OrderLogSZ[["skey", "date", "time", "clockAtArrival", "datetime", "ApplSeqNum", "order_side", "order_type", "order_price",
                                             "order_qty"]]

print(OrderLogSZ["date"].iloc[0])
print("order finished")

database_name = 'com_md_eq_cn'
user = "zhenyuy"
password = "bnONBrzSMGoE"

db1 = DB("192.168.10.178", database_name, user, password)
db1.write('md_order', OrderLogSZ)

del OrderLogSZ

print(datetime.datetime.now() - startTm)










readPath = '/mnt/dailyRawData/' + y + '/logs_' + y + '_zt_88_03_day_pcap/mdTradePcap_SH_***'
dataPathLs = np.array(glob.glob(readPath))

startTm = datetime.datetime.now()
SH = pd.read_csv(dataPathLs[0])
print(datetime.datetime.now() - startTm)

startTm = datetime.datetime.now()
SH = SH.rename(columns={"time": 'TransactTime', "ID":"skey"})
SH = SH[(SH['skey'] >= 1600000) & (SH['skey'] <= 1700000)]
SH['date'] = int(y)
SH['time1'] = int(y) * 1000000000 + SH['TransactTime']
SH["TransactTime"] = SH['TransactTime'].astype('int64') * 1000
SH["clockAtArrival"] = SH["time1"].astype(str).apply(
    lambda x: np.int64(datetime.datetime.strptime(x, '%Y%m%d%H%M%S%f').timestamp() * 1e6))
SH.drop("time1", axis=1, inplace=True)
SH['datetime'] = SH["clockAtArrival"].apply(lambda x: datetime.datetime.fromtimestamp(x / 1e6))
SH['TradePrice'] = (SH['TradePrice'] / 10000).round(2)
SH = SH.rename(columns={"TradeQty":"trade_qty", "TradePrice":"trade_price", 
                                    "TradeBSFlag":"trade_flag", "TradeAmount":"trade_money",
                                   "TradeIndex":"ApplSeqNum", "SellNo":"OfferApplSeqNum",
                                   "BuyNo":"BidApplSeqNum", 'TransactTime':"time"})
SH["trade_type"] = 1
SH["trade_flag"] = np.where(SH["trade_flag"] == 'B', 1, np.where(
    SH["trade_flag"] == 'S', 2, 0))
for col in ["skey", "date", "ApplSeqNum", "BidApplSeqNum", "OfferApplSeqNum", "trade_qty", "trade_type", "trade_flag"]:
    SH[col] = SH[col].astype('int32')
 
SH = SH[["skey", "date", "time", "clockAtArrival", "datetime", "ApplSeqNum", "trade_type", "trade_flag",
                                             "trade_price", "trade_qty", "BidApplSeqNum", "OfferApplSeqNum"]]
print(SH['date'].iloc[0])
print("trade finished")

database_name = 'com_md_eq_cn'
user = "zhenyuy"
password = "bnONBrzSMGoE"

db1 = DB("192.168.10.178", database_name, user, password)
db1.write('md_trade', SH)

del SH

print(datetime.datetime.now() - startTm)














readPath = '/mnt/dailyRawData/' + y + '/logs_' + y + '_zs_96_03_day_pcap/mdTradePcap_SZ_***'
dataPathLs = np.array(glob.glob(readPath))

startTm = datetime.datetime.now()
TradeLogSZ = pd.read_csv(dataPathLs[0])
print(datetime.datetime.now() - startTm)

startTm = datetime.datetime.now()
TradeLogSZ = TradeLogSZ.rename(columns={"time": 'TransactTime', "ID":'skey'})
TradeLogSZ["TradeBSFlag"] = 'N'
TradeLogSZ = TradeLogSZ[(TradeLogSZ['skey'] < 2004000) | ((TradeLogSZ['skey'] > 2300000)
                                                             & (TradeLogSZ['skey'] < 2310000))]
TradeLogSZ['date'] = int(y)
TradeLogSZ['time1'] = int(y) * 1000000000 + TradeLogSZ['TransactTime']
TradeLogSZ["TransactTime"] = TradeLogSZ['TransactTime'].astype('int64') * 1000
TradeLogSZ["clockAtArrival"] = TradeLogSZ["time1"].astype(str).apply(
    lambda x: np.int64(datetime.datetime.strptime(x, '%Y%m%d%H%M%S%f').timestamp() * 1e6))
TradeLogSZ.drop("time1", axis=1, inplace=True)
TradeLogSZ['datetime'] = TradeLogSZ["clockAtArrival"].apply(lambda x: datetime.datetime.fromtimestamp(x / 1e6))
TradeLogSZ['TradePrice'] = (TradeLogSZ['TradePrice'] / 10000).round(2)
TradeLogSZ = TradeLogSZ.rename(columns={"TradeQty":"trade_qty", "TradePrice":"trade_price", "ExecType":"trade_type", 'TransactTime':'time'})
TradeLogSZ["trade_flag"] = 0
TradeLogSZ["trade_type"] = np.where(TradeLogSZ["trade_type"] == 'F', 1, TradeLogSZ["trade_type"])
for col in ["skey", "date", "ApplSeqNum", "BidApplSeqNum", "OfferApplSeqNum", "trade_qty", "trade_type", "trade_flag"]:
    TradeLogSZ[col] = TradeLogSZ[col].astype('int32')
 
TradeLogSZ = TradeLogSZ[["skey", "date", "time", "clockAtArrival", "datetime", "ApplSeqNum", "trade_type", "trade_flag",
                                             "trade_price", "trade_qty", "BidApplSeqNum", "OfferApplSeqNum"]]
print(TradeLogSZ['date'].iloc[0])
print("trade finished")


database_name = 'com_md_eq_cn'
user = "zhenyuy"
password = "bnONBrzSMGoE"

db1 = DB("192.168.10.178", database_name, user, password)
db1.write('md_trade', TradeLogSZ)    

del TradeLogSZ

print(datetime.datetime.now() - startTm)
