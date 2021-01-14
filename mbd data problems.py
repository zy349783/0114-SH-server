#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Aug 27 16:36:21 2020

@author: work516
"""

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


class go():
    def __init__(self, thisDate_str, orders_data, trades_data):
        self.orders_data = orders_data
        self.trades_data = trades_data
        self.thisDate_str = thisDate_str

    def run(self, s):
        mdTradeLog = self.trades_data[s]
        mdOrderLog = self.orders_data[s]
        ###
        mdOrderLog['ID'] = int(mdOrderLog['skey'].dropna().unique())
        mdOrderLog['order_type'] = mdOrderLog['order_type'].astype(str)
        mdOrderLog['status'] = 'order'
        ## rename
        mdOrderLog.columns = ['skey', 'date', 'TransactTime', 'clockAtArrival', 'datetime', 'ApplSeqNum',
                              'Side', 'OrderType', 'Price', 'OrderQty', 'SecurityID', 'status']
        mdTradeLog['ID'] = int(mdTradeLog['skey'].dropna().unique())
        mdTradeLog['trade_type'] = mdTradeLog['trade_type'].astype(str)
        if 'trade_money' not in mdTradeLog.columns:
            mdTradeLog.columns = ['skey', 'date', 'TransactTime', 'clockAtArrival', 'datetime', 'ApplSeqNum',
                                  'ExecType', 'trade_flag', 'TradePrice', 'TradeQty', 'BidApplSeqNum',
                                  'OfferApplSeqNum', 'SecurityID']
        else:
            mdTradeLog.columns = ['skey', 'date', 'TransactTime', 'clockAtArrival', 'datetime', 'ApplSeqNum',
                                  'ExecType', 'trade_flag', 'TradePrice', 'TradeQty', 'BidApplSeqNum',
                                  'OfferApplSeqNum', 'SecurityID', 'trade_money']
            ###
        tradedLog = mdTradeLog[mdTradeLog['ExecType'] == '1'].reset_index(drop=True)
        tradedLog['status'] = 'trade'
        #
        bidOrderInfo = mdOrderLog[['ApplSeqNum', 'SecurityID', 'Price', 'OrderType', 'Side']].reset_index(drop=True)
        bidOrderInfo = bidOrderInfo.rename(
            columns={'TransactTime': 'TransactTime', 'ApplSeqNum': 'BidApplSeqNum', 'Price': 'BidOrderPrice',
                     'OrderType': 'BidOrderType', 'Side': 'BidSide'})
        tradedLog = pd.merge(tradedLog, bidOrderInfo, how='left', on=['SecurityID', 'BidApplSeqNum'],
                             validate='many_to_one')
        del bidOrderInfo

        askOrderInfo = mdOrderLog[['ApplSeqNum', 'SecurityID', 'Price', 'OrderType', 'Side']].reset_index(drop=True)
        askOrderInfo = askOrderInfo.rename(
            columns={'TransactTime': 'TransactTime', 'ApplSeqNum': 'OfferApplSeqNum', 'Price': 'OfferOrderPrice',
                     'OrderType': 'OfferOrderType', 'Side': 'OfferSide'})
        tradedLog = pd.merge(tradedLog, askOrderInfo, how='left', on=['SecurityID', 'OfferApplSeqNum'],
                             validate='many_to_one')
        del askOrderInfo

        cancelLog = mdTradeLog[mdTradeLog['ExecType'] == '4'].reset_index(drop=True)
        cancelLog['status'] = 'cancel'
        cancelLog['CancelApplSeqNum'] = cancelLog['BidApplSeqNum']
        mask = cancelLog['CancelApplSeqNum'] == 0
        cancelLog.loc[mask, 'CancelApplSeqNum'] = cancelLog.loc[mask, 'OfferApplSeqNum'].values
        del mask
        assert (cancelLog[cancelLog['CancelApplSeqNum'] == 0].shape[0] == 0)
        cancelLog = cancelLog.drop(columns=['TradePrice'])

        cancelPrice = mdOrderLog[['ApplSeqNum', 'SecurityID', 'Price', 'OrderType', 'Side']].reset_index(drop=True)
        cancelPrice = cancelPrice.rename(columns={'ApplSeqNum': 'CancelApplSeqNum', 'Price': 'TradePrice',
                                                  'OrderType': 'CancelOrderType', 'Side': 'CancelSide'})
        cancelLog = pd.merge(cancelLog, cancelPrice, how='left', on=['SecurityID', 'CancelApplSeqNum'],
                             validate='one_to_one')
        del cancelPrice

        msgData = pd.concat([mdOrderLog[['clockAtArrival', 'TransactTime', 'ApplSeqNum', 'SecurityID',
                                         'status', 'Side', 'OrderType', 'Price', 'OrderQty']],
                             tradedLog[['clockAtArrival', 'TransactTime', 'ApplSeqNum', 'SecurityID',
                                        'status', 'ExecType', 'TradePrice', 'TradeQty', 'BidApplSeqNum',
                                        'OfferApplSeqNum', 'BidOrderType', 'BidSide', 'OfferOrderType', 'OfferSide',
                                        'BidOrderPrice', 'OfferOrderPrice']]], sort=False)
        msgData = pd.concat([msgData, cancelLog[['clockAtArrival', 'TransactTime', 'ApplSeqNum',
                                                 'SecurityID', 'status', 'ExecType', 'TradePrice', 'TradeQty',
                                                 'CancelApplSeqNum',
                                                 'CancelOrderType', 'CancelSide']]], sort=False)
        del tradedLog
        del cancelLog
        msgData = msgData.sort_values(by=['ApplSeqNum']).reset_index(drop=True)
        for stockID, stockMsg in msgData.groupby(['SecurityID']):
            stockMsg = stockMsg.reset_index(drop=True)
            stockMsg['TransactTime'] = stockMsg['TransactTime'] / 1000
            stockMsg['isAuction'] = np.where(stockMsg['TransactTime'] < 92900000, True, False)
            stockMsg = stockMsg[stockMsg['TransactTime'] < 145655000].reset_index(drop=True)
            stockMsgNP = stockMsg.to_records()
            simMarket = SimMktSnapshotAllNew(exchange='SZ', stockID=stockID, levels=30)
        #             self.simMarket = simMarket
        try:
            for rowEntry in stockMsgNP:
                if rowEntry.isAuction:
                    if rowEntry.status == 'order':
                        simMarket.insertAuctionOrder(rowEntry.clockAtArrival, rowEntry.TransactTime,
                                                     rowEntry.ApplSeqNum, rowEntry.Side, rowEntry.Price,
                                                     rowEntry.OrderQty)
                    elif rowEntry.status == 'cancel':
                        simMarket.removeOrderByAuctionCancel(rowEntry.clockAtArrival, rowEntry.TransactTime,
                                                             rowEntry.ApplSeqNum, rowEntry.TradePrice,
                                                             rowEntry.TradeQty,
                                                             rowEntry.CancelApplSeqNum, rowEntry.CancelOrderType,
                                                             rowEntry.CancelSide)
                    elif rowEntry.status == 'trade':
                        simMarket.removeOrderByAuctionTrade(rowEntry.clockAtArrival, rowEntry.TransactTime,
                                                            rowEntry.ApplSeqNum, rowEntry.TradePrice, rowEntry.TradeQty,
                                                            rowEntry.BidOrderPrice, rowEntry.OfferOrderPrice)
                else:
                    if rowEntry.status == 'order':
                        simMarket.insertOrder(rowEntry.clockAtArrival, rowEntry.TransactTime, rowEntry.ApplSeqNum,
                                              rowEntry.Side, rowEntry.OrderType, rowEntry.Price, rowEntry.OrderQty,
                                              rowEntry.ApplSeqNum)
                    elif rowEntry.status == 'cancel':
                        simMarket.removeOrderByCancel(rowEntry.clockAtArrival, rowEntry.TransactTime,
                                                      rowEntry.ApplSeqNum, rowEntry.TradePrice, rowEntry.TradeQty,
                                                      rowEntry.CancelApplSeqNum, rowEntry.CancelOrderType,
                                                      rowEntry.CancelSide)
                    elif rowEntry.status == 'trade':
                        simMarket.removeOrderByTrade(rowEntry.clockAtArrival, rowEntry.TransactTime,
                                                     rowEntry.ApplSeqNum, rowEntry.TradePrice, rowEntry.TradeQty,
                                                     rowEntry.BidApplSeqNum,
                                                     rowEntry.OfferApplSeqNum)
            self.af = simMarket.getAllInfo()
            database_name = 'com_md_eq_cn'
            user = "zhenyuy"
            password = "bnONBrzSMGoE"
            db = DB("192.168.10.178", database_name, user, password)
            data = self.af
            data = data.rename(columns={'StockID': "skey"})
            data = data.rename(columns={'sequenceNo': "ApplSeqNum"})
            data['date'] = int(thisDate_str)
            data['datetime'] = data["clockAtArrival"].apply(lambda x: datetime.datetime.fromtimestamp(x / 1e6))
            for cols in ['bid30p', 'bid29p',
                         'bid28p', 'bid27p', 'bid26p', 'bid25p', 'bid24p', 'bid23p', 'bid22p', 'bid21p', 'bid20p',
                         'bid19p',
                         'bid18p', 'bid17p', 'bid16p', 'bid15p', 'bid14p', 'bid13p', 'bid12p', 'bid11p',
                         'bid10p', 'bid9p', 'bid8p', 'bid7p', 'bid6p', 'bid5p', 'bid4p', 'bid3p',
                         'bid2p', 'bid1p', 'ask1p', 'ask2p', 'ask3p', 'ask4p', 'ask5p', 'ask6p', 'ask7p', 'ask8p',
                         'ask9p', 'ask10p',
                         'ask11p', 'ask12p', 'ask13p', 'ask14p', 'ask15p', 'ask16p', 'ask17p',
                         'ask18p', 'ask19p', 'ask20p', 'ask21p', 'ask22p', 'ask23p', 'ask24p',
                         'ask25p', 'ask26p', 'ask27p', 'ask28p', 'ask29p', 'ask30p']:
                data[cols] = data[cols].astype(float)
            for cols in ['ApplSeqNum', 'date']:
                data[cols] = data[cols].astype('int32')
            data = data[['skey', 'date', 'time', 'clockAtArrival', 'datetime', 'ApplSeqNum', 'cum_volume', 'cum_amount',
                         'close', 'bid30p', 'bid29p',
                         'bid28p', 'bid27p', 'bid26p', 'bid25p', 'bid24p', 'bid23p', 'bid22p', 'bid21p', 'bid20p',
                         'bid19p',
                         'bid18p', 'bid17p', 'bid16p', 'bid15p', 'bid14p', 'bid13p', 'bid12p', 'bid11p',
                         'bid10p', 'bid9p', 'bid8p', 'bid7p', 'bid6p', 'bid5p', 'bid4p', 'bid3p',
                         'bid2p', 'bid1p', 'ask1p', 'ask2p', 'ask3p', 'ask4p', 'ask5p', 'ask6p', 'ask7p', 'ask8p',
                         'ask9p', 'ask10p',
                         'ask11p', 'ask12p', 'ask13p', 'ask14p', 'ask15p', 'ask16p', 'ask17p',
                         'ask18p', 'ask19p', 'ask20p', 'ask21p', 'ask22p', 'ask23p', 'ask24p',
                         'ask25p', 'ask26p', 'ask27p', 'ask28p', 'ask29p', 'ask30p', 'bid30q',
                         'bid29q', 'bid28q', 'bid27q', 'bid26q', 'bid25q', 'bid24q', 'bid23q',
                         'bid22q', 'bid21q', 'bid20q', 'bid19q', 'bid18q', 'bid17q', 'bid16q', 'bid15q', 'bid14q',
                         'bid13q', 'bid12q', 'bid11q',
                         'bid10q', 'bid9q', 'bid8q', 'bid7q', 'bid6q', 'bid5q', 'bid4q', 'bid3q',
                         'bid2q', 'bid1q', 'ask1q', 'ask2q', 'ask3q', 'ask4q', 'ask5q', 'ask6q',
                         'ask7q', 'ask8q', 'ask9q', 'ask10q', 'ask11q', 'ask12q', 'ask13q',
                         'ask14q', 'ask15q', 'ask16q', 'ask17q', 'ask18q', 'ask19q', 'ask20q',
                         'ask21q', 'ask22q', 'ask23q', 'ask24q', 'ask25q', 'ask26q', 'ask27q', 'ask28q', 'ask29q',
                         'ask30q',
                         'bid30n', 'bid29n', 'bid28n', 'bid27n', 'bid26n', 'bid25n', 'bid24n',
                         'bid23n', 'bid22n', 'bid21n', 'bid20n', 'bid19n', 'bid18n', 'bid17n',
                         'bid16n', 'bid15n', 'bid14n', 'bid13n', 'bid12n', 'bid11n', 'bid10n',
                         'bid9n', 'bid8n', 'bid7n', 'bid6n', 'bid5n', 'bid4n', 'bid3n', 'bid2n',
                         'bid1n', 'ask1n', 'ask2n', 'ask3n', 'ask4n', 'ask5n', 'ask6n', 'ask7n', 'ask8n', 'ask9n',
                         'ask10n',
                         'ask11n', 'ask12n', 'ask13n', 'ask14n', 'ask15n', 'ask16n', 'ask17n',
                         'ask18n', 'ask19n', 'ask20n', 'ask21n', 'ask22n', 'ask23n', 'ask24n',
                         'ask25n', 'ask26n', 'ask27n', 'ask28n', 'ask29n', 'ask30n',
                         'bid1Top1q', 'bid1Top2q', 'bid1Top3q', 'bid1Top4q', 'bid1Top5q', 'bid1Top6q',
                         'bid1Top7q', 'bid1Top8q', 'bid1Top9q', 'bid1Top10q', 'bid1Top11q', 'bid1Top12q', 'bid1Top13q',
                         'bid1Top14q', 'bid1Top15q', 'bid1Top16q', 'bid1Top17q', 'bid1Top18q',
                         'bid1Top19q', 'bid1Top20q', 'bid1Top21q', 'bid1Top22q', 'bid1Top23q', 'bid1Top24q',
                         'bid1Top25q', 'bid1Top26q', 'bid1Top27q', 'bid1Top28q', 'bid1Top29q',
                         'bid1Top30q', 'bid1Top31q', 'bid1Top32q', 'bid1Top33q', 'bid1Top34q', 'bid1Top35q',
                         'bid1Top36q', 'bid1Top37q', 'bid1Top38q', 'bid1Top39q', 'bid1Top40q',
                         'bid1Top41q', 'bid1Top42q', 'bid1Top43q', 'bid1Top44q', 'bid1Top45q', 'bid1Top46q',
                         'bid1Top47q', 'bid1Top48q', 'bid1Top49q', 'bid1Top50q', 'ask1Top1q',
                         'ask1Top2q', 'ask1Top3q', 'ask1Top4q', 'ask1Top5q', 'ask1Top6q', 'ask1Top7q', 'ask1Top8q',
                         'ask1Top9q', 'ask1Top10q', 'ask1Top11q', 'ask1Top12q', 'ask1Top13q',
                         'ask1Top14q', 'ask1Top15q', 'ask1Top16q', 'ask1Top17q', 'ask1Top18q', 'ask1Top19q',
                         'ask1Top20q', 'ask1Top21q', 'ask1Top22q', 'ask1Top23q',
                         'ask1Top24q', 'ask1Top25q', 'ask1Top26q', 'ask1Top27q', 'ask1Top28q', 'ask1Top29q',
                         'ask1Top30q', 'ask1Top31q', 'ask1Top32q', 'ask1Top33q',
                         'ask1Top34q', 'ask1Top35q', 'ask1Top36q', 'ask1Top37q', 'ask1Top38q', 'ask1Top39q',
                         'ask1Top40q', 'ask1Top41q', 'ask1Top42q', 'ask1Top43q',
                         'ask1Top44q', 'ask1Top45q', 'ask1Top46q', 'ask1Top47q', 'ask1Top48q', 'ask1Top49q',
                         'ask1Top50q', 'total_bid_quantity', 'total_ask_quantity',
                         'total_bid_vwap', 'total_ask_vwap', 'total_bid_orders', 'total_ask_orders', 'total_bid_levels',
                         'total_ask_levels']]
            db.write('md_snapshot_mbd', data)

        except Exception as e:
            print(s)
            print(e)


class SimMktSnapshotAllNew():

    def __init__(self, exchange, stockID, levels):

        self.errors = []
        self.exchange = exchange
        self.stockID = stockID
        self.levels = levels
        self.topK = 50

        self.bid = {}
        self.ask = {}
        self.allBidp = []
        self.allAskp = []
        self.bidp = []
        self.bidq = []
        self.askp = []
        self.askq = []
        self.bidn = []
        self.askn = []
        self.uOrder = {}
        self.takingOrder = {}
        self.tempOrder = {}
        self.hasTempOrder = False
        self.isAuction = True

        self.cur_cum_volume = 0
        self.cur_cum_amount = 0
        self.cur_close = 0
        self.bid1p = 0
        self.ask1p = 0
        self.cum_volume = []
        self.cum_amount = []
        self.close = []
        self.localTime = []
        self.exchangeTime = []
        self.sequenceNum = []
        self.bboImprove = []

        self.cum_aggressive_volume = []
        self.cum_aggressive_amount = []
        self.cum_market_volume = []
        self.cum_market_amount = []

        self.total_bid_qty = []
        self.total_bid_vwap = []
        self.total_bid_levels = []
        self.total_bid_orders_num = []
        self.total_ask_qty = []
        self.total_ask_vwap = []
        self.total_ask_levels = []
        self.total_ask_orders_num = []

        self.bidnq = defaultdict(OrderedDict)
        self.asknq = defaultdict(OrderedDict)
        self.bid1Topq = []
        self.ask1Topq = []

        self.bid_qty = 0
        self.ask_qty = 0
        self.bid_amount = 0
        self.ask_amount = 0
        self.bid_price_levels = 0
        self.ask_price_levels = 0
        self.bid_order_nums = 0
        self.ask_order_nums = 0

    def insertAuctionOrder(self, clockAtArrival, exchangeTime, seqNum, side, price, qty):

        if side == 1:
            if price in self.bid:
                self.bid[price] += qty
            else:
                self.bid[price] = qty
                ##**##
                self.bid_price_levels += 1
                ##**##
            ######
            self.bidnq[price][seqNum] = qty
            ######
            ##**##
            self.bid_qty += qty
            self.bid_amount += qty * price
            self.bid_order_nums += 1
            ##**##
        elif side == 2:
            if price in self.ask:
                self.ask[price] += qty
            else:
                self.ask[price] = qty
                ##**##
                self.ask_price_levels += 1
                ##**##
            ######
            self.asknq[price][seqNum] = qty
            ######
            ##**##
            self.ask_qty += qty
            self.ask_amount += qty * price
            self.ask_order_nums += 1
            ##**##
        self.localTime.append(clockAtArrival)
        self.exchangeTime.append(exchangeTime)
        self.sequenceNum.append(seqNum)

    def removeOrderByAuctionTrade(self, clockAtArrival, exchangeTime, seqNum,
                                  price, qty, bidOrderPrice, offerOrderPrice):
        if bidOrderPrice in self.bid:
            bidRemain = self.bid[bidOrderPrice] - qty
            if bidRemain == 0:
                self.bid.pop(bidOrderPrice)
                ##**##
                self.bid_price_levels -= 1
                ##**##
            elif bidRemain > 0:
                self.bid[bidOrderPrice] = bidRemain
            ######
            cum_vol = 0
            for seqNo in self.bidnq[bidOrderPrice]:
                cum_vol += self.bidnq[bidOrderPrice][seqNo]
                if cum_vol > qty:
                    ##**##
                    useful_qty = (self.bidnq[bidOrderPrice][seqNo] - (cum_vol - qty))
                    ##**##
                    self.bidnq[bidOrderPrice][seqNo] = cum_vol - qty
                    ##**##
                    self.bid_qty -= useful_qty
                    self.bid_amount -= useful_qty * bidOrderPrice
                    ##**##
                    break
                elif cum_vol == qty:
                    ##**##
                    useful_qty = self.bidnq[bidOrderPrice][seqNo]
                    ##**##
                    self.bidnq[bidOrderPrice].pop(seqNo)
                    ##**##
                    self.bid_qty -= useful_qty
                    self.bid_amount -= useful_qty * bidOrderPrice
                    self.bid_order_nums -= 1
                    ##**##
                    break
                else:
                    ##**##
                    useful_qty = self.bidnq[bidOrderPrice][seqNo]
                    ##**##
                    self.bidnq[bidOrderPrice].pop(seqNo)
                    ##**##
                    self.bid_qty -= useful_qty
                    self.bid_amount -= useful_qty * bidOrderPrice
                    self.bid_order_nums -= 1
                    ##**##
            ######
        else:
            print('bid price not in bid')

        if offerOrderPrice in self.ask:
            askRemain = self.ask[offerOrderPrice] - qty
            if askRemain == 0:
                self.ask.pop(offerOrderPrice)
                ##**##
                self.ask_price_levels -= 1
                ##**##
            elif askRemain > 0:
                self.ask[offerOrderPrice] = askRemain
            ######
            cum_vol = 0
            for seqNo in self.asknq[offerOrderPrice]:
                cum_vol += self.asknq[offerOrderPrice][seqNo]
                if cum_vol > qty:
                    ##**##
                    useful_qty = (self.asknq[offerOrderPrice][seqNo] - (cum_vol - qty))
                    ##**##
                    self.asknq[offerOrderPrice][seqNo] = cum_vol - qty
                    ##**##
                    self.ask_qty -= useful_qty
                    self.ask_amount -= useful_qty * offerOrderPrice
                    ##**##
                    break
                elif cum_vol == qty:
                    ##**##
                    useful_qty = self.asknq[offerOrderPrice][seqNo]
                    ##**##
                    self.asknq[offerOrderPrice].pop(seqNo)
                    ##**##
                    self.ask_qty -= useful_qty
                    self.ask_amount -= useful_qty * offerOrderPrice
                    self.ask_order_nums -= 1
                    ##**##
                    break
                else:
                    ##**##
                    useful_qty = self.asknq[offerOrderPrice][seqNo]
                    ##**##
                    self.asknq[offerOrderPrice].pop(seqNo)
                    ##**##
                    self.ask_qty -= useful_qty
                    self.ask_amount -= useful_qty * offerOrderPrice
                    self.ask_order_nums -= 1
                    ##**##
            ######
        else:
            print('ask price not in ask')

        self.cur_cum_volume += qty
        self.cur_cum_amount += price * qty
        self.cur_close = price

        self.localTime.append(clockAtArrival)
        self.exchangeTime.append(exchangeTime)
        self.sequenceNum.append(seqNum)

    def removeOrderByAuctionCancel(self, clockAtArrival, exchangeTime, seqNum,
                                   cancelPrice, cancelQty, cancelApplSeqNum, cancelOrderType, cancelSide):
        ######
        if cancelApplSeqNum in self.asknq[cancelPrice]:
            self.asknq[cancelPrice][cancelApplSeqNum] -= cancelQty
            if self.asknq[cancelPrice][cancelApplSeqNum] == 0:
                self.asknq[cancelPrice].pop(cancelApplSeqNum)
        else:
            self.bidnq[cancelPrice][cancelApplSeqNum] -= cancelQty
            if self.bidnq[cancelPrice][cancelApplSeqNum] == 0:
                self.bidnq[cancelPrice].pop(cancelApplSeqNum)
                ######
        if cancelApplSeqNum in self.uOrder:
            cancelPrice, cancelSide = self.uOrder[cancelApplSeqNum]
            assert (cancelPrice > 0)
            self.uOrder.pop(cancelApplSeqNum)

        if cancelSide == 1:
            remain = self.bid[cancelPrice] - cancelQty
            if remain == 0:
                self.bid.pop(cancelPrice)
                ##**##
                self.bid_price_levels -= 1
                ##**##
            elif remain > 0:
                self.bid[cancelPrice] = remain
            ##**##
            self.bid_qty -= cancelQty
            self.bid_amount -= cancelQty * cancelPrice
            self.bid_order_nums -= 1
            ##**##

        elif cancelSide == 2:
            remain = self.ask[cancelPrice] - cancelQty
            if remain == 0:
                self.ask.pop(cancelPrice)
                ##**##
                self.ask_price_levels -= 1
                ##**##
            elif remain > 0:
                self.ask[cancelPrice] = remain
            ##**##
            self.ask_qty -= cancelQty
            self.ask_amount -= cancelQty * cancelPrice
            self.ask_order_nums -= 1
            ##**##
        self.localTime.append(clockAtArrival)
        self.exchangeTime.append(exchangeTime)
        self.sequenceNum.append(seqNum)

    def insertOrder(self, clockAtArrival, exchangeTime, seqNum, side, orderType, price, qty, applySeqNum):
        if self.isAuction:
            auctionClockAtArrival = self.localTime[-1]
            auctionExchangeTime = self.exchangeTime[-1]
            auctionSeqNum = self.sequenceNum[-1]
            self.localTime = []
            self.exchangeTime = []
            self.sequenceNum = []
            self.bboImprove = []
            self.updateMktInfo(auctionClockAtArrival, auctionExchangeTime, auctionSeqNum, record=True)
            self.isAuction = False

        hasConvert = False
        if self.hasTempOrder:
            tempSeqNum = list(self.tempOrder.keys())[0]
            tempOrderType, tempSide, tempPrice, tempQty, tempStatus = self.tempOrder[tempSeqNum]
            if tempOrderType == '1':
                hasConvert = True
            self.tempToLimit(clockAtArrival, exchangeTime, tempSeqNum)
            self.hasTempOrder = False

        if orderType == '2':
            if side == 1 and price < self.ask1p:
                if price in self.bid:
                    self.bid[price] += qty
                else:
                    self.bid[price] = qty
                    ##**##
                    self.bid_price_levels += 1
                    ##**##
                self.bidnq[price][applySeqNum] = qty
                ##**##
                self.bid_qty += qty
                self.bid_amount += qty * price
                self.bid_order_nums += 1
                ##**##
                if hasConvert:
                    self.updateMktInfo(clockAtArrival, exchangeTime, seqNum, record=True)
                else:
                    self.updateMktInfo(clockAtArrival, exchangeTime, seqNum, record=True)
            elif side == 2 and price > self.bid1p:
                if price in self.ask:
                    self.ask[price] += qty
                else:
                    self.ask[price] = qty
                    ##**##
                    self.ask_price_levels += 1
                    ##**##
                self.asknq[price][applySeqNum] = qty
                ##**##
                self.ask_qty += qty
                self.ask_amount += qty * price
                self.ask_order_nums += 1
                ##**##
                if hasConvert:
                    self.updateMktInfo(clockAtArrival, exchangeTime, seqNum, record=True)
                else:
                    self.updateMktInfo(clockAtArrival, exchangeTime, seqNum, record=True)
            else:
                # *****
                self.tempOrder[applySeqNum] = (orderType, side, price, qty, 0)
                #                 self.tempOrder[applySeqNum] = ('1', side, price, qty, 0)
                # *****
                self.hasTempOrder = True
                self.guessingTrade(clockAtArrival, exchangeTime, seqNum)

        elif orderType == '1':
            if side == 1:
                self.tempOrder[applySeqNum] = (orderType, side, self.ask1p, qty, 0)
                self.takingOrder[applySeqNum] = (self.ask1p, side)
            else:
                self.tempOrder[applySeqNum] = (orderType, side, self.bid1p, qty, 0)
                self.takingOrder[applySeqNum] = (self.bid1p, side)
            self.hasTempOrder = True

        elif orderType == '3':
            if side == 1:
                if len(self.bid) != 0:
                    self.bid[self.bid1p] += qty
                    self.uOrder[applySeqNum] = (self.bid1p, side)
                    self.bidnq[self.bid1p][applySeqNum] = qty
                    ##**##
                    self.bid_qty += qty
                    self.bid_amount += qty * self.bid1p
                    self.bid_order_nums += 1
                    ##**##
                else:
                    self.tempOrder[applySeqNum] = (orderType, side, self.bid1p, qty, 0)
                    self.hasTempOrder = True
            else:
                if len(self.ask) != 0:
                    self.ask[self.ask1p] += qty
                    self.uOrder[applySeqNum] = (self.ask1p, side)
                    self.asknq[self.ask1p][applySeqNum] = qty
                    ##**##
                    self.ask_qty += qty
                    self.ask_amount += qty * self.ask1p
                    self.ask_order_nums += 1
                    ##**##
                else:
                    self.tempOrder[applySeqNum] = (orderType, side, self.ask1p, qty, 0)
                    self.hasTempOrder = True
            if hasConvert:
                self.updateMktInfo(clockAtArrival, exchangeTime, seqNum, record=True)
            else:
                self.updateMktInfo(clockAtArrival, exchangeTime, seqNum, record=True)

    def removeOrderByTrade(self, clockAtArrival, exchangeTime, seqNum, price, qty, bidApplSeqNum, offerApplSeqNum):

        assert (len(self.tempOrder) == 1)

        if bidApplSeqNum in self.tempOrder:
            tempSeqNum = bidApplSeqNum
            passiveSeqNum = offerApplSeqNum
        elif offerApplSeqNum in self.tempOrder:
            tempSeqNum = offerApplSeqNum
            passiveSeqNum = bidApplSeqNum
        else:
            print('Trade not happend in taking order', bidApplSeqNum, offerApplSeqNum)

        tempOrderType, tempSide, tempPrice, tempQty, tempStatus = self.tempOrder[tempSeqNum]
        tempRemain = tempQty - qty
        if tempRemain == 0:
            self.tempOrder.pop(tempSeqNum)
            self.hasTempOrder = False
        else:
            self.tempOrder[tempSeqNum] = (tempOrderType, tempSide, tempPrice, tempRemain, 1)

        if tempSide == 1:
            assert (self.ask1p == price)
            askRemain = self.ask[price] - qty
            if tempOrderType == '1':
                ##**##
                self.ask_qty -= qty
                self.ask_amount -= qty * price
                ##**##
            if askRemain == 0:
                self.ask.pop(price)
                if tempOrderType == '1':
                    ##**##
                    self.ask_price_levels -= 1
                    ##**##
            elif askRemain > 0:
                self.ask[price] = askRemain
            else:
                assert (askRemain > 0)
            if tempOrderType == '1':
                self.asknq[price][passiveSeqNum] -= qty
                if self.asknq[price][passiveSeqNum] == 0:
                    self.asknq[price].pop(passiveSeqNum)
                    ##**##
                    self.ask_order_nums -= 1
                    ##**##
        elif tempSide == 2:
            if self.bid1p != price:
                print(seqNum)
            assert (self.bid1p == price)
            bidRemain = self.bid[price] - qty
            if tempOrderType == '1':
                ##**##
                self.bid_qty -= qty
                self.bid_amount -= qty * price
                ##**##
            if bidRemain == 0:
                self.bid.pop(price)
                if tempOrderType == '1':
                    ##**##
                    self.bid_price_levels -= 1
                    ##**##
            elif bidRemain > 0:
                self.bid[price] = bidRemain
            else:
                assert (bidRemain > 0)
            if tempOrderType == '1':
                self.bidnq[price][passiveSeqNum] -= qty
                if self.bidnq[price][passiveSeqNum] == 0:
                    self.bidnq[price].pop(passiveSeqNum)
                    ##**##
                    self.bid_order_nums -= 1
                    ##**##
        self.cur_cum_volume += qty
        self.cur_cum_amount += price * qty
        self.cur_close = price

        if self.hasTempOrder == False and tempOrderType == '1':
            self.updateMktInfo(clockAtArrival, exchangeTime, seqNum, record=True)
        else:
            self.updateMktInfo(clockAtArrival, exchangeTime, seqNum, record=False)

    def removeOrderByCancel(self, clockAtArrival, exchangeTime, seqNum,
                            cancelPrice, cancelQty, cancelApplSeqNum, cancelOrderType, cancelSide):

        if self.isAuction:
            auctionClockAtArrival = self.localTime[-1]
            auctionExchangeTime = self.exchangeTime[-1]
            auctionSeqNum = self.sequenceNum[-1]
            self.localTime = []
            self.exchangeTime = []
            self.sequenceNum = []
            self.updateMktInfo(auctionClockAtArrival, auctionExchangeTime, auctionSeqNum, record=True)
            self.isAuction = False

        if cancelApplSeqNum in self.tempOrder:
            tempOrderType, tempSide, tempPrice, tempQty, tempStatus = self.tempOrder[cancelApplSeqNum]
            self.tempOrder.pop(cancelApplSeqNum)
            self.hasTempOrder = False

            if tempOrderType == '2':
                if cancelApplSeqNum in self.asknq[cancelPrice]:
                    self.asknq[cancelPrice][cancelApplSeqNum] -= cancelQty
                    ##**##
                    self.ask_qty -= cancelQty
                    self.ask_amount -= cancelQty * cancelPrice
                    self.ask_order_nums -= 1
                    ##**##
                    if self.asknq[cancelPrice][cancelApplSeqNum] == 0:
                        self.asknq[cancelPrice].pop(cancelApplSeqNum)
                        ##**##
                        self.ask_price_levels -= 1
                        ##**##
                else:
                    self.bidnq[cancelPrice][cancelApplSeqNum] -= cancelQty
                    ##**##
                    self.bid_qty -= cancelQty
                    self.bid_amount -= cancelQty * cancelPrice
                    self.bid_order_nums -= 1
                    ##**##
                    if self.bidnq[cancelPrice][cancelApplSeqNum] == 0:
                        self.bidnq[cancelPrice].pop(cancelApplSeqNum)
                        ##**##
                        self.bid_price_levels -= 1
                        ##**##
            if tempStatus == 1:
                self.updateMktInfo(clockAtArrival, exchangeTime, seqNum, record=True)
            else:
                self.updateMktInfo(clockAtArrival, exchangeTime, seqNum, record=False)

        else:
            hasConvert = False
            if self.hasTempOrder:
                tempSeqNum = list(self.tempOrder.keys())[0]
                tempOrderType, tempSide, tempPrice, tempQty, tempStatus = self.tempOrder[tempSeqNum]
                if tempOrderType == '1':
                    hasConvert = True
                self.tempToLimit(clockAtArrival, exchangeTime, seqNum)
                self.hasTempOrder = False

            if cancelOrderType == '3':
                cancelPrice, cancelSide = self.uOrder[cancelApplSeqNum]
                assert (cancelPrice > 0)
                self.uOrder.pop(cancelApplSeqNum)

            if cancelOrderType == '1':
                cancelPrice, cancelSide = self.takingOrder[cancelApplSeqNum]
                assert (cancelPrice > 0)

            if cancelSide == 1:
                remain = self.bid[cancelPrice] - cancelQty
                if remain == 0:
                    self.bid.pop(cancelPrice)
                    ##**##
                    self.bid_price_levels -= 1
                    ##**##
                elif remain > 0:
                    self.bid[cancelPrice] = remain
                ##**##
                self.bid_qty -= cancelQty
                self.bid_amount -= cancelQty * cancelPrice
                self.bid_order_nums -= 1
                ##**##

            elif cancelSide == 2:
                remain = self.ask[cancelPrice] - cancelQty
                if remain == 0:
                    self.ask.pop(cancelPrice)
                    ##**##
                    self.ask_price_levels -= 1
                    ##**##
                elif remain > 0:
                    self.ask[cancelPrice] = remain
                ##**##
                self.ask_qty -= cancelQty
                self.ask_amount -= cancelQty * cancelPrice
                self.ask_order_nums -= 1
                ##**##
            if cancelApplSeqNum in self.asknq[cancelPrice]:
                self.asknq[cancelPrice][cancelApplSeqNum] -= cancelQty
                if self.asknq[cancelPrice][cancelApplSeqNum] == 0:
                    self.asknq[cancelPrice].pop(cancelApplSeqNum)
            else:
                self.bidnq[cancelPrice][cancelApplSeqNum] -= cancelQty
                if self.bidnq[cancelPrice][cancelApplSeqNum] == 0:
                    self.bidnq[cancelPrice].pop(cancelApplSeqNum)

            if hasConvert:
                self.updateMktInfo(clockAtArrival, exchangeTime, seqNum, record=True)
            else:
                self.updateMktInfo(clockAtArrival, exchangeTime, seqNum, record=True)

    def guessingTrade(self, clockAtArrival, exchangeTime, seqNum):
        assert (len(self.tempOrder) == 1)
        key = list(self.tempOrder.keys())[0]
        orderType, orderSide, orderPrice, orderQty, tempStatus = self.tempOrder[key]
        fakeBid = self.bid.copy()
        fakeAsk = self.ask.copy()
        fakeVol = 0
        fakeAmount = 0
        fakeClose = 0
        if orderType == '1':
            print('orderType is 1')
            if orderSide == 1:
                curAskP = sorted(fakeAsk.keys())
                remain = orderQty
                for askP in curAskP:
                    if remain > 0:
                        askSize = fakeAsk[askP]
                        if askSize > remain:
                            fakeAsk[askP] = askSize - remain
                            ######
                            cum_vol = 0
                            for seqNo in self.asknq[askP]:
                                cum_vol += self.asknq[askP][seqNo]
                                if cum_vol > remain:
                                    self.asknq[askP][seqNo] = cum_vol - remain
                                    break
                                elif cum_vol == remain:
                                    self.asknq[askP].pop(seqNo)
                                    break
                                else:
                                    self.asknq[askP].pop(seqNo)
                            ######
                            fakeVol += remain
                            fakeAmount += remain * askP
                            remain = 0
                        else:
                            fakeAsk.pop(askP)
                            ######
                            for seqNo in self.asknq[askP]:
                                self.asknq[askP].pop(seqNo)
                            ######
                            fakeVol += askSize
                            fakeAmount += askSize * askP
                            remain -= askSize
                        fakeClose = askP

            elif orderSide == 2:
                curBidP = sorted(fakeBid.keys(), reverse=True)
                remain = orderQty
                for bidP in curBidP:
                    if remain > 0:
                        bidSize = fakeBid[bidP]
                        if bidSize > remain:
                            fakeBid[bidP] = bidSize - remain
                            ######
                            cum_vol = 0
                            for seqNo in self.bidnq[bidP]:
                                cum_vol += self.bidnq[bidP][seqNo]
                                if cum_vol > remain:
                                    self.bidnq[bidP][seqNo] = cum_vol - remain
                                    break
                                elif cum_vol == remain:
                                    self.bidnq[bidP].pop(seqNo)
                                    break
                                else:
                                    self.bidnq[bidP].pop(seqNo)
                            ######
                            fakeVol += remain
                            fakeAmount += remain * bidP
                            remain = 0
                        else:
                            fakeBid.pop(bidP)
                            ######
                            for seqNo in self.bidnq[bidP]:
                                self.asknq[bidP].pop(seqNo)
                            ######
                            fakeVol += bidSize
                            fakeAmount += bidSize * bidP
                            remain -= bidSize
                        fakeClose = bidP

        elif orderType == '2':
            if orderSide == 1:
                curAskP = sorted(fakeAsk.keys())
                remain = orderQty
                for askP in curAskP:
                    if remain > 0 and askP <= orderPrice:
                        askSize = fakeAsk[askP]
                        if askSize > remain:
                            fakeAsk[askP] = askSize - remain
                            ##**##
                            self.ask_qty -= remain
                            self.ask_amount -= remain * askP
                            ##**##
                            ######
                            cum_vol = 0
                            pop_list = []
                            for seqNo in self.asknq[askP]:
                                cum_vol += self.asknq[askP][seqNo]
                                if cum_vol > remain:
                                    self.asknq[askP][seqNo] = cum_vol - remain
                                    break
                                elif cum_vol == remain:
                                    pop_list.append(seqNo)
                                    break
                                else:
                                    pop_list.append(seqNo)
                            for seqNo in pop_list:
                                self.asknq[askP].pop(seqNo)
                                ##**##
                                self.ask_order_nums -= 1
                                ##**##
                            ######
                            fakeVol += remain
                            fakeAmount += remain * askP
                            remain = 0
                        else:
                            fakeAsk.pop(askP)
                            ##**##
                            self.ask_qty -= askSize
                            self.ask_amount -= askSize * askP
                            self.ask_price_levels -= 1
                            ##**##
                            ######
                            pop_list = list(self.asknq[askP].keys())
                            for seqNo in pop_list:
                                self.asknq[askP].pop(seqNo)
                                ##**##
                                self.ask_order_nums -= 1
                                ##**##
                            ######
                            fakeVol += askSize
                            fakeAmount += askSize * askP
                            remain -= askSize
                        fakeClose = askP
                if remain > 0:
                    fakeBid[orderPrice] = remain
                    ######
                    self.bidnq[orderPrice][seqNum] = remain
                    ######
                    ##**##
                    self.bid_qty += remain
                    self.bid_amount += remain * orderPrice
                    self.bid_order_nums += 1
                    self.bid_price_levels += 1
                    ##**##
            elif orderSide == 2:
                curBidP = sorted(fakeBid.keys(), reverse=True)
                remain = orderQty
                for bidP in curBidP:
                    if remain > 0 and bidP >= orderPrice:
                        bidSize = fakeBid[bidP]
                        if bidSize > remain:
                            fakeBid[bidP] = bidSize - remain
                            ##**##
                            self.bid_qty -= remain
                            self.bid_amount -= remain * bidP
                            ##**##
                            ######
                            cum_vol = 0
                            pop_list = []
                            for seqNo in self.bidnq[bidP]:
                                cum_vol += self.bidnq[bidP][seqNo]
                                if cum_vol > remain:
                                    self.bidnq[bidP][seqNo] = cum_vol - remain
                                    break
                                elif cum_vol == remain:
                                    pop_list.append(seqNo)
                                    break
                                else:
                                    pop_list.append(seqNo)
                            for seqNo in pop_list:
                                self.bidnq[bidP].pop(seqNo)
                                ##**##
                                self.bid_order_nums -= 1
                                ##**##
                            ######
                            fakeVol += remain
                            fakeAmount += remain * bidP
                            remain = 0
                        else:
                            fakeBid.pop(bidP)
                            ##**##
                            self.bid_qty -= bidSize
                            self.bid_amount -= bidSize * bidP
                            self.bid_price_levels -= 1
                            ##**##
                            ######
                            pop_list = list(self.bidnq[bidP].keys())
                            for seqNo in pop_list:
                                self.bidnq[bidP].pop(seqNo)
                                ##**##
                                self.bid_order_nums -= 1
                                ##**##
                            ######
                            fakeVol += bidSize
                            fakeAmount += bidSize * bidP
                            remain -= bidSize
                        fakeClose = bidP
                if remain > 0:
                    fakeAsk[orderPrice] = remain
                    ######
                    self.asknq[orderPrice][seqNum] = remain
                    ######
                    ##**##
                    self.ask_qty += remain
                    self.ask_amount += remain * orderPrice
                    self.ask_order_nums += 1
                    self.ask_price_levels += 1
                    ##**##
        self.localTime.append(clockAtArrival)
        self.exchangeTime.append(exchangeTime)
        self.sequenceNum.append(seqNum)
        self.bboImprove.append(1)

        curBidP = sorted(fakeBid.keys(), reverse=True)[:self.levels]
        curAskP = sorted(fakeAsk.keys())[:self.levels]
        curBidQ = [fakeBid[i] for i in curBidP]
        curBidN = [len(list(self.bidnq[i].keys())) for i in curBidP]

        self.bidp += [curBidP + [0] * (self.levels - len(curBidP))]
        self.bidq += [curBidQ + [0] * (self.levels - len(curBidQ))]
        self.bidn += [curBidN + [0] * (self.levels - len(curBidN))]

        curAskQ = [fakeAsk[i] for i in curAskP]
        curAskN = [len(list(self.asknq[i].keys())) for i in curAskP]
        self.askp += [curAskP + [0] * (self.levels - len(curAskP))]
        self.askq += [curAskQ + [0] * (self.levels - len(curAskQ))]
        self.askn += [curAskN + [0] * (self.levels - len(curAskN))]

        self.cum_volume.append(self.cur_cum_volume + fakeVol)
        self.cum_amount.append(self.cur_cum_amount + fakeAmount)
        self.close.append(fakeClose)

        ######
        if len(fakeAsk) != 0:
            ask1p = curAskP[0]
        else:
            ask1p = curBidP[0] + 0.01

        if len(fakeBid) != 0:
            bid1p = curBidP[0]
        else:
            bid1p = curAskP[0] - 0.01
        self.currMid = (bid1p + ask1p) / 2
        ######
        bid1pList = self.bidnq[self.bid1p].values()
        ask1pList = self.asknq[self.ask1p].values()
        bid_odrs, ask_odrs = calcTopK(bid1pList, ask1pList, self.topK)
        self.bid1Topq.append(bid_odrs + [0] * (self.topK - len(bid_odrs)))
        self.ask1Topq.append(ask_odrs + [0] * (self.topK - len(ask_odrs)))
        ######
        ####record these infos
        # &#
        self.calcVwapInfo()
        # &#

    def tempToLimit(self, clockAtArrival, exchangeTime, seqNum):
        assert (len(self.tempOrder) == 1)
        tempSeqNum = list(self.tempOrder.keys())[0]
        tempOrderType, tempSide, tempPrice, tempQty, tempStatus = self.tempOrder[tempSeqNum]
        if len(self.bid) != 0 and len(self.ask) != 0:
            assert (tempPrice < self.ask1p)
            assert (tempPrice > self.bid1p)
        if tempSide == 1:
            self.bid[tempPrice] = tempQty
            ######
            self.bidnq[tempPrice][tempSeqNum] = tempQty
            ######
            if tempOrderType == '1':
                ##**##
                self.bid_price_levels += 1
                self.bid_qty += tempQty
                self.bid_amount += tempQty * tempPrice
                self.bid_order_nums += 1
                ##**##
        elif tempSide == 2:
            self.ask[tempPrice] = tempQty
            ######
            self.asknq[tempPrice][tempSeqNum] = tempQty
            ######
            if tempOrderType == '1':
                ##**##
                self.ask_price_levels += 1
                self.ask_qty += tempQty
                self.ask_amount += tempQty * tempPrice
                self.ask_order_nums += 1
                ##**##
        self.tempOrder = {}
        self.hasTempOrder = False
        self.updateMktInfo(clockAtArrival, exchangeTime, seqNum, record=False)

    def updateMktInfo(self, clockAtArrival, exchangeTime, seqNum, record=True):

        curBidP = sorted(self.bid.keys(), reverse=True)[:self.levels]
        curAskP = sorted(self.ask.keys())[:self.levels]

        if len(self.ask) != 0:
            self.ask1p = curAskP[0]
        else:
            self.ask1p = curBidP[0] + 0.01

        if len(self.bid) != 0:
            self.bid1p = curBidP[0]
        else:
            self.bid1p = curAskP[0] - 0.01

        if record == True:
            self.localTime.append(clockAtArrival)
            self.exchangeTime.append(exchangeTime)
            self.sequenceNum.append(seqNum)

            curBidQ = [self.bid[i] for i in curBidP]
            curBidN = [len(list(self.bidnq[i].keys())) for i in curBidP]
            self.bidp += [curBidP + [0] * (self.levels - len(curBidP))]
            self.bidq += [curBidQ + [0] * (self.levels - len(curBidQ))]
            self.bidn += [curBidN + [0] * (self.levels - len(curBidN))]

            curAskQ = [self.ask[i] for i in curAskP]
            curAskN = [len(list(self.asknq[i].keys())) for i in curAskP]
            self.askp += [curAskP + [0] * (self.levels - len(curAskP))]
            self.askq += [curAskQ + [0] * (self.levels - len(curAskQ))]
            self.askn += [curAskN + [0] * (self.levels - len(curAskN))]

            self.cum_volume.append(self.cur_cum_volume)
            self.cum_amount.append(self.cur_cum_amount)
            self.close.append(self.cur_close)

            ######
            self.currMid = (self.bid1p + self.ask1p) / 2
            bid1pList = self.bidnq[self.bid1p].values()
            ask1pList = self.asknq[self.ask1p].values()
            bid_odrs, ask_odrs = calcTopK(bid1pList, ask1pList, self.topK)
            self.bid1Topq.append(bid_odrs + [0] * (self.topK - len(bid_odrs)))
            self.ask1Topq.append(ask_odrs + [0] * (self.topK - len(ask_odrs)))
            ######
            ####record these infos
            # &#
            self.calcVwapInfo()
            # &#

    def getAllInfo(self):
        ##get n levels OrderBook
        bp_names = []
        ap_names = []
        bq_names = []
        aq_names = []
        bn_names = []
        an_names = []
        for n in range(1, self.levels + 1):
            bp_names.append('bid{}p'.format(n))
            ap_names.append('ask{}p'.format(n))
            bq_names.append('bid{}q'.format(n))
            aq_names.append('ask{}q'.format(n))
            bn_names.append('bid{}n'.format(n))
            an_names.append('ask{}n'.format(n))
        btopK_names = []
        atopK_names = []
        for n in range(1, self.topK + 1):
            btopK_names.append('bid1Top{}q'.format(n))
            atopK_names.append('ask1Top{}q'.format(n))
        #
        bidp = pd.DataFrame(self.bidp, columns=bp_names)
        bidq = pd.DataFrame(self.bidq, columns=bq_names)
        bidn = pd.DataFrame(self.bidn, columns=bn_names)
        bidTopK = pd.DataFrame(self.bid1Topq, columns=btopK_names)
        askp = pd.DataFrame(self.askp, columns=ap_names)
        askq = pd.DataFrame(self.askq, columns=aq_names)
        askn = pd.DataFrame(self.askn, columns=an_names)
        askTopK = pd.DataFrame(self.ask1Topq, columns=atopK_names)
        mdDataBase = pd.DataFrame({'clockAtArrival': self.localTime, 'time': self.exchangeTime,
                                   'sequenceNo': self.sequenceNum, 'cum_volume': self.cum_volume,
                                   'cum_amount': self.cum_amount, 'close': self.close})
        aggDf = pd.DataFrame([self.total_bid_qty, self.total_ask_qty,
                              self.total_bid_vwap, self.total_ask_vwap,
                              self.total_bid_levels, self.total_ask_levels,
                              self.total_bid_orders_num, self.total_ask_orders_num]).T
        aggCols = ['total_bid_quantity', 'total_ask_quantity',
                   'total_bid_vwap', 'total_ask_vwap',
                   'total_bid_levels', 'total_ask_levels',
                   'total_bid_orders', 'total_ask_orders']
        aggDf.columns = aggCols
        lst = [mdDataBase, bidp, bidq, bidn, bidTopK, askp, askq, askn, askTopK, aggDf]
        mdData = pd.concat(lst, axis=1, sort=False)
        mdData['source'] = 100
        mdData['exchange'] = self.exchange
        mdData['StockID'] = self.stockID
        closePrice = mdData['close'].values
        openPrice = closePrice[closePrice > 0][0]
        mdData['openPrice'] = openPrice
        mdData.loc[mdData['cum_volume'] == 0, 'openPrice'] = 0
        targetCols = (['time', 'clockAtArrival', 'sequenceNo', 'StockID', 'cum_volume', 'cum_amount', 'close'] +
                      bp_names[::-1] + ap_names + bq_names[::-1] + aq_names + bn_names[::-1]
                      + an_names + btopK_names[::-1] + atopK_names + aggCols)
        mdData = mdData[targetCols].reset_index(drop=True)
        ##orderbook columns formatting
        for col in (['cum_volume', 'total_bid_quantity', 'total_ask_quantity'] + bq_names + aq_names):
            mdData[col] = mdData[col].fillna(0).astype('int64')
        for col in ['time', 'StockID', 'total_bid_levels', 'total_ask_levels',
                    'total_bid_orders', 'total_ask_orders'] + bn_names + an_names + btopK_names + atopK_names:
            mdData[col] = mdData[col].astype('int32')
        for col in ['time']:
            mdData[col] = (mdData[col] * 1000).astype('int64')
        for col in ['cum_amount']:
            mdData[col] = mdData[col].round(2)
        return mdData

    def calcVwapInfo(self):
        self.total_bid_qty.append(self.bid_qty)
        self.total_bid_levels.append(self.bid_price_levels)
        self.total_bid_orders_num.append(self.bid_order_nums)
        bmaq = 0 if self.bid_qty == 0 else self.bid_amount / self.bid_qty
        self.total_bid_vwap.append(bmaq)
        self.total_ask_qty.append(self.ask_qty)
        self.total_ask_levels.append(self.ask_price_levels)
        self.total_ask_orders_num.append(self.ask_order_nums)
        amaq = 0 if self.ask_qty == 0 else self.ask_amount / self.ask_qty
        self.total_ask_vwap.append(amaq)


def calcTopK(bid1pList, ask1pList, topK):
    bid_odrs = []
    count = 0
    for this_q in bid1pList:
        if count >= topK:
            break
        bid_odrs.append(this_q)
        count += 1
    ask_odrs = []
    count = 0
    for this_q in ask1pList:
        if count >= topK:
            break
        ask_odrs.append(this_q)
        count += 1
    return bid_odrs, ask_odrs


if __name__ == '__main__':
    import multiprocessing as mp
    import time

    db = DB("192.168.10.178", 'com_md_eq_cn', 'zhenyuy', 'bnONBrzSMGoE')
    # start date
    thisDate = datetime.date(2020, 4, 1)
    while thisDate <= datetime.date(2020, 4, 1):
        intDate = (thisDate - datetime.date(1899, 12, 30)).days
        thisDate_str = str(thisDate).replace('-', '')

        mdOrderLog = db.read('md_order', start_date=thisDate_str, end_date=thisDate_str, symbol=2002108)
        mdTradeLog = db.read('md_trade', start_date=thisDate_str, end_date=thisDate_str, symbol=2002108)

        orders_data = {}
        trades_data = {}
        orders_data[2002108] = mdOrderLog
        trades_data[2002108] = mdTradeLog
        g = go(thisDate_str, orders_data, trades_data)
        g.run(2002108)
        thisDate = thisDate + datetime.timedelta(days=1)
