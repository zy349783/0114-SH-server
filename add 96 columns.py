#!/usr/bin/env python
# coding: utf-8

# In[ ]:

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


# In[ ]:
import pandas as pd
import random
import numpy as np
import glob
import pickle
import os
import datetime
import time

i = 20200213
database_name = 'com_md_eq_cn'
user = "zhenyuy"
password = "bnONBrzSMGoE"

#
#
#
#
# # SH lv2
# startDate = str(i)
# endDate = str(i)
# db = DB("192.168.10.178", database_name, user, password)
# SH = db.read('md_snapshot_l2', start_date=startDate, end_date=endDate)
# SZ = SH[SH['skey'] > 2000000]
# SH = SH[SH['skey'] < 2000000]
# SH['num'] = SH['skey'] * 10000 + SH['ordering']
# SZ['num'] = SZ['skey'] * 10000 + SZ['ordering']
#
# SH = SH[['date', 'skey', 'time', 'cum_volume', 'cum_amount', "close", "bid1p", "bid2p", "bid3p", "bid4p", "bid5p", "bid1q", "bid2q",
#            "bid3q", "bid4q", "bid5q", "ask1p", "ask2p", "ask3p", "ask4p", "ask5p", "ask1q", "ask2q", "ask3q",
#            "ask4q", "ask5q", "open", 'num']]
# SZ = SZ[['date', 'skey', 'time', 'cum_volume', 'cum_amount', "close", "bid1p", "bid2p", "bid3p", "bid4p", "bid5p", "bid1q", "bid2q",
#            "bid3q", "bid4q", "bid5q", "ask1p", "ask2p", "ask3p", "ask4p", "ask5p", "ask1q", "ask2q", "ask3q",
#            "ask4q", "ask5q", "open", 'num']]
#
# startDate = str(i)
# endDate = str(i)
#
# readPath = 'F:\\data\\20200731\\logs_***_zt_96_04_day_96data'
# dataPathLs = np.array(glob.glob(readPath))
# dateLs = np.array([os.path.basename(i).split('_')[1] for i in dataPathLs])
# dataPathLs = dataPathLs[(dateLs >= startDate) & (dateLs <= endDate)]
#
#
# for n in range(len(dataPathLs)):
#     path1 = np.array(glob.glob(dataPathLs[n] + '/mdLog_SH_***'))
#     SH1 = pd.read_csv(path1[0])
#     index1 = SH1[SH1['StockID'].isin([16, 300, 852, 905])]
#     SH1 = SH1[SH1['source'] == 13]
#
#     SH1['skey'] = SH1['StockID'] + 1000000
#     SH1 = SH1.rename(columns={"openPrice":"open"})
#     SH1["open"] = np.where(SH1["cum_volume"] > 0, SH1.groupby("skey")["open"].transform("max"), SH1["open"])
#     SH1["time"] = SH1["time"].apply(lambda x: int((x.replace(':', "")).replace(".", "")) * 1000)
#
# SH1 = SH1[['clockAtArrival', 'sequenceNo', 'skey', 'time', 'cum_volume', 'cum_amount', "close", "bid1p", "bid2p", "bid3p", "bid4p", "bid5p", "bid1q", "bid2q",
#            "bid3q", "bid4q", "bid5q", "ask1p", "ask2p", "ask3p", "ask4p", "ask5p", "ask1q", "ask2q", "ask3q",
#            "ask4q", "ask5q", "open"]]
# for cols in ['cum_amount', "close", 'open']:
#     SH1[cols] = SH1[cols].round(2)
# cols = ['skey', 'time', 'cum_volume', 'cum_amount', "close", "bid1p", "bid2p", "bid3p", "bid4p", "bid5p", "bid1q", "bid2q",
#            "bid3q", "bid4q", "bid5q", "ask1p", "ask2p", "ask3p", "ask4p", "ask5p", "ask1q", "ask2q", "ask3q",
#            "ask4q", "ask5q", "open"]
# SH1 = SH1[SH1['skey'].isin(SH['skey'].unique())]
# re = pd.merge(SH, SH1, on=cols, how='outer')
# assert(re[re['date'].isnull()]['skey'].unique().min() > 1688000)
# assert(re[re['date'].isnull()]['time'].unique().min() > 150000000000)
# print('%.2f%%' % (re[(re['sequenceNo'].isnull()) & (re['time'] < 150000000000) & (~re['skey'].isin(list(set(SH['skey'].unique()) - set(SH1['skey'].unique())))) & (re['skey'] < 1688000)].shape[0] / SH.shape[0] * 100))
#
# p2 = re[(re['sequenceNo'].isnull())]
#
# p11 = re[(~re['sequenceNo'].isnull()) & (~re['date'].isnull())][re[(~re['sequenceNo'].isnull())
#                                                     & (~re['date'].isnull())]['num'].duplicated(keep=False)]
# p12 = re[(~re['sequenceNo'].isnull()) & (~re['date'].isnull())].drop_duplicates(['num'], keep=False)
# p11 = p11.sort_values(by=['num', 'sequenceNo'])
# print(p11)
# p11["order1"] = p11.groupby(["num"]).cumcount()
# p11["order2"] = p11.groupby(["sequenceNo"]).cumcount()
# p11 = p11[p11['order1'] == p11['order2']]
#
# p11_1 = re[(~re['sequenceNo'].isnull()) & (~re['date'].isnull())][re[(~re['sequenceNo'].isnull())
#                                                     & (~re['date'].isnull())]['num'].duplicated(keep=False)].drop_duplicates('num')
# p11_1 = pd.merge(p11_1, p11[['num', 'order1']], on='num', how='left')
# p11_1 = p11_1[p11_1['order1'].isnull()]
# p11_1['sequenceNo'] = np.nan
# p11_1['clockAtArrival'] = np.nan
#
# p11.drop(['order1', 'order2'],axis=1,inplace=True)
# p11_1.drop(['order1'],axis=1,inplace=True)
# p11 = pd.concat([p11, p11_1])
#
# p1 = pd.concat([p11, p12])
# re1 = pd.concat([p1, p2])
# re1 = re1.sort_values(by='num')
# re1['seq1'] = re1.groupby('skey')['sequenceNo'].ffill().bfill()
# sl = list(set(SH['skey'].unique()) - set(SH1['skey'].unique()))
# re1.loc[re1['skey'].isin(sl), 'seq1'] = np.nan
# re1['count1'] = re1.groupby(['seq1']).cumcount()
# re1['cc'] = np.where(re1['sequenceNo'] == re1['seq1'], re1['count1'], 0)
# re1['cc'] = re1.groupby(['seq1'])['cc'].transform('max')
# re1['count'] = re1['count1']-re1['cc']
# re1.drop(["cc"],axis=1,inplace=True)
# re1.drop(["count1"],axis=1,inplace=True)
# re1['dup'] = np.where(~re1["sequenceNo"].isnull(), re1.groupby(['sequenceNo']).cumcount(), 0)
# re1['dup1'] = np.where(~re1["sequenceNo"].isnull(), re1.groupby(['sequenceNo'])['num'].transform('nunique'), 0)
# re1['nan'] = np.where((re1['sequenceNo'].isnull()) | (re1['dup'] != 0), 1, 0)
# re1['count'] = np.where(re1['dup1'] > 1, re1['dup'], re1['count'])
# re1.loc[(re1['dup1'] > 1) & (re1['dup'] > 0), 'sequenceNo'] = np.nan
# assert((len(set(sl) - set(re1[re1['seq1'].isnull()]['skey'].unique())) == 0) &
#            (len(set(re1[re1['seq1'].isnull()]['skey'].unique()) - set(sl)) == 0))
# assert(re1.shape[0] == SH.shape[0])
#
# print('%.2f%%' % (re1[re1['sequenceNo'].isnull()].shape[0]/re1.shape[0] * 100))
#
#
# # In[ ]:
#
#
# # SZ lv2
# startDate = str(i)
# endDate = str(i)
#
# readPath = 'F:\\data\\20200731\\logs_***_zt_96_04_day_96data'
# dataPathLs = np.array(glob.glob(readPath))
# dateLs = np.array([os.path.basename(i).split('_')[1] for i in dataPathLs])
# dataPathLs = dataPathLs[(dateLs >= startDate) & (dateLs <= endDate)]
#
#
# for n in range(len(dataPathLs)):
#     path1 = np.array(glob.glob(dataPathLs[n] + '/mdLog_SZ_***'))
#     SZ1 = pd.read_csv(path1[0])
#     SZ1 = SZ1[SZ1['source'] == 13]
#
#     SZ1['skey'] = SZ1['StockID'] + 2000000
#     SZ1 = SZ1.rename(columns={"openPrice":"open"})
#     SZ1["open"] = np.where(SZ1["cum_volume"] > 0, SZ1.groupby("skey")["open"].transform("max"), SZ1["open"])
#     SZ1["time"] = SZ1["time"].apply(lambda x: int((x.replace(':', "")).replace(".", "")) * 1000)
#
# SZ1 = SZ1[['clockAtArrival', 'sequenceNo', 'skey', 'time', 'cum_volume', 'cum_amount', "close", "bid1p", "bid2p", "bid3p", "bid4p", "bid5p", "bid1q", "bid2q",
#            "bid3q", "bid4q", "bid5q", "ask1p", "ask2p", "ask3p", "ask4p", "ask5p", "ask1q", "ask2q", "ask3q",
#            "ask4q", "ask5q", "open"]]
# for cols in ['cum_amount']:
#     SZ1[cols] = SZ1[cols].round(2)
# cols = ['skey', 'time', 'cum_volume', 'cum_amount', "close", "bid1p", "bid2p", "bid3p", "bid4p", "bid5p", "bid1q", "bid2q",
#            "bid3q", "bid4q", "bid5q", "ask1p", "ask2p", "ask3p", "ask4p", "ask5p", "ask1q", "ask2q", "ask3q",
#            "ask4q", "ask5q", "open"]
# SZ1 = SZ1[SZ1['skey'].isin(SZ['skey'].unique())]
# if SZ1[SZ1['sequenceNo'].duplicated(keep=False)].shape[0] != 0:
#     te_st = SZ1[SZ1.duplicated('sequenceNo', keep=False)]
#     te_st = te_st.sort_values(by='clockAtArrival')
#     te_st['caa'] = te_st.groupby('sequenceNo').cumcount()
#     SZ1 = pd.merge(SZ1, te_st, on=['clockAtArrival', 'sequenceNo'], how='left')
#
#
# re = pd.merge(SZ, SZ1, on=cols, how='outer')
#
# print(re.shape[0])
# print(re[~re['sequenceNo'].isnull()].shape[0])
# print(re[~re['date'].isnull()].shape[0])
# print(SZ.shape[0])
# print(SZ1.shape[0])
# print('%.2f%%' % (re[(re['sequenceNo'].isnull()) & (re['time'] < 150000000000) & (~re['skey'].isin(list(set(SZ['skey'].unique()) - set(SZ1['skey'].unique()))))].shape[0] / SZ.shape[0] * 100))
#
# try:
#     assert(re.shape[0] == re[~re['date'].isnull()].shape[0])
#     print('SZ lv2 is complete')
# except:
#     print('%.2f%%' % (re[~re['date'].isnull()].shape[0]/re.shape[0] * 100))
#     print('96 have unique values not shared by database')
#     re = pd.merge(SZ, SZ1, on=cols, how='left')
#
# if re[re.duplicated('num', keep=False)].shape[0] == 0:
#     re2 = re.sort_values(by='num')
#     re2['seq1'] = re2.groupby('skey')['sequenceNo'].ffill().bfill()
#     sl = list(set(SZ['skey'].unique()) - set(SZ1['skey'].unique()))
#     re2.loc[re2['skey'].isin(sl), 'seq1'] = np.nan
#     re2['count1'] = re2.groupby(['seq1']).cumcount()
#     re2['cc'] = np.where(re2['sequenceNo'] == re2['seq1'], re2['count1'], 0)
#     re2['cc'] = re2.groupby(['seq1'])['cc'].transform('max')
#     re2['count'] = re2['count1'] - re2['cc']
#     re2.drop(["cc"], axis=1, inplace=True)
#     re2.drop(["count1"], axis=1, inplace=True)
#     re2['dup'] = np.where(~re2["sequenceNo"].isnull(), re2.groupby(['sequenceNo']).cumcount(), 0)
#     re2['dup1'] = np.where(~re2["sequenceNo"].isnull(), re2.groupby(['sequenceNo'])['num'].transform('nunique'), 0)
#     re2['nan'] = np.where((re2['sequenceNo'].isnull()) | (re2['dup'] != 0), 1, 0)
#     re2['count'] = np.where(re2['dup1'] > 1, re2['dup'], re2['count'])
#     re2.loc[(re2['dup1'] > 1) & (re2['dup'] > 0), 'sequenceNo'] = np.nan
#     assert((len(set(sl) - set(re2[re2['seq1'].isnull()]['skey'].unique())) == 0) &
#            (len(set(re2[re2['seq1'].isnull()]['skey'].unique()) - set(sl)) == 0))
#     assert(re2.shape[0] == SZ.shape[0])
#
#     print('%.2f%%' % (re2[re2['sequenceNo'].isnull()].shape[0]/re2.shape[0] * 100))
#
#
# else:
#     p1 = re[re['num'].duplicated(keep=False)]
#     p2 = re.drop_duplicates(['num'], keep=False)
#     p1["order1"] = p1.groupby(["num"]).cumcount()
#     p1["order2"] = p1.groupby(["sequenceNo"]).cumcount()
#     p1 = p1[p1['order1'] == p1['order2']]
#     p1.drop(['order1', 'order2'],axis=1,inplace=True)
#     re = pd.concat([p1, p2])
#     re2 = re.sort_values(by='num')
#     re2['seq1'] = re2.groupby('skey')['sequenceNo'].ffill().bfill()
#     sl = list(set(SZ['skey'].unique()) - set(SZ1['skey'].unique()))
#     re2.loc[re2['skey'].isin(sl), 'seq1'] = np.nan
#     re2['count1'] = re2.groupby(['seq1']).cumcount()
#     re2['cc'] = np.where(re2['sequenceNo'] == re2['seq1'], re2['count1'], 0)
#     re2['cc'] = re2.groupby(['seq1'])['cc'].transform('max')
#     re2['count'] = re2['count1'] - re2['cc']
#     re2.drop(["cc"], axis=1, inplace=True)
#     re2.drop(["count1"], axis=1, inplace=True)
#     re2['dup'] = np.where(~re2["sequenceNo"].isnull(), re2.groupby(['sequenceNo']).cumcount(), 0)
#     re2['dup1'] = np.where(~re2["sequenceNo"].isnull(), re2.groupby(['sequenceNo'])['num'].transform('nunique'), 0)
#     re2['nan'] = np.where((re2['sequenceNo'].isnull()) | (re2['dup'] != 0), 1, 0)
#     re2['count'] = np.where(re2['dup1'] > 1, re2['dup'], re2['count'])
#     re2.loc[(re2['dup1'] > 1) & (re2['dup'] > 0), 'sequenceNo'] = np.nan
#     assert((len(set(sl) - set(re2[re2['seq1'].isnull()]['skey'].unique())) == 0) &
#            (len(set(re2[re2['seq1'].isnull()]['skey'].unique()) - set(sl)) == 0))
#     assert(re2.shape[0] == SZ.shape[0])
#
#     print('%.2f%%' % (re2[re2['sequenceNo'].isnull()].shape[0]/re2.shape[0] * 100))


# In[ ]:


# SH & SZ trade

startDate = str(i)
endDate = str(i)
database_name = 'com_md_eq_cn'
user = "zhenyuy"
password = "bnONBrzSMGoE"

db = DB("192.168.10.178", database_name, user, password)
trade = db.read('md_trade', start_date=startDate, end_date=endDate)[['skey', 'date', 'ApplSeqNum', 'time']]

startDate = str(i)
endDate = str(i)

readPath = 'F:\\data\\20200731\\logs_***_zt_96_04_day_96data'
dataPathLs = np.array(glob.glob(readPath))
dateLs = np.array([os.path.basename(i).split('_')[1] for i in dataPathLs])
dataPathLs = dataPathLs[(dateLs >= startDate) & (dateLs <= endDate)]
for n in range(len(dataPathLs)):
    path1 = np.array(glob.glob(dataPathLs[n] + '/mdTradeLog***'))
    trade1 = pd.read_csv(path1[0])
trade1['skey'] = np.where(trade1['exchId'] == 2, trade1['SecurityID'] + 2000000, trade1['SecurityID'] + 1000000)
trade1 = trade1[trade1['skey'].isin(trade['skey'].unique())]
assert(trade1[trade1['TransactTime'] > trade['time'].max()/1000]['ChannelNo'].unique() == [103])
assert(trade1[trade1['ChannelNo'] == 103]['TransactTime'].min() > trade['time'].max()/1000)
trade1 = trade1[trade1['ChannelNo'] != 103]
trade = trade[['skey', 'date', 'ApplSeqNum']]
re = pd.merge(trade, trade1[['skey', 'ApplSeqNum', 'sequenceNo', 'clockAtArrival']], on=['skey', 'ApplSeqNum'],
             how='outer')
try:
    assert(re.shape[0] == trade.shape[0])
    print('trade data is complete')
    k = 0
except:
    print('%.2f%%' % (trade.shape[0]/re.shape[0] * 100))
    k = 1
    print('trade data incomplete')
    k1 = pd.merge(trade1, re[re['date'].isnull()][['skey', 'ApplSeqNum']], on=['skey', 'ApplSeqNum'], how='right')
    print(k1.shape[0])
    print(k1['ExecType'].unique())
    print(k1['TransactTime'].unique())
    k1['date'] = trade['date'].iloc[0]
    new_trade_data += [k1[['clockAtArrival', 'sequenceNo', 'TransactTime', 'ApplSeqNum', 'date', 'skey', 'ExecType', 'TradeBSFlag',
   'TradePrice', 'TradeQty', 'BidApplSeqNum', 'OfferApplSeqNum']]]
    re = pd.merge(trade, trade1[['skey', 'ApplSeqNum', 'sequenceNo', 'clockAtArrival']], on=['skey', 'ApplSeqNum'],
             how='left')
    assert(re.shape[0] == trade.shape[0])

re3 = re.sort_values(by=['skey', 'ApplSeqNum'])
re3['seq1'] = re3.groupby('skey')['sequenceNo'].ffill().bfill()
sl = list(set(trade['skey'].unique()) - set(trade1['skey'].unique()))
re3.loc[re3['skey'].isin(sl), 'seq1'] = np.nan
re3['count1'] = re3.groupby(['seq1']).cumcount()
re3['cc'] = np.where(re3['sequenceNo'] == re3['seq1'], re3['count1'], 0)
re3['cc'] = re3.groupby(['seq1'])['cc'].transform('max')
re3['count'] = re3['count1']-re3['cc']
re3.drop(["count1"],axis=1,inplace=True)
re3.drop(["cc"],axis=1,inplace=True)
re3['dup'] = np.where(~re3["sequenceNo"].isnull(), re3.groupby(['sequenceNo']).cumcount(), 0)
re3['dup1'] = np.where(~re3["sequenceNo"].isnull(), re3.groupby(['sequenceNo'])['ApplSeqNum'].transform('nunique'), 0)
re3['nan'] = np.where((re3['sequenceNo'].isnull()) | (re3['dup'] != 0), 1, 0)
re3['count'] = np.where(re3['dup1'] > 1, re3['dup'], re3['count'])
re3.loc[(re3['dup1'] > 1) & (re3['dup'] > 0), 'sequenceNo'] = np.nan
assert((len(set(sl) - set(re3[re3['seq1'].isnull()]['skey'].unique())) == 0) &
       (len(set(re3[re3['seq1'].isnull()]['skey'].unique()) - set(sl)) == 0))
assert(re3.shape[0] == trade.shape[0])
if k == 1:
    k1['seq1'] = k1['sequenceNo']
    k1['count'] = 0
    k1['nan'] = 0
    k1['dup1'] = 1
    re3 = pd.concat([re3, k1[['clockAtArrival', 'date', 'sequenceNo', 'skey', 'ApplSeqNum', 'seq1',
                              'count', 'nan', 'dup1']]])

print('%.2f%%' % (re3[re3['sequenceNo'].isnull()].shape[0]/re3.shape[0] * 100))


# In[ ]:


# SZ order

startDate = str(i)
endDate = str(i)
database_name = 'com_md_eq_cn'
user = "zhenyuy"
password = "bnONBrzSMGoE"

db = DB("192.168.10.178", database_name, user, password)
order = db.read('md_order', start_date=startDate, end_date=endDate)[['skey', 'date', 'ApplSeqNum']]

startDate = str(i)
endDate = str(i)

readPath = 'F:\\data\\20200731\\logs_***_zt_96_04_day_96data'
dataPathLs = np.array(glob.glob(readPath))
dateLs = np.array([os.path.basename(i).split('_')[1] for i in dataPathLs])
dataPathLs = dataPathLs[(dateLs >= startDate) & (dateLs <= endDate)]
for n in range(len(dataPathLs)):
    path1 = np.array(glob.glob(dataPathLs[n] + '/mdOrderLog***'))
    order1 = pd.read_csv(path1[0])
order1['skey'] = order1['SecurityID'] + 2000000
order1 = order1[order1['skey'].isin(order['skey'].unique())]
re = pd.merge(order, order1[['skey', 'ApplSeqNum', 'sequenceNo', 'clockAtArrival']], on=['skey', 'ApplSeqNum'],
             how='outer')
try:
    assert(re.shape[0] == order.shape[0])
    print('order data is complete')
    k = 0
except:
    print('%.2f%%' % (order.shape[0]/re.shape[0] * 100))
    k = 1
    print('order data incomplete')
    k2 = pd.merge(order1, re[re['date'].isnull()][['skey', 'ApplSeqNum']], on=['skey', 'ApplSeqNum'], how='right')
    print(k2.shape[0])
    print(k2['SecurityID'].unique())
    print(k2['TransactTime'].unique())
    k2['date'] = order['date'].iloc[0]
    new_order_data += [k2[['clockAtArrival', 'sequenceNo', 'TransactTime', 'ApplSeqNum', 'date', 'skey', 'Side',
   'OrderType', 'Price', 'OrderQty']]]
    re = pd.merge(order, order1[['skey', 'ApplSeqNum', 'sequenceNo', 'clockAtArrival']], on=['skey', 'ApplSeqNum'],
             how='left')
    assert(re.shape[0] == order.shape[0])

re4 = re.sort_values(by=['skey', 'ApplSeqNum'])
re4['seq1'] = re4.groupby('skey')['sequenceNo'].ffill().bfill()
sl = list(set(order['skey'].unique()) - set(order1['skey'].unique()))
re4.loc[re4['skey'].isin(sl), 'seq1'] = np.nan
re4['count1'] = re4.groupby(['seq1']).cumcount()
re4['cc'] = np.where(re4['sequenceNo'] == re4['seq1'], re4['count1'], 0)
re4['cc'] = re4.groupby(['seq1'])['cc'].transform('max')
re4['count'] = re4['count1'] - re4['cc']
re4.drop(["cc"], axis=1, inplace=True)
re4.drop(["count1"], axis=1, inplace=True)
re4['dup'] = np.where(~re4["sequenceNo"].isnull(), re4.groupby(['sequenceNo']).cumcount(), 0)
re4['dup1'] = np.where(~re4["sequenceNo"].isnull(), re4.groupby(['sequenceNo'])['ApplSeqNum'].transform('nunique'), 0)
re4['nan'] = np.where((re4['sequenceNo'].isnull()) | (re4['dup'] != 0), 1, 0)
re4['count'] = np.where(re4['dup1'] > 1, re4['dup'], re4['count'])
re4.loc[(re4['dup1'] > 1) & (re4['dup'] > 0), 'sequenceNo'] = np.nan
assert((len(set(sl) - set(re4[re4['seq1'].isnull()]['skey'].unique())) == 0) &
       (len(set(re4[re4['seq1'].isnull()]['skey'].unique()) - set(sl)) == 0))
assert(re4.shape[0] == order.shape[0])
if k == 1:
    k2['seq1'] = k2['ApplSeqNum']
    k2['count'] = 0
    k2['nan'] = 0
    k2['dup1'] = 1
    re4 = pd.concat([re4, k2[['clockAtArrival', 'date', 'sequenceNo', 'skey', 'ApplSeqNum', 'seq1',
                              'count', 'nan', "dup1"]]])


print('%.2f%%' % (re4[re4['sequenceNo'].isnull()].shape[0]/re4.shape[0] * 100))


# In[ ]:


# SH index
    
# startDate = str(i)
# endDate = str(i)
# database_name = 'com_md_eq_cn'
# user = "zhenyuy"
# password = "bnONBrzSMGoE"
#
# db = DB("192.168.10.178", database_name, user, password)
# index = db.read('md_index', start_date=startDate, end_date=endDate)
#
# index1['skey'] = index1['StockID'] + 1000000
# index1 = index1.rename(columns={"openPrice":"open"})
# index1["open"] = np.where(index1["cum_volume"] > 0, index1.groupby("skey")["open"].transform("max"), index1["open"])
# index1['close'] = np.where(index1['cum_volume'] == 0, 0, index1['close'])
# index1["time"] = index1["time"].apply(lambda x: int((x.replace(':', "")).replace(".", "")) * 1000)
# index['close'] = np.where(index['cum_volume'] == 0, 0, index['close'])
# index['num'] = index['skey'] * 10000 + index['ordering']
# index1 = index1[index1['source'] == 13]
# index = index[['skey', 'date', 'cum_volume', 'cum_amount', "close", "open", 'num']]
# index1 = index1[['clockAtArrival', 'sequenceNo', 'skey', 'cum_volume', 'cum_amount', "close", "open", "time"]]
# for cols in ['cum_amount']:
#     index1[cols] = index1[cols].round(1)
# cols = ['skey', 'cum_volume', 'cum_amount', "close", "open"]
# index1 = index1[index1['skey'].isin(index['skey'].unique())]
# re = pd.merge(index, index1, on=cols, how='outer')
#
# print(re.shape[0])
# print(re[~re['sequenceNo'].isnull()].shape[0])
# print(re[~re['date'].isnull()].shape[0])
# print(index.shape[0])
# print(index1.shape[0])
#
# try:
#     assert(re.shape[0] == re[~re['date'].isnull()].shape[0])
#     print('index data is complete')
# except:
#     print('%.2f%%' % (re[~re['date'].isnull()].shape[0]/re.shape[0] * 100))
#     re = pd.merge(index, index1, on=cols, how='left')
#     print('96 have unique values not shared by database')
#
# p11 = re[re.duplicated('num', keep=False)]
# p2 = re.drop_duplicates('num', keep=False)
# p11["order1"] = p11.groupby(["num"]).cumcount()
# p11["order2"] = p11.groupby(["sequenceNo"]).cumcount()
# p11 = p11[p11['order1'] == p11['order2']]
#
# p12 = re[re.duplicated('num', keep=False)].drop_duplicates('num')
# p12 = pd.merge(p12, p11[['num', 'order1']], on='num', how='left')
# p12 = p12[p12['order1'].isnull()]
# p12['sequenceNo'] = np.nan
# p12['clockAtArrival'] = np.nan
#
# p11.drop(['order1', 'order2'],axis=1,inplace=True)
# p12.drop(['order1'],axis=1,inplace=True)
# p1 = pd.concat([p11, p12])
#
# re = pd.concat([p1, p2])
# assert(re[re.duplicated('num', keep=False)].shape[0] == 0)
#
# if re[re['sequenceNo'].isnull()].shape[0] != 0:
#     re5 = re.sort_values(by='num')
#     re5['seq1'] = re5.groupby('skey')['sequenceNo'].ffill().bfill()
#     sl = list(set(index['skey'].unique()) - set(index1['skey'].unique()))
#     re5.loc[re5['skey'].isin(sl), 'seq1'] = np.nan
#     re5['count1'] = re5.groupby(['seq1']).cumcount()
#     re5['cc'] = np.where(re5['sequenceNo'] == re5['seq1'], re5['count1'], 0)
#     re5['cc'] = re5.groupby(['seq1'])['cc'].transform('max')
#     re5['count'] = re5['count1'] - re5['cc']
#     re5.drop(["cc"], axis=1, inplace=True)
#     re5.drop(["count1"], axis=1, inplace=True)
#     re5['dup'] = np.where(~re5["sequenceNo"].isnull(), re5.groupby(['sequenceNo']).cumcount(), 0)
#     re5['dup1'] = np.where(~re5["sequenceNo"].isnull(), re5.groupby(['sequenceNo'])['num'].transform('nunique'), 0)
#     re5['nan'] = np.where((re5['sequenceNo'].isnull()) | (re5['dup'] != 0), 1, 0)
#     re5['count'] = np.where(re5['dup1'] > 1, re5['dup'], re5['count'])
#     re5.loc[(re5['dup1'] > 1) & (re5['dup'] > 0), 'sequenceNo'] = np.nan
#     assert((len(set(sl) - set(re5[re5['seq1'].isnull()]['skey'].unique())) == 0) &
#            (len(set(re5[re5['seq1'].isnull()]['skey'].unique()) - set(sl)) == 0))
#     assert(re5.shape[0] == index.shape[0])
#
#     print('%.0f%%' % (re5[re5['sequenceNo'].isnull()].shape[0]/re5.shape[0] * 100))
# else:
#     re5 = re.sort_values(by='num')
#     re5['seq1'] = re5['sequenceNo']
#     sl = list(set(index['skey'].unique()) - set(index1['skey'].unique()))
#     re5.loc[re5['skey'].isin(sl), 'seq1'] = np.nan
#     re5['count1'] = re5.groupby(['seq1']).cumcount()
#     re5['cc'] = np.where(re5['sequenceNo'] == re5['seq1'], re5['count1'], 0)
#     re5['cc'] = re5.groupby(['seq1'])['cc'].transform('max')
#     re5['count'] = re5['count1'] - re5['cc']
#     re5.drop(["cc"], axis=1, inplace=True)
#     re5.drop(["count1"], axis=1, inplace=True)
#     re5['dup'] = np.where(~re5["sequenceNo"].isnull(), re5.groupby(['sequenceNo']).cumcount(), 0)
#     re5['dup1'] = np.where(~re5["sequenceNo"].isnull(), re5.groupby(['sequenceNo'])['num'].transform('nunique'), 0)
#     re5['nan'] = np.where((re5['sequenceNo'].isnull()) | (re5['dup'] != 0), 1, 0)
#     re5['count'] = np.where(re5['dup1'] > 1, re5['dup'], re5['count'])
#     re5.loc[(re5['dup1'] > 1) & (re5['dup'] > 0), 'sequenceNo'] = np.nan
#     assert((len(set(sl) - set(re5[re5['seq1'].isnull()]['skey'].unique())) == 0) &
#            (len(set(re5[re5['seq1'].isnull()]['skey'].unique()) - set(sl)) == 0))
#     assert(re5.shape[0] == index.shape[0])
#
#     print('%.0f%%' % (re5[re5['sequenceNo'].isnull()].shape[0]/re5.shape[0] * 100))


# In[ ]:


# final concat

try:
    assert(len(set(SZ1['sequenceNo']) & set(SH1['sequenceNo'])) == 0)
except:
    print(SZ1[SZ1['sequenceNo'].isin(list(set(SZ1['sequenceNo']) & set(SH1['sequenceNo'])))])
    print(SH1[SH1['sequenceNo'].isin(list(set(SZ1['sequenceNo']) & set(SH1['sequenceNo'])))])
try:
    assert(len(set(SZ1['sequenceNo']) & set(trade1['sequenceNo'])) == 0)
except:
    print(SZ1[SZ1['sequenceNo'].isin(list(set(SZ1['sequenceNo']) & set(trade1['sequenceNo'])))])
    print(trade1[trade1['sequenceNo'].isin(list(set(SZ1['sequenceNo']) & set(trade1['sequenceNo'])))])
try:
    assert(len(set(SZ1['sequenceNo']) & set(order1['sequenceNo'])) == 0)
except:
    print(SZ1[SZ1['sequenceNo'].isin(list(set(SZ1['sequenceNo']) & set(order1['sequenceNo'])))])
    print(order1[order1['sequenceNo'].isin(list(set(SZ1['sequenceNo']) & set(order1['sequenceNo'])))])
try:
    assert(len(set(SZ1['sequenceNo']) & set(index1['sequenceNo'])) == 0)
except:
    print(SZ1[SZ1['sequenceNo'].isin(list(set(SZ1['sequenceNo']) & set(index1['sequenceNo'])))])
    print(index1[index1['sequenceNo'].isin(list(set(SZ1['sequenceNo']) & set(index1['sequenceNo'])))])
try:
    assert(len(set(SH1['sequenceNo']) & set(index1['sequenceNo'])) == 0)
except:
    print(SH1[SH1['sequenceNo'].isin(list(set(SH1['sequenceNo']) & set(index1['sequenceNo'])))])
    print(index1[index1['sequenceNo'].isin(list(set(SH1['sequenceNo']) & set(index1['sequenceNo'])))])
try:
    assert(len(set(SH1['sequenceNo']) & set(trade1['sequenceNo'])) == 0)
except:
    print(SH1[SH1['sequenceNo'].isin(list(set(SH1['sequenceNo']) & set(trade1['sequenceNo'])))])
    print(trade1[trade1['sequenceNo'].isin(list(set(SH1['sequenceNo']) & set(trade1['sequenceNo'])))])
try:
    assert(len(set(SH1['sequenceNo']) & set(order1['sequenceNo'])) == 0)
except:
    print(SH1[SH1['sequenceNo'].isin(list(set(SH1['sequenceNo']) & set(order1['sequenceNo'])))])
    print(order1[order1['sequenceNo'].isin(list(set(SH1['sequenceNo']) & set(order1['sequenceNo'])))])
try:
    assert(len(set(trade1['sequenceNo']) & set(order1['sequenceNo'])) == 0)
except:
    print(trade1[trade1['sequenceNo'].isin(list(set(trade1['sequenceNo']) & set(order1['sequenceNo'])))])
    print(order1[order1['sequenceNo'].isin(list(set(trade1['sequenceNo']) & set(order1['sequenceNo'])))])
try:
    assert(len(set(trade1['sequenceNo']) & set(index1['sequenceNo'])) == 0)
except:
    print(trade1[trade1['sequenceNo'].isin(list(set(trade1['sequenceNo']) & set(index1['sequenceNo'])))])
    print(index1[index1['sequenceNo'].isin(list(set(trade1['sequenceNo']) & set(index1['sequenceNo'])))])
try:
    assert(len(set(index1['sequenceNo']) & set(order1['sequenceNo'])) == 0)
except:
    print(index1[index1['sequenceNo'].isin(list(set(index1['sequenceNo']) & set(order1['sequenceNo'])))])
    print(order1[order1['sequenceNo'].isin(list(set(index1['sequenceNo']) & set(order1['sequenceNo'])))])

del SH
del SH1
del SZ
del SZ1
del trade
del trade1
del order
del order1
del index
del index1
re1['tag'] = 'SH'
re2['tag'] = 'SZ'
re3['tag'] = 'trade'
re4['tag'] = 'order'
re5['tag'] = 'index'

re1 = re1[['skey', 'date', 'num', 'sequenceNo', 'seq1', 'clockAtArrival', 'nan', 'count', 'tag', 'dup1']]
re2 = re2[['skey', 'date', 'num', 'sequenceNo', 'seq1', 'clockAtArrival', 'nan', 'count', 'tag', 'dup1']]
re3 = re3[['skey', 'date', 'ApplSeqNum', 'sequenceNo', 'seq1', 'clockAtArrival', 'nan', 'count', 'tag', 'dup1']]
re4 = re4[['skey', 'date', 'ApplSeqNum', 'sequenceNo', 'seq1', 'clockAtArrival', 'nan', 'count', 'tag', 'dup1']]
re5 = re5[['skey', 'date', 'num', 'sequenceNo', 'seq1', 'clockAtArrival', 'nan', 'count', 'tag', 'dup1']]
re1 = re1.sort_values(by='num').reset_index(drop=True)
re1['seq2'] = re1.index
re2 = re2.sort_values(by='num').reset_index(drop=True)
re2['seq2'] = re2.index
re3 = re3.sort_values(by=['skey', 'ApplSeqNum']).reset_index(drop=True)
re3['seq2'] = re3.index
re4 = re4.sort_values(by=['skey', 'ApplSeqNum']).reset_index(drop=True)
re4['seq2'] = re4.index
re5 = re5.sort_values(by='num').reset_index(drop=True)
re5['seq2'] = re5.index

fr1 = []
fr2 = []
fr1 += [re1[re1['seq1'].isnull()]]
fr2 += [re1[~re1['seq1'].isnull()]]
del re1
print('1. here~')
fr1 += [re2[re2['seq1'].isnull()]]
fr2 += [re2[~re2['seq1'].isnull()]]
del re2
print('2. here~')
fr1 += [re3[re3['seq1'].isnull()]]
fr2 += [re3[~re3['seq1'].isnull()]]
del re3
print('3. here~')
fr1 += [re4[re4['seq1'].isnull()]]
fr2 += [re4[~re4['seq1'].isnull()]]
del re4
print('4. here~')
fr1 += [re5[re5['seq1'].isnull()]]
fr2 += [re5[~re5['seq1'].isnull()]]
del re5
print('5. here~')
fr1 = pd.concat(fr1).reset_index(drop=True)
fr2 = pd.concat(fr2).reset_index(drop=True)

startTm = datetime.datetime.now()
fr2 = fr2.sort_values(by=['seq1', 'seq2'])
print(datetime.datetime.now() - startTm)

#     fr2.loc[(fr2['nan']==0) & (fr2['dup1']==1), 'count'] = 0
fr2['sum_nan'] = fr2['nan'].cumsum()
fr2['sequenceNo'] = fr2['sequenceNo'] + fr2['sum_nan']
startTm = datetime.datetime.now()
fr2['sequenceNo'] = fr2.groupby('seq1')['sequenceNo'].ffill().bfill()
print(datetime.datetime.now() - startTm)
fr2['sequenceNo'] = fr2['sequenceNo'] + fr2['count']
fr21 = fr2[~fr2['sequenceNo'].isnull()]
fr22 = fr2[fr2['sequenceNo'].isnull()]
print(fr22.shape[0])
print(fr21.shape[0])
print(fr2.shape[0])
if fr22.shape[0] != 0:
    fr22['sequenceNo'] = range(int(fr21['sequenceNo'].max()) + 1, int(fr21['sequenceNo'].max()) + 1 + fr22.shape[0])
    fr2 = pd.concat([fr21, fr22])
del fr21
del fr22
print(fr2.shape[0])
try:
    assert(fr2[fr2.duplicated('sequenceNo', keep=False)].shape[0] == 0)
except:
    te_st = fr2[fr2.duplicated('sequenceNo', keep=False)]
    print(te_st)
    caa = te_st['clockAtArrival'].max()
    seq = te_st['sequenceNo'].iloc[0]
    m_in = fr2[fr2['sequenceNo'] > seq]['sequenceNo'].min()
    if m_in > seq + 1:
        fr2.loc[fr2['sequenceNo'] > seq, 'sequenceNo'] = fr2[fr2['sequenceNo'] > seq]['sequenceNo'] + 1
        fr2.loc[(fr2['sequenceNo'] == seq) & (fr2['clockAtArrival'] == caa), 'sequenceNo'] = seq + 1
    else:
        fr2.loc[fr2['sequenceNo'] > seq, 'sequenceNo'] = fr2[fr2['sequenceNo'] > seq]['sequenceNo'] + 2
        fr2.loc[(fr2['sequenceNo'] == seq) & (fr2['clockAtArrival'] == caa), 'sequenceNo'] = seq + 1
    assert(fr2[fr2.duplicated('sequenceNo', keep=False)].shape[0] == 0)


fr1['sequenceNo'] = range(int(fr2['sequenceNo'].max()) + 1, int(fr2['sequenceNo'].max()) + 1 + fr1.shape[0])
fr2 = pd.concat([fr1, fr2])
del fr1
assert(fr2[fr2.duplicated('sequenceNo', keep=False)].shape[0] == 0)

import pickle
os.mkdir('/mnt/e/result/' + startDate)
SH = fr2[fr2['tag'] == 'SH'][["skey", "date", "num", 'sequenceNo', "clockAtArrival"]]
SH.to_pickle('/mnt/e/result/' + startDate + '/SH.pkl')
del SH

SZ = fr2[fr2['tag'] == 'SZ'][["skey", "date", "num", 'sequenceNo', "clockAtArrival"]]
SZ.to_pickle('/mnt/e/result/' + startDate + '/SZ.pkl')
del SZ

trade = fr2[fr2['tag'] == 'trade'][["skey", "date", "ApplSeqNum", 'sequenceNo', "clockAtArrival"]]
trade.to_pickle('/mnt/e/result/' + startDate + '/trade.pkl')
del trade

order = fr2[fr2['tag'] == 'order'][["skey", "date", "ApplSeqNum", 'sequenceNo', "clockAtArrival"]]
order.to_pickle('/mnt/e/result/' + startDate + '/order.pkl')
del order

index = fr2[fr2['tag'] == 'index'][["skey", "date", "num", 'sequenceNo', "clockAtArrival"]]
index.to_pickle('/mnt/e/result/' + startDate + '/index.pkl')
del index
del fr2

print(str(i) + 'finished')

