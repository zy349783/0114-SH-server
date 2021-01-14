import numpy as np
import pandas as pd
import pickle
from matplotlib import pyplot as plt
from matplotlib.ticker import Formatter
import collections
import glob
import os
import datetime

pd.set_option("max_columns", 200)

for y in ['20200812', '20200813', '20200814', '20200817', '20200821', '20200831', '20200901', '20200902']:
    print('----------------------------------------------------------------')
    print(y)

    re = {}
    for col in ['date', 'data', 'baseline', 'test', 'merge', 'time', 'stock_list']:
        re[col] = []

    print('----------------------------------------------------------------')
    print('SZ lv2 data:')

    readPath = 'F:\\data\\' + y + '\\***_zt_88_03_day_88data\\mdLog_SZ_***'
    dataPathLs = np.array(glob.glob(readPath))
    startTm = datetime.datetime.now()
    logSZ1 = pd.read_csv(dataPathLs[0])
    logSZ1["time"] = logSZ1["time"].apply(lambda x: int(x.replace(':', ""))).astype('int64') * 1000
    print(datetime.datetime.now() - startTm)

    logSZ1 = logSZ1.loc[:, ["clockAtArrival", "sequenceNo", "StockID", "source", "time", "cum_volume", "cum_amount", "close",
                            "bid1p", "bid2p", "bid3p", "bid4p", "bid5p", "bid1q",
                            "bid2q", "bid3q", "bid4q", "bid5q", "ask1p", "ask2p",
                            "ask3p", "ask4p", "ask5p", "ask1q", "ask2q", "ask3q",
                            "ask4q", "ask5q", "openPrice", "numTrades"]]
    logSZ1 = logSZ1[(logSZ1['StockID'] < 4000) | ((logSZ1['StockID'] > 300000) & (logSZ1['StockID'] < 310000))]

    readPath = 'F:\\data\\' + y + '\\***_zs_96_03_day_96data\\mdLog_SZ_***'
    dataPathLs = np.array(glob.glob(readPath))
    startTm = datetime.datetime.now()
    logSZ = pd.read_csv(dataPathLs[0])
    logSZ["time"] = logSZ["time"].apply(lambda x: int(x.replace(':', "").replace('.', "")))
    print(datetime.datetime.now() - startTm)

    startTm = datetime.datetime.now()
    data1 = logSZ[(logSZ["time"] >= 91500000) & (logSZ["time"] < 150000000) & (logSZ['source'] == 24)]
    data2 = logSZ1[(logSZ1["time"] >= 91500000) & (logSZ1["time"] < 150000000) & (logSZ1['source'] == 12)]

    columns = ["StockID", "cum_volume", "cum_amount", "close", "bid1p", "bid2p", "bid3p", "bid4p", "bid5p", "bid1q",
               "bid2q",
               "bid3q", "bid4q", "bid5q", "ask1p", "ask2p", "ask3p", "ask4p", "ask5p", "ask1q", "ask2q", "ask3q",
               "ask4q", "ask5q", "openPrice", "numTrades", "time"]
    data1_1 = data1.drop_duplicates(subset=columns, keep="first").reset_index()
    data2_1 = data2.drop_duplicates(subset=columns, keep="first").reset_index()

    data1_1['cum_amount'] = data1_1['cum_amount'].astype(str).apply(lambda x: x.split('.')[0]).astype('int64')

    n1 = len(data1_1["StockID"].unique())
    n2 = len(data2_1["StockID"].unique())
    print(n1)
    print(n2)
    print(len(set(data1_1["StockID"].unique()) - set(data2_1["StockID"].unique())))
    print(set(data1_1["StockID"].unique()) - set(data2_1["StockID"].unique()))
    if n1 != n2:
        sl = list(set(data1_1["StockID"].unique()) & set(data2_1["StockID"].unique()))
        data1_1 = data1_1[data1_1["StockID"].isin(sl)]
        data2_1 = data2_1[data2_1["StockID"].isin(sl)]
    for cols in ['close']:
        data1_1[cols] = data1_1[cols].round(2)
        data2_1[cols] = data2_1[cols].round(2)
    test = pd.merge(data1_1, data2_1, left_on=columns, right_on=columns, how="outer")
    print(datetime.datetime.now() - startTm)
    n1 = test["sequenceNo_x"].count()
    n2 = test["sequenceNo_y"].count()
    len1 = len(test)
    re['date'].append(y)
    re['data'].append('SZ lv2 data')
    re['baseline'].append(n1)
    re['test'].append(n2)
    re['merge'].append(len1)
    if (n1 == len1) & (n2 == len1):
        re['time'].append(0)
        re['stock_list'].append(0)
    print(n1)
    print(n2)
    print(len1)
    print("-----------------------------------------------")
    if n2 < len1:
        print("test is not complete:")
        print(test[np.isnan(test["sequenceNo_y"])])
        print(len(test[np.isnan(test["sequenceNo_y"])]) / n1)
        print(np.sort(test[np.isnan(test["sequenceNo_y"])]["time"].unique()))
        print(len(np.sort(test[np.isnan(test["sequenceNo_y"])]["StockID"].unique())))
        print(np.sort(test[np.isnan(test["sequenceNo_y"])]["StockID"].unique()))
        re['time'].append(np.sort(test[np.isnan(test["sequenceNo_y"])]["time"].unique()))
        re['stock_list'].append(np.sort(test[np.isnan(test["sequenceNo_y"])]["StockID"].unique()))
    if (len1 == n2) & (n1 < len1):
        print("baseline is not complete:")
        print(test[np.isnan(test["sequenceNo_x"])])
        print(n2 - n1)
        re['time'].append(np.sort(test[np.isnan(test["sequenceNo_x"])]["time"].unique()))
        re['stock_list'].append(np.sort(test[np.isnan(test["sequenceNo_x"])]["StockID"].unique()))
    del logSZ
    del logSZ1
    del data1
    del data2
    del test
    del data1_1
    del data2_1

    readPath = 'F:\\data\\' + y + '\\***_zt_88_03_day_88data\\mdOrderLog_***'
    dataPathLs = np.array(glob.glob(readPath))
    startTm = datetime.datetime.now()
    OrderLogSZ1 = pd.read_csv(dataPathLs[0])
    print(datetime.datetime.now() - startTm)

    readPath = 'F:\\data\\' + y + '\\***_zs_96_03_day_96data\\mdOrderLog_***'
    dataPathLs = np.array(glob.glob(readPath))
    startTm = datetime.datetime.now()
    OrderLogSZ = pd.read_csv(dataPathLs[0])
    print(datetime.datetime.now() - startTm)

    OrderLogSZ["OrderType"] = np.where(OrderLogSZ["OrderType"] == 2, '2', np.where(
        OrderLogSZ["OrderType"] == 1, '1', OrderLogSZ['OrderType']))

    OrderLogSZ1["OrderType"] = np.where(OrderLogSZ1["OrderType"] == 2, '2', np.where(
        OrderLogSZ1["OrderType"] == 1, '1', OrderLogSZ1['OrderType']))

    print(len(OrderLogSZ["SecurityID"].unique()))
    print(len(OrderLogSZ1["SecurityID"].unique()))
    print(len(set(OrderLogSZ["SecurityID"].unique()) - set(OrderLogSZ1["SecurityID"].unique())))
    print(set(OrderLogSZ["SecurityID"].unique()) - set(OrderLogSZ1["SecurityID"].unique()))

    sl = list(set(OrderLogSZ["SecurityID"].unique()) & set(OrderLogSZ1['SecurityID'].unique()))
    OrderLogSZ = OrderLogSZ[OrderLogSZ["SecurityID"].isin(sl)]
    OrderLogSZ1 = OrderLogSZ1[OrderLogSZ1["SecurityID"].isin(sl)]
    print(len(OrderLogSZ["SecurityID"].unique()))
    print(len(OrderLogSZ1["SecurityID"].unique()))

    print('----------------------------------------------------------------')
    print('SZ order data:')

    columns = ["ApplSeqNum", "TransactTime", "Side", 'OrderType', 'Price', 'OrderQty', "SecurityID"]
    ree = pd.merge(OrderLogSZ, OrderLogSZ1, on=columns, how="outer", validate='one_to_one')
    n1 = ree["sequenceNo_x"].count()
    n2 = ree["sequenceNo_y"].count()
    len1 = len(ree)
    print(n1)
    print(n2)
    print(len1)
    re['date'].append(y)
    re['data'].append('SZ order data')
    re['baseline'].append(n1)
    re['test'].append(n2)
    re['merge'].append(len1)
    if (n1 == len1) & (n2 == len1):
        re['time'].append(0)
        re['stock_list'].append(0)

    print("-----------------------------------------------")
    if n2 < len1:
        print("test is not complete:")
        print(ree[np.isnan(ree["sequenceNo_y"])])
        print(len(ree[np.isnan(ree["sequenceNo_y"])]))
        print(np.sort(ree[np.isnan(ree["sequenceNo_y"])]["TransactTime"].unique()))
        print(len(ree[np.isnan(ree["sequenceNo_y"])]["SecurityID"].unique()))
        print(ree[np.isnan(ree["sequenceNo_y"])]["SecurityID"].unique())
        re['time'].append(
            np.sort(ree[np.isnan(ree["sequenceNo_y"]) & (~ree["OrderType"].isnull())]["TransactTime"].unique()))
        re['stock_list'].append(
            np.sort(ree[np.isnan(ree["sequenceNo_y"]) & (~ree["OrderType"].isnull())]["SecurityID"].unique()))
    if (len1 == n2) & (n1 < len1):
        print("test is complete, baseline is not complete:")
        print(ree[np.isnan(ree["sequenceNo_x"])])
        print(np.sort(ree[np.isnan(ree["sequenceNo_x"])]["TransactTime"].unique()))
        print(len(ree[np.isnan(ree["sequenceNo_x"])]["SecurityID"].unique()))
        print(ree[np.isnan(ree["sequenceNo_x"])]["SecurityID"].unique())
        print(n2 - n1)
        re['time'].append(np.sort(ree[np.isnan(ree["sequenceNo_x"])]["TransactTime"].unique()))
        re['stock_list'].append(np.sort(ree[np.isnan(ree["sequenceNo_x"])]["SecurityID"].unique()))
    del OrderLogSZ
    del OrderLogSZ1
    del ree


    readPath = 'F:\\data\\' + y + '\\***_zt_88_03_day_88data\\mdTradeLog_***'
    dataPathLs = np.array(glob.glob(readPath))

    startTm = datetime.datetime.now()
    TradeLogSZ1 = pd.read_csv(dataPathLs[0])
    print(datetime.datetime.now() - startTm)
    TradeLogSZ1["TradeBSFlag"] = 'N'


    readPath = 'F:\\data\\' + y + '\\***_zs_96_03_day_96data\\mdTradeLog_***'
    dataPathLs = np.array(glob.glob(readPath))

    startTm = datetime.datetime.now()
    TradeLogSZ = pd.read_csv(dataPathLs[0])
    print(datetime.datetime.now() - startTm)
    TradeLogSZ["TradeBSFlag"] = 'N'

    print(len(TradeLogSZ["SecurityID"].unique()))
    print(len(TradeLogSZ1["SecurityID"].unique()))
    print(len(set(TradeLogSZ["SecurityID"].unique()) - set(TradeLogSZ1["SecurityID"].unique())))
    print(set(TradeLogSZ["SecurityID"].unique()) - set(TradeLogSZ1["SecurityID"].unique()))

    sl = list(set(TradeLogSZ["SecurityID"].unique()) & set(TradeLogSZ1['SecurityID'].unique()))
    TradeLogSZ = TradeLogSZ[TradeLogSZ["SecurityID"].isin(sl)]
    TradeLogSZ1 = TradeLogSZ1[TradeLogSZ1["SecurityID"].isin(sl)]
    print(len(TradeLogSZ["SecurityID"].unique()))
    print(len(TradeLogSZ1["SecurityID"].unique()))

    print('----------------------------------------------------------------')
    print('SZ trade data:')

    TradeLogSZ["ExecType"] = TradeLogSZ["ExecType"].apply(lambda x: str(x))
    TradeLogSZ1["ExecType"] = TradeLogSZ1["ExecType"].apply(lambda x: str(x))
    columns = ["TransactTime", "ApplSeqNum", "SecurityID", "ExecType", "TradeBSFlag", "TradePrice", "TradeQty",
               "TradeMoney", "BidApplSeqNum", "OfferApplSeqNum"]
    ree = pd.merge(TradeLogSZ, TradeLogSZ1, left_on=columns, right_on=columns, how="outer", validate='one_to_one')
    n1 = ree["sequenceNo_x"].count()
    n2 = ree["sequenceNo_y"].count()
    len1 = len(ree)
    re['date'].append(y)
    re['data'].append('SZ trade data')
    re['baseline'].append(n1)
    re['test'].append(n2)
    re['merge'].append(len1)
    if (n1 == len1) & (n2 == len1):
        re['time'].append(0)
        re['stock_list'].append(0)
    print(n1)
    print(n2)
    print(len1)
    print("-----------------------------------------------")
    if n2 < len1:
        print("test is not complete:")
        print(ree[np.isnan(ree["sequenceNo_y"])])
        print(len(ree[np.isnan(ree["sequenceNo_y"])]))
        print(np.sort(ree[np.isnan(ree["sequenceNo_y"])]["TransactTime"].unique()))
        print(len(ree[np.isnan(ree["sequenceNo_y"])]["SecurityID"].unique()))
        print(ree[np.isnan(ree["sequenceNo_y"])]["SecurityID"].unique())
        re['time'].append(np.sort(ree[np.isnan(ree["sequenceNo_y"])]["TransactTime"].unique()))
        re['stock_list'].append(ree[np.isnan(ree["sequenceNo_y"])]["SecurityID"].unique())
    if (len1 == n2) & (n1 < len1):
        print("baseline is not complete:")
        print(ree[np.isnan(ree["sequenceNo_x"])])
        print(np.sort(ree[np.isnan(ree["sequenceNo_x"])]["TransactTime"].unique()))
        print(len(ree[np.isnan(ree["sequenceNo_x"])]["SecurityID"].unique()))
        print(ree[np.isnan(ree["sequenceNo_x"])]["SecurityID"].unique())
        print(n2 - n1)
        re['time'].append(np.sort(ree[np.isnan(ree["sequenceNo_x"])]["TransactTime"].unique()))
        re['stock_list'].append(ree[np.isnan(ree["sequenceNo_x"])]["SecurityID"].unique())
    del TradeLogSZ
    del TradeLogSZ1
    del ree

    re = pd.DataFrame(re)
    re.to_csv('D:\\work\\project 7 snapshot data\\zt_88_03\\' + y + '.csv')
