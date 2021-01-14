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

for y in ['20200903']:
    print('----------------------------------------------------------------')
    print(y)

    re = {}
    for col in ['date', 'data', 'baseline', 'test', 'merge', 'time', 'stock_list']:
        re[col] = []

    readPath = 'F:\\data\\' + y + '\\***_zt_96_04_day_96data\\mdLog_SH_***'
    dataPathLs = np.array(glob.glob(readPath))
    startTm = datetime.datetime.now()
    logSH1 = pd.read_csv(dataPathLs[0])
    print(datetime.datetime.now() - startTm)

    logSH1 = logSH1[["sequenceNo", "StockID", "source", "time", "cum_volume", "cum_amount", "close",
                     "bid1p", "bid2p", "bid3p", "bid4p", "bid5p", "bid1q", "bid2q", "bid3q",
                     "bid4q", "bid5q", "ask1p", "ask2p", "ask3p", "ask4p", "ask5p", "ask1q",
                     "ask2q", "ask3q", "ask4q", "ask5q", "openPrice", "numTrades"]]
    logSH1["time"] = logSH1["time"].apply(lambda x: int(x.replace(':', "").replace('.', "")))

    readPath = 'F:\\data\\' + y + '\\***_zt_88_03_day_88data\\mdLog_SH_***'
    dataPathLs = np.array(glob.glob(readPath))
    startTm = datetime.datetime.now()
    logSH2 = pd.read_csv(dataPathLs[0])
    logSH2 = logSH2[["sequenceNo", "StockID", "source", "time", "cum_volume", "cum_amount", "close",
                     "bid1p", "bid2p", "bid3p", "bid4p", "bid5p", "bid1q", "bid2q", "bid3q",
                     "bid4q", "bid5q", "ask1p", "ask2p", "ask3p", "ask4p", "ask5p", "ask1q",
                     "ask2q", "ask3q", "ask4q", "ask5q", "openPrice", "numTrades"]]
    logSH2["time"] = logSH2["time"].apply(lambda x: int(x.replace(':', "").replace('.', "")))

    # print(datetime.datetime.now() - startTm)
    # print('----------------------------------------------------------------')
    # print('SH lv2 data:')
    # in_dex = [16, 300, 852, 905]
    # data1 = logSH2[~logSH2["StockID"].isin(in_dex) & (logSH2["time"] >= 91500000) & (logSH2["time"] <= 150000000)
    # & (logSH2['source'] == 23)]
    # data2 = logSH1[~logSH1["StockID"].isin(in_dex) & (logSH1["time"] >= 91500000) & (logSH1["time"] <= 150000000) & (
    #             logSH1['source'] == 13)]
    # columns = ["StockID", "cum_volume", "cum_amount", "close", "bid1p", "bid2p", "bid3p", "bid4p", "bid5p", "bid1q",
    #            "bid2q",
    #            "bid3q", "bid4q", "bid5q", "ask1p", "ask2p", "ask3p", "ask4p", "ask5p", "ask1q", "ask2q", "ask3q",
    #            "ask4q", "ask5q", "openPrice", "numTrades", "time"]
    # data1_1 = data1.drop_duplicates(subset=columns, keep="first").reset_index()
    # data2_1 = data2.drop_duplicates(subset=columns, keep="first").reset_index()
    #
    # n1 = len(data1_1["StockID"].unique())
    # n2 = len(data2_1["StockID"].unique())
    # print(n1)
    # print(n2)
    # print(len(set(data1_1["StockID"].unique()) - set(data2_1["StockID"].unique())))
    # print(set(data1_1["StockID"].unique()) - set(data2_1["StockID"].unique()))
    #
    # if n1 != n2:
    #     sl = list(set(data1_1["StockID"].unique()) & set(data2_1["StockID"].unique()))
    #     data1_1 = data1_1[data1_1["StockID"].isin(sl)]
    #     data2_1 = data2_1[data2_1["StockID"].isin(sl)]
    #
    # data2_1['cum_amount'] = data2_1['cum_amount'].round(2)
    # data1_1['cum_amount'] = data1_1['cum_amount'].round(2)
    # data1_1['openPrice'] = data1_1.groupby('StockID')['openPrice'].transform('max')
    # data2_1['openPrice'] = data2_1.groupby('StockID')['openPrice'].transform('max')
    #
    # data2_1 = data2_1[~data2_1['bid1p'].isnull()]
    # test = pd.merge(data1_1, data2_1, left_on=columns, right_on=columns, how="outer")
    # n1 = test["sequenceNo_x"].count()
    # n2 = test["sequenceNo_y"].count()
    # len1 = len(test)
    # re['date'].append(y)
    # re['data'].append('SH lv2 data')
    # re['baseline'].append(n1)
    # re['test'].append(n2)
    # re['merge'].append(len1)
    # if (n1 == len1) & (n2 == len1):
    #     re['time'].append(0)
    #     re['stock_list'].append(0)
    # print(n1)
    # print(n2)
    # print(len1)
    # print("-----------------------------------------------")
    # if n2 < len1:
    #     print("test is not complete:")
    #     print(test[np.isnan(test["sequenceNo_y"])])
    #     print(len(test[np.isnan(test["sequenceNo_y"])]) / n1)
    #     print(len(test[np.isnan(test["sequenceNo_y"])]["time"].unique()))
    #     print(test[np.isnan(test["sequenceNo_y"])]["time"].unique())
    #     print(len(test[np.isnan(test["sequenceNo_y"])]["StockID"].unique()))
    #     print(test[np.isnan(test["sequenceNo_y"])]["StockID"].unique())
    #     re['time'].append(np.sort(test[np.isnan(test["sequenceNo_y"])]["time"].unique()))
    #     re['stock_list'].append(np.sort(test[np.isnan(test["sequenceNo_y"])]["StockID"].unique()))
    # if (len1 == n2) & (n1 < len1):
    #     print("baseline is not complete:")
    #     print(test[np.isnan(test["sequenceNo_x"])])
    #     print(n2 - n1)
    #     re['time'].append(np.sort(test[np.isnan(test["sequenceNo_x"])]["time"].unique()))
    #     re['stock_list'].append(np.sort(test[np.isnan(test["sequenceNo_x"])]["StockID"].unique()))
    #     print((n2 - n1) / n1)
    # del data1
    # del data2
    # del test
    # del data1_1
    # del data2_1

    print('----------------------------------------------------------------')
    print('SH lv1 data:')
    in_dex = [16, 300, 852, 905]
    data1 = logSH2[~logSH2["StockID"].isin(in_dex) & (
                logSH2['source'] == 22)]
    data2 = logSH1[~logSH1["StockID"].isin(in_dex) & (
                logSH1['source'] == 9)]
    columns = ["StockID", "cum_volume", "cum_amount", "close", "bid1p", "bid2p", "bid3p", "bid4p", "bid5p", "bid1q",
               "bid2q",  "bid3q", "bid4q", "bid5q", "ask1p", "ask2p", "ask3p", "ask4p", "ask5p", "ask1q", "ask2q", "ask3q",
               "ask4q", "ask5q", "openPrice"]
    data1_1 = data1.drop_duplicates(subset=columns, keep="first").reset_index()
    data2_1 = data2.drop_duplicates(subset=columns, keep="first").reset_index()

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
    data1_1['cum_amount'] = data1_1['cum_amount'].round(2)
    data2_1['cum_amount'] = data2_1['cum_amount'].round(2)

    data2_1 = data2_1[(data2_1['bid1p'] != 0) | (data2_1['ask1p'] != 0) | (data2_1['cum_volume'] != 0)]
    data1_1 = data1_1[(data1_1['bid1p'] != 0) | (data1_1['ask1p'] != 0) | (data1_1['cum_volume'] != 0)]

    test = pd.merge(data1_1, data2_1, left_on=columns, right_on=columns, how="outer")
    n1 = test["sequenceNo_x"].count()
    n2 = test["sequenceNo_y"].count()
    len1 = len(test)
    re['date'].append(y)
    re['data'].append('SH lv1 data')
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
        print(test[np.isnan(test["sequenceNo_x"])])
        print(len(test[np.isnan(test["sequenceNo_x"])]) / n1)
        print(len(test[np.isnan(test["sequenceNo_x"])]["time"].unique()))
        print(test[np.isnan(test["sequenceNo_x"])]["time"].unique())
        print(len(test[np.isnan(test["sequenceNo_x"])]["StockID"].unique()))
        print(test[np.isnan(test["sequenceNo_x"])]["StockID"].unique())
        re['time'].append(np.sort(test[np.isnan(test["sequenceNo"])]["time"].unique()))
        re['stock_list'].append(np.sort(test[np.isnan(test["sequenceNo"])]["StockID"].unique()))
    if (len1 == n2) & (n1 < len1):
        print("baseline is not complete:")
        print(test[np.isnan(test["sequenceNo_y"])])
        print(n2 - n1)
        re['time'].append(np.sort(test[np.isnan(test["sequenceNo_y"])]["time"].unique()))
        re['stock_list'].append(np.sort(test[np.isnan(test["sequenceNo_y"])]["StockID"].unique()))
        print((n2 - n1) / n1)
    del data1
    del data2
    del test
    del data1_1
    del data2_1

    print('----------------------------------------------------------------')
    print('SH index data:')

    data1 = logSH2[(logSH2["StockID"].isin(in_dex)) & (logSH2["time"] >= 91500000) & (logSH2["time"] <= 150000000)]
    data2 = logSH1[(logSH1["StockID"].isin(in_dex)) & (logSH1["time"] >= 91500000) & (logSH1["time"] <= 150000000) & (logSH1['source'] == 13)]

    columns = ["StockID", "cum_volume", "cum_amount", "close", "openPrice"]
    data1_1 = data1.drop_duplicates(subset=columns, keep="first").reset_index()
    data2_1 = data2.drop_duplicates(subset=columns, keep="first").reset_index()

    for cols in ['close', 'openPrice']:
        data1_1[cols] = data1_1[cols].round(4)
        data2_1[cols] = data2_1[cols].round(4)
    for cols in ['cum_amount']:
        data1_1[cols] = data1_1[cols].round(1)
        data2_1[cols] = data2_1[cols].round(1)

    test = pd.merge(data1_1, data2_1, left_on=columns, right_on=columns, how="outer")
    n1 = test["sequenceNo_x"].count()
    n2 = test["sequenceNo_y"].count()
    len1 = len(test)
    print(n1)
    print(n2)
    print(len1)
    re['date'].append(y)
    re['data'].append('SH index data without time column')
    re['baseline'].append(n1)
    re['test'].append(n2)
    re['merge'].append(len1)
    if (n1 == len1) & (n2 == len1):
        re['time'].append(0)
        re['stock_list'].append(0)
    if n2 < len1:
        print("test is not complete:")
        print(test[np.isnan(test["sequenceNo_y"])])
        re['time'].append(np.sort(test[np.isnan(test['sequenceNo_y'])]['time_x'].unique()))
        re['stock_list'].append(np.sort(test[np.isnan(test['sequenceNo_y'])]['StockID'].unique()))
    if (n2 == len1) & (n1 < len1):
        print("baseline is not complete::")
        print(test[np.isnan(test["sequenceNo_x"])])
        re['time'].append(np.sort(test[np.isnan(test["sequenceNo_x"])]['time_y'].unique()))
        re['stock_list'].append(np.sort(test[np.isnan(test['sequenceNo_x'])]['StockID'].unique()))

    del index
    del logSH
    del data1
    del data2
    del test
    del data1_1
    del data2_1






    readPath = 'F:\\data\\' + y + '\\***_zs_96_03_day_96data\\mdTradeLog_***'
    dataPathLs = np.array(glob.glob(readPath))

    startTm = datetime.datetime.now()
    SH1 = pd.read_csv(dataPathLs[0])
    print(datetime.datetime.now() - startTm)

    readPath = 'F:\\data\\' + y + '\\***_zt_88_03_day_88data\\mdTradeLog_***'
    dataPathLs = np.array(glob.glob(readPath))

    startTm = datetime.datetime.now()
    SH = pd.read_csv(dataPathLs[0])
    print(datetime.datetime.now() - startTm)

    print(len(SH["SecurityID"].unique()))
    print(len(SH1["SecurityID"].unique()))
    print(len(set(SH["SecurityID"].unique()) - set(SH1["SecurityID"].unique())))
    print(set(SH["SecurityID"].unique()) - set(SH1["SecurityID"].unique()))

    sl = list(set(SH["SecurityID"].unique()) & set(SH1['SecurityID'].unique()))
    SH = SH[SH["SecurityID"].isin(sl)]
    SH1 = SH1[SH1["SecurityID"].isin(sl)]
    print(len(SH["SecurityID"].unique()))
    print(len(SH1["SecurityID"].unique()))

    print(SH1.columns)

    print('----------------------------------------------------------------')
    print('SH trade data:')

    SH["ExecType"] = 'F'
    SH1["ExecType"] = 'F'
    columns = ["TransactTime", "ApplSeqNum", "SecurityID", "TradePrice", "TradeQty", "TradeMoney", "TradeBSFlag",
               "ExecType",  "BidApplSeqNum", "OfferApplSeqNum"]
    ree = pd.merge(SH, SH1, left_on=columns, right_on=columns, how="outer", validate='one_to_one')
    n1 = ree["sequenceNo_x"].count()
    n2 = ree["sequenceNo_y"].count()
    len1 = len(ree)
    print(n1)
    print(n2)
    print(len1)
    re['date'].append(y)
    re['data'].append('SH trade data')
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
        re['time'].append(np.sort(ree[np.isnan(ree["sequenceNo_y"])]["TransactTime"].unique()))
        re['stock_list'].append(np.sort(ree[np.isnan(ree["sequenceNo_y"])]["SecurityID"].unique()))
    if (len1 == n2) & (n1 < len1):
        print("baseline is not complete:")
        print(ree[np.isnan(ree["sequenceNo_x"])])
        print(np.sort(ree[np.isnan(ree["sequenceNo_x"])]["TransactTime"].unique()))
        print(len(ree[np.isnan(ree["sequenceNo_x"])]["SecurityID"].unique()))
        print(ree[np.isnan(ree["sequenceNo_x"])]["SecurityID"].unique())
        print(n2 - n1)
        re['time'].append(np.sort(ree[np.isnan(ree["sequenceNo_x"])]["TransactTime"].unique()))
        re['stock_list'].append(np.sort(ree[np.isnan(ree["sequenceNo_x"])]["SecurityID"].unique()))
    del SH
    del SH1
    del ree


    re = pd.DataFrame(re)
    re.to_csv('D:\\work\\project 7 snapshot data\\zs_96_03\\' + y + '.csv')