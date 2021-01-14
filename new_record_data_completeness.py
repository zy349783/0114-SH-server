#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri Jul 10 14:53:31 2020

@author: work516
"""

# !/usr/bin/env python
# coding: utf-8


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


def new_record_data_check():
    y = '20200806'
    print('----------------------------------------------------------------')
    print(y)
    #
    # com = {}
    # for col in ['tag', 'complete']:
    #     com[col] = []
    #
    # readPath = 'F:\\data\\' + y + '\\logs_' + y + '_zt_***_day_pcap\\mdL2Pcap_SH_***'
    # dataPathLs = np.array(glob.glob(readPath))
    # print(dataPathLs[0])
    # print(dataPathLs[1])
    # logSH1 = pd.read_csv(dataPathLs[0])
    #
    # logSH1["StockID"] = logSH1["ID"] - 1000000
    # logSH1 = logSH1[["sequenceNo", "StockID", "time", "cum_volume", "cum_amount", "close",
    #                  "bid1p", "bid2p", "bid3p", "bid4p", "bid5p", "bid1q", "bid2q", "bid3q",
    #                  "bid4q", "bid5q", "ask1p", "ask2p", "ask3p", "ask4p", "ask5p", "ask1q",
    #                  "ask2q", "ask3q", "ask4q", "ask5q", "open", "cum_tradesCnt"]]
    # logSH1 = logSH1.rename(columns={"open": "openPrice", "cum_tradesCnt": "numTrades"})
    # logSH1 = logSH1[(logSH1['StockID'] >= 600000) & (logSH1['StockID'] < 700000)]
    #
    # logSH2 = pd.read_csv(dataPathLs[1])
    # logSH2["StockID"] = logSH2["ID"] - 1000000
    # logSH2 = logSH2[["sequenceNo", "StockID", "time", "cum_volume", "cum_amount", "close",
    #                  "bid1p", "bid2p", "bid3p", "bid4p", "bid5p", "bid1q", "bid2q", "bid3q",
    #                  "bid4q", "bid5q", "ask1p", "ask2p", "ask3p", "ask4p", "ask5p", "ask1q",
    #                  "ask2q", "ask3q", "ask4q", "ask5q", "open", "cum_tradesCnt"]]
    # logSH2 = logSH2.rename(columns={"open": "openPrice", "cum_tradesCnt": "numTrades"})
    # logSH2 = logSH2[(logSH2['StockID'] >= 600000) & (logSH2['StockID'] < 700000)]
    #
    # print('----------------------------------------------------------------')
    # print('SH lv2 data:')
    # in_dex = [16, 300, 852, 905]
    # data1 = logSH2[~logSH2["StockID"].isin(in_dex) & (logSH2["time"] >= 91500000) & (logSH2["time"] <= 150000000)]
    # data2 = logSH1[~logSH1["StockID"].isin(in_dex) & (logSH1["time"] >= 91500000) & (logSH1["time"] <= 150000000)]
    # columns = ["StockID", "cum_volume", "cum_amount", "close", "bid1p", "bid2p", "bid3p", "bid4p", "bid5p", "bid1q",
    #            "bid2q",
    #            "bid3q", "bid4q", "bid5q", "ask1p", "ask2p", "ask3p", "ask4p", "ask5p", "ask1q", "ask2q", "ask3q",
    #            "ask4q", "ask5q", "openPrice", "time", "numTrades"]
    # data1_1 = data1.drop_duplicates(subset=columns, keep="first").reset_index()
    # data2_1 = data2.drop_duplicates(subset=columns, keep="first").reset_index()
    #
    # n1 = len(data1_1["StockID"].unique())
    # n2 = len(data2_1["StockID"].unique())
    #
    # if n1 != n2:
    #     sl = list(set(data1_1["StockID"].unique()) & set(data2_1["StockID"].unique()))
    #     data1_1 = data1_1[data1_1["StockID"].isin(sl)]
    #     data2_1 = data2_1[data2_1["StockID"].isin(sl)]
    #
    # test = pd.merge(data1_1, data2_1, left_on=columns, right_on=columns, how="outer")
    # n1 = test["sequenceNo_x"].count()
    # n2 = test["sequenceNo_y"].count()
    # len1 = len(test)
    # com['tag'].append('SH snapshot')
    # if (n1 == len1) & (n2 < len1):
    #     print(dataPathLs[0].split('\\')[4][14:22] + " is not complete:")
    #     print(test[np.isnan(test["sequenceNo_y"])])
    #     print(len(test[np.isnan(test["sequenceNo_y"])]) / n1)
    #     print(len(test[np.isnan(test["sequenceNo_y"])]["time"].unique()))
    #     print(test[np.isnan(test["sequenceNo_y"])]["time"].unique())
    #     print(len(test[np.isnan(test["sequenceNo_y"])]["StockID"].unique()))
    #     print(test[np.isnan(test["sequenceNo_y"])]["StockID"].unique())
    #     com['complete'].append(dataPathLs[1].split('\\')[4][14:22])
    # elif (n2 == len1) & (n1 < len1):
    #     print(dataPathLs[1].split('\\')[4][14:22] + " is not complete:")
    #     print(test[np.isnan(test["sequenceNo_x"])])
    #     print(n2 - n1)
    #     print((n2 - n1) / n1)
    #     com['complete'].append(dataPathLs[0].split('\\')[4][14:22])
    # elif (n2 == len1) & (n1 == len1):
    #     com['complete'].append('zt_88_03')
    # else:
    #     com['complete'].append('nan')
    #
    # del logSH1
    # del data1
    # del data2
    # del test
    # del data1_1
    # del data2_1
    # del logSH2
    #
    # print('----------------------------------------------------------------')
    # print('SH index data:')
    #
    # readPath = 'F:\\data\\' + y + '\\logs_' + y + '_zt_***_day_pcap\\mdIndexPcap_SH_***'
    # dataPathLs = np.array(glob.glob(readPath))
    #
    # index = pd.read_csv(dataPathLs[0])
    #
    # index["StockID"] = index["ID"] - 1000000
    # index = index.rename(columns={"open": "openPrice"})
    #
    # logSH = pd.read_csv(dataPathLs[1])
    #
    # logSH["StockID"] = logSH["ID"] - 1000000
    # logSH = logSH.rename(columns={"open": "openPrice"})
    #
    # in_dex = [16, 300, 852, 905]
    # index = index[index["StockID"].isin(in_dex)]
    #
    # data1 = logSH[(logSH["StockID"].isin(in_dex)) & (logSH["time"] >= 91500000) & (logSH["time"] <= 150000000)]
    # data2 = index[(index["time"] >= 91500000) & (index["time"] <= 150000000)]
    #
    # columns = ["StockID", "cum_volume", "cum_amount", "close", "openPrice", 'time']
    # data1_1 = data1.drop_duplicates(subset=columns, keep="first").reset_index()
    # data2_1 = data2.drop_duplicates(subset=columns, keep="first").reset_index()
    #
    # test = pd.merge(data1_1, data2_1, left_on=columns, right_on=columns, how="outer")
    # n1 = test["sequenceNo_x"].count()
    # n2 = test["sequenceNo_y"].count()
    # len1 = len(test)
    # com['tag'].append("SH index")
    #
    # if (n1 == len1) & (n2 < len1):
    #     print(dataPathLs[0].split('\\')[4][14:22] + " is not complete:")
    #     print(test[np.isnan(test["sequenceNo_y"])])
    #     com['complete'].append(dataPathLs[1].split('\\')[4][14:22])
    # elif (n2 == len1) & (n1 < len1):
    #     print(dataPathLs[1].split('\\')[4][14:22] + " is not complete:")
    #     print(test[np.isnan(test["sequenceNo_x"])])
    #     com['complete'].append(dataPathLs[0].split('\\')[4][14:22])
    # elif (n2 == len1) & (n1 == len1):
    #     com['complete'].append("zt_88_03")
    # else:
    #     com['complete'].append("nan")
    # del index
    # del data1
    # del data2
    # del test
    # del data1_1
    # del data2_1
    # del logSH
    #
    # print('----------------------------------------------------------------')
    # print('SZ lv2 data:')
    #
    # readPath = 'F:\\data\\' + y + '\\logs_' + y + '_zs_***_day_pcap\\mdL2Pcap_SZ_***'
    # dataPathLs = np.array(glob.glob(readPath))
    #
    # logSZ1 = pd.read_csv(dataPathLs[0])
    #
    # logSZ1 = logSZ1.loc[:, ["clockAtArrival", "sequenceNo", "ID", "time", "cum_volume", "cum_amount", "close",
    #                         "bid1p", "bid2p", "bid3p", "bid4p", "bid5p", "bid1q",
    #                         "bid2q", "bid3q", "bid4q", "bid5q", "ask1p", "ask2p",
    #                         "ask3p", "ask4p", "ask5p", "ask1q", "ask2q", "ask3q",
    #                         "ask4q", "ask5q", "open", "cum_tradesCnt"]]
    # logSZ1 = logSZ1.rename(columns={"open": "openPrice", "cum_tradesCnt": "numTrades"})
    # logSZ1["StockID"] = logSZ1["ID"] - 2000000
    # logSZ1 = logSZ1[(logSZ1['StockID'] < 4000) | ((logSZ1['StockID'] > 300000) & (logSZ1['StockID'] < 310000))]
    #
    # logSZ = pd.read_csv(dataPathLs[1])
    #
    # logSZ = logSZ.loc[:, ["clockAtArrival", "sequenceNo", "ID", "time", "cum_volume", "cum_amount", "close",
    #                       "bid1p", "bid2p", "bid3p", "bid4p", "bid5p", "bid1q",
    #                       "bid2q", "bid3q", "bid4q", "bid5q", "ask1p", "ask2p",
    #                       "ask3p", "ask4p", "ask5p", "ask1q", "ask2q", "ask3q",
    #                       "ask4q", "ask5q", "open", "cum_tradesCnt"]]
    # logSZ = logSZ.rename(columns={"open": "openPrice", "cum_tradesCnt": "numTrades"})
    # logSZ["StockID"] = logSZ["ID"] - 2000000
    # logSZ = logSZ[(logSZ['StockID'] < 4000) | ((logSZ['StockID'] > 300000) & (logSZ['StockID'] < 310000))]
    #
    # data1 = logSZ[(logSZ["time"] >= 91500000) & (logSZ["time"] < 150000000)]
    # data2 = logSZ1[(logSZ1["time"] >= 91500000) & (logSZ1["time"] < 150000000)]
    #
    # columns = ["StockID", "cum_volume", "cum_amount", "close", "bid1p", "bid2p", "bid3p", "bid4p", "bid5p", "bid1q",
    #            "bid2q",
    #            "bid3q", "bid4q", "bid5q", "ask1p", "ask2p", "ask3p", "ask4p", "ask5p", "ask1q", "ask2q", "ask3q",
    #            "ask4q", "ask5q", "openPrice", "numTrades", "time"]
    # data1_1 = data1.drop_duplicates(subset=columns, keep="first").reset_index()
    # data2_1 = data2.drop_duplicates(subset=columns, keep="first").reset_index()
    #
    # n1 = len(data1_1["StockID"].unique())
    # n2 = len(data2_1["StockID"].unique())
    # if n1 != n2:
    #     sl = list(set(data1_1["StockID"].unique()) & set(data2_1["StockID"].unique()))
    #     data1_1 = data1_1[data1_1["StockID"].isin(sl)]
    #     data2_1 = data2_1[data2_1["StockID"].isin(sl)]
    #
    # test = pd.merge(data1_1, data2_1, left_on=columns, right_on=columns, how="outer")
    # n1 = test["sequenceNo_x"].count()
    # n2 = test["sequenceNo_y"].count()
    # len1 = len(test)
    #
    # com['tag'].append("SZ snapshot")
    # if (n1 == len1) & (n2 < len1):
    #     print(dataPathLs[0].split('\\')[4][14:22] + " is not complete:")
    #     print(test[np.isnan(test["sequenceNo_y"])])
    #     print(len(test[np.isnan(test["sequenceNo_y"])]) / n1)
    #     print(np.sort(test[np.isnan(test["sequenceNo_y"])]["time"].unique()))
    #     print(len(np.sort(test[np.isnan(test["sequenceNo_y"])]["StockID"].unique())))
    #     print(np.sort(test[np.isnan(test["sequenceNo_y"])]["StockID"].unique()))
    #     com['complete'].append(dataPathLs[1].split('\\')[4][14:22])
    # elif (n2 == len1) & (n1 < len1):
    #     print(dataPathLs[1].split('\\')[4][14:22] + " is not complete:")
    #     print(test[np.isnan(test["sequenceNo_x"])])
    #     print(n2 - n1)
    #     com['complete'].append(dataPathLs[0].split('\\')[4][14:22])
    # elif (n2 == len1) & (n1 == len1):
    #     com['complete'].append("zs_96_03")
    # else:
    #     com['complete'].append("nan")
    # del logSZ1
    # del data1
    # del data2
    # del test
    # del data1_1
    # del data2_1
    # del logSZ
    #
    # readPath = 'F:\\data\\' + y + '\\logs_' + y + '_zs_***_day_pcap\\mdOrderPcap_SZ_***'
    # dataPathLs = np.array(glob.glob(readPath))
    #
    # OrderLogSZ1 = pd.read_csv(dataPathLs[0])
    #
    # OrderLogSZ1["SecurityID"] = OrderLogSZ1["ID"] - 2000000
    # OrderLogSZ1 = OrderLogSZ1.rename(columns={"time": 'TransactTime'})
    #
    # OrderLogSZ1["OrderType"] = np.where(OrderLogSZ1["OrderType"] == 2, '2', np.where(
    #     OrderLogSZ1["OrderType"] == 1, '1', OrderLogSZ1['OrderType']))
    # OrderLogSZ1 = OrderLogSZ1[(OrderLogSZ1['SecurityID'] < 4000) | ((OrderLogSZ1['SecurityID'] > 300000)
    #                                                                 & (OrderLogSZ1['SecurityID'] < 310000))]
    #
    # OrderLogSZ = pd.read_csv(dataPathLs[1])
    #
    # OrderLogSZ["SecurityID"] = OrderLogSZ["ID"] - 2000000
    # OrderLogSZ = OrderLogSZ.rename(columns={"time": 'TransactTime'})
    #
    # OrderLogSZ["OrderType"] = np.where(OrderLogSZ["OrderType"] == 2, '2', np.where(
    #     OrderLogSZ["OrderType"] == 1, '1', OrderLogSZ['OrderType']))
    # OrderLogSZ = OrderLogSZ[(OrderLogSZ['SecurityID'] < 4000) | ((OrderLogSZ['SecurityID'] > 300000)
    #                                                              & (OrderLogSZ['SecurityID'] < 310000))]
    #
    # sl = list(set(OrderLogSZ["SecurityID"].unique()) & set(OrderLogSZ1['SecurityID'].unique()))
    # OrderLogSZZ = OrderLogSZ[OrderLogSZ["SecurityID"].isin(sl)]
    # OrderLogSZ1 = OrderLogSZ1[OrderLogSZ1["SecurityID"].isin(sl)]
    #
    # print('----------------------------------------------------------------')
    # print('SZ order data:')
    #
    # columns = ["ApplSeqNum", "TransactTime", "Side", 'OrderType', 'Price', 'OrderQty', "SecurityID"]
    # ree = pd.merge(OrderLogSZZ, OrderLogSZ1, on=columns, how="outer", validate='one_to_one')
    # n1 = ree["sequenceNo_x"].count()
    # n2 = ree["sequenceNo_y"].count()
    # len1 = len(ree)
    # com['tag'].append("SZ order")
    # if (n1 == len1) & (n2 < len1):
    #     print(dataPathLs[0].split('\\')[4][14:22] + " is not complete:")
    #     print(ree[np.isnan(ree["sequenceNo_y"])])
    #     print(len(ree[np.isnan(ree["sequenceNo_y"])]))
    #     print(np.sort(ree[np.isnan(ree["sequenceNo_y"])]["TransactTime"].unique()))
    #     print(len(ree[np.isnan(ree["sequenceNo_y"])]["SecurityID"].unique()))
    #     print(ree[np.isnan(ree["sequenceNo_y"])]["SecurityID"].unique())
    #     com['complete'].append(dataPathLs[1].split('\\')[4][14:22])
    # elif (n2 == len1) & (n1 < len1):
    #     print(dataPathLs[1].split('\\')[4][14:22] + " is not complete:")
    #     print(ree[np.isnan(ree["sequenceNo_x"])])
    #     print(np.sort(ree[np.isnan(ree["sequenceNo_x"])]["TransactTime"].unique()))
    #     print(len(ree[np.isnan(ree["sequenceNo_x"])]["SecurityID"].unique()))
    #     print(ree[np.isnan(ree["sequenceNo_x"])]["SecurityID"].unique())
    #     print(n2 - n1)
    #     com['complete'].append(dataPathLs[0].split('\\')[4][14:22])
    # elif (n2 == len1) & (n1 == len1):
    #     com['complete'].append("zs_96_03")
    # else:
    #     com['complete'].append("nan")
    # del OrderLogSZ1
    # del OrderLogSZZ
    # del ree
    # del OrderLogSZ
    #
    # readPath = 'F:\\data\\' + y + '\\logs_' + y + '_zt_***_day_pcap\\mdTradePcap_SH_***'
    # dataPathLs = np.array(glob.glob(readPath))
    #
    # SH1 = pd.read_csv(dataPathLs[0])
    #
    # SH1["SecurityID"] = SH1["ID"] - 1000000
    # SH1 = SH1.rename(columns={"time": 'TransactTime'})
    # SH1 = SH1[(SH1['SecurityID'] >= 600000) & (SH1['SecurityID'] <= 700000)]
    #
    # SH = pd.read_csv(dataPathLs[1])
    #
    # SH["SecurityID"] = SH["ID"] - 1000000
    # SH = SH.rename(columns={"time": 'TransactTime'})
    # SH = SH[(SH['SecurityID'] >= 600000) & (SH['SecurityID'] <= 700000)]
    #
    # sl = list(set(SH["SecurityID"].unique()) & set(SH1['SecurityID'].unique()))
    # SHH = SH[SH["SecurityID"].isin(sl)]
    # SH1 = SH1[SH1["SecurityID"].isin(sl)]
    #
    # print('----------------------------------------------------------------')
    # print('SH trade data:')
    #
    # SHH["ExecType"] = 'F'
    # SH1["ExecType"] = 'F'
    # columns = ["TransactTime", "ApplSeqNum", "SecurityID", "TradePrice", "TradeQty", "TradeMoney", "TradeBSFlag",
    #            "ExecType", "BidApplSeqNum", "OfferApplSeqNum"]
    # ree = pd.merge(SHH, SH1, left_on=columns, right_on=columns, how="outer", validate='one_to_one')
    # n1 = ree["sequenceNo_x"].count()
    # n2 = ree["sequenceNo_y"].count()
    # len1 = len(ree)
    # com['tag'].append("SH trade")
    # if (n1 == len1) & (n2 < len1):
    #     print(dataPathLs[0].split('\\')[4][14:22] + " is not complete:")
    #     print(ree[np.isnan(ree["sequenceNo_y"])])
    #     print(len(ree[np.isnan(ree["sequenceNo_y"])]))
    #     print(np.sort(ree[np.isnan(ree["sequenceNo_y"])]["TransactTime"].unique()))
    #     print(len(ree[np.isnan(ree["sequenceNo_y"])]["SecurityID"].unique()))
    #     print(ree[np.isnan(ree["sequenceNo_y"])]["SecurityID"].unique())
    #     com['complete'].append(dataPathLs[1].split('\\')[4][14:22])
    # elif (n2 == len1) & (n1 < len1):
    #     print(dataPathLs[1].split('\\')[4][14:22] + " is not complete:")
    #     print(ree[np.isnan(ree["sequenceNo_x"])])
    #     print(np.sort(ree[np.isnan(ree["sequenceNo_x"])]["TransactTime"].unique()))
    #     print(len(ree[np.isnan(ree["sequenceNo_x"])]["SecurityID"].unique()))
    #     print(ree[np.isnan(ree["sequenceNo_x"])]["SecurityID"].unique())
    #     print(n2 - n1)
    #     com['complete'].append(dataPathLs[0].split('\\')[4][14:22])
    # elif (n2 == len1) & (n1 == len1):
    #     com['complete'].append("zt_88_03")
    # else:
    #     com['complete'].append("nan")
    # del SHH
    # del SH1
    # del ree
    # del SH
    #
    # readPath = 'F:\\data\\' + y + '\\logs_' + y + '_zs_***_day_pcap\\mdTradePcap_SZ_***'
    # dataPathLs = np.array(glob.glob(readPath))
    #
    # TradeLogSZ1 = pd.read_csv(dataPathLs[0])
    #
    # TradeLogSZ1["SecurityID"] = TradeLogSZ1["ID"] - 2000000
    # TradeLogSZ1 = TradeLogSZ1.rename(columns={"time": 'TransactTime'})
    # TradeLogSZ1["TradeBSFlag"] = 'N'
    # TradeLogSZ1 = TradeLogSZ1[(TradeLogSZ1['SecurityID'] < 4000) | ((TradeLogSZ1['SecurityID'] > 300000)
    #                                                                 & (TradeLogSZ1['SecurityID'] < 310000))]
    #
    # TradeLogSZ = pd.read_csv(dataPathLs[1])
    #
    # TradeLogSZ["SecurityID"] = TradeLogSZ["ID"] - 2000000
    # TradeLogSZ = TradeLogSZ.rename(columns={"time": 'TransactTime'})
    # TradeLogSZ["TradeBSFlag"] = 'N'
    # TradeLogSZ = TradeLogSZ[(TradeLogSZ['SecurityID'] < 4000) | ((TradeLogSZ['SecurityID'] > 300000)
    #                                                              & (TradeLogSZ['SecurityID'] < 310000))]
    #
    #
    # sl = list(set(TradeLogSZ["SecurityID"].unique()) & set(TradeLogSZ1['SecurityID'].unique()))
    # TradeLogSZZ = TradeLogSZ[TradeLogSZ["SecurityID"].isin(sl)]
    # TradeLogSZ1 = TradeLogSZ1[TradeLogSZ1["SecurityID"].isin(sl)]
    #
    # print('----------------------------------------------------------------')
    # print('SZ trade data:')
    #
    # TradeLogSZZ["ExecType"] = TradeLogSZZ["ExecType"].apply(lambda x: str(x))
    # TradeLogSZ1["ExecType"] = TradeLogSZ1["ExecType"].apply(lambda x: str(x))
    #
    # columns = ["TransactTime", "ApplSeqNum", "SecurityID", "ExecType", "TradeBSFlag", "TradePrice", "TradeQty",
    #            "TradeMoney", "BidApplSeqNum", "OfferApplSeqNum"]
    # ree = pd.merge(TradeLogSZZ, TradeLogSZ1, left_on=columns, right_on=columns, how="outer", validate='one_to_one')
    # n1 = ree["sequenceNo_x"].count()
    # n2 = ree["sequenceNo_y"].count()
    # len1 = len(ree)
    # com['tag'].append("SZ trade")
    # if (n1 == len1) & (n2 < len1):
    #     print(dataPathLs[0].split('\\')[4][14:22] + " is not complete:")
    #     print(ree[np.isnan(ree["sequenceNo_y"])])
    #     print(len(ree[np.isnan(ree["sequenceNo_y"])]))
    #     print(np.sort(ree[np.isnan(ree["sequenceNo_y"])]["TransactTime"].unique()))
    #     print(len(ree[np.isnan(ree["sequenceNo_y"])]["SecurityID"].unique()))
    #     print(ree[np.isnan(ree["sequenceNo_y"])]["SecurityID"].unique())
    #     com['complete'].append(dataPathLs[1].split('\\')[4][14:22])
    # elif (n2 == len1) & (n1 < len1):
    #     print(dataPathLs[1].split('\\')[4][14:22] + " is not complete:")
    #     print(ree[np.isnan(ree["sequenceNo_x"])])
    #     print(np.sort(ree[np.isnan(ree["sequenceNo_x"])]["TransactTime"].unique()))
    #     print(len(ree[np.isnan(ree["sequenceNo_x"])]["SecurityID"].unique()))
    #     print(ree[np.isnan(ree["sequenceNo_x"])]["SecurityID"].unique())
    #     print(n2 - n1)
    #     com['complete'].append(dataPathLs[0].split('\\')[4][14:22])
    # elif (n2 == len1) & (n1 == len1):
    #     com['complete'].append("zs_96_03")
    # else:
    #     com['complete'].append("nan")
    # del TradeLogSZZ
    # del TradeLogSZ1
    # del ree
    # del TradeLogSZ
    #
    # com = pd.DataFrame(com)
    # print(com)


    com = pd.DataFrame()
    com['tag'] = ['SH snapshot', 'SH index', 'SZ snapshot', 'SZ order', 'SH trade', 'SZ trade']
    com['complete'] = ['zt_88_03', 'zt_88_03', 'zs_96_03', 'zs_96_03', 'zt_88_03', 'zs_96_03']
    com['data'] = ['new_record_data', 'new_record_data', 'new_record_data', 'new_record_data', 'new_record_data', 'new_record_data']

    if 'KR_data' in com['data'].values:
        print(y)
        print('Attention!!!!!!!!!!!!!!!! baseline data is incomplete today')
        print(com)


    server = com[com['tag'] == 'SH snapshot']['complete'].iloc[0]
    if server == 'nan':
        print('SH snapshot server unmatched')
    readPath = 'F:\\data\\' + y + '\\logs_' + y + '_' + server + '_day_pcap\\mdL2Pcap_SH_***'
    dataPathLs = np.array(glob.glob(readPath))
    print(dataPathLs[0])
    logSH1 = pd.read_csv(dataPathLs[0])

    logSH1["StockID"] = logSH1["ID"] - 1000000
    logSH1 = logSH1[["sequenceNo", "clockAtArrival", "StockID", "time", "cum_volume", "cum_amount", "close",
                     "bid1p", "bid2p", "bid3p", "bid4p", "bid5p", "bid1q", "bid2q", "bid3q",
                     "bid4q", "bid5q", "ask1p", "ask2p", "ask3p", "ask4p", "ask5p", "ask1q",
                     "ask2q", "ask3q", "ask4q", "ask5q", "open", "cum_tradesCnt"]]
    logSH1 = logSH1.rename(columns={"open": "openPrice", "cum_tradesCnt": "numTrades"})
    logSH1 = logSH1[(logSH1['StockID'] >= 600000) & (logSH1['StockID'] < 700000)]

    logSH2 = pd.read_csv('F:\\data\\20200803\\zs96\\mdLog_SH_20200803_0833_test.csv').loc[:, ["clockAtArrival", "sequenceNo", "source", "StockID",
                                              "exchange", "time", "cum_volume", "cum_amount", "close",
                                              "bid1p", "bid2p", "bid3p", "bid4p", "bid5p", "bid1q",
                                              "bid2q", "bid3q", "bid4q", "bid5q", "ask1p", "ask2p",
                                              "ask3p", "ask4p", "ask5p", "ask1q", "ask2q", "ask3q",
                                              "ask4q", "ask5q", "openPrice", "numTrades"]]
    logSH2["time"] = logSH2["time"].apply(lambda x: int((x.replace(':', "")).replace(".", "")))

    print('----------------------------------------------------------------')
    print('SH lv2 data:')
    in_dex = [16, 300, 852, 905]
    logSH = logSH2[logSH2['StockID'].isin(in_dex)]
    data1 = logSH2[~logSH2["StockID"].isin(in_dex) & (logSH2["time"] >= 91500000) & (logSH2["time"] <= 150000000) & (logSH2['source'] == 13)]
    data2 = logSH1[~logSH1["StockID"].isin(in_dex) & (logSH1["time"] >= 91500000) & (logSH1["time"] <= 150000000)]
    columns = ["StockID", "cum_volume", "cum_amount", "close", "bid1p", "bid2p", "bid3p", "bid4p", "bid5p", "bid1q",
               "bid2q", "bid3q", "bid4q", "bid5q", "ask1p", "ask2p", "ask3p", "ask4p", "ask5p", "ask1q", "ask2q", "ask3q",
               "ask4q", "ask5q", "openPrice", "time", "numTrades"]
    data1_1 = data1.drop_duplicates(subset=columns, keep="first").reset_index()
    data2_1 = data2.drop_duplicates(subset=columns, keep="first").reset_index()

    n1 = len(data1_1["StockID"].unique())
    n2 = len(data2_1["StockID"].unique())
    print(n1)
    print(n2)
    print(set(data1_1["StockID"].unique()) - set(data2_1["StockID"].unique()))
    print(set(data2_1["StockID"].unique()) - set(data1_1["StockID"].unique()))

    if n1 != n2:
        sl = list(set(data1_1["StockID"].unique()) & set(data2_1["StockID"].unique()))
        data1_1 = data1_1[data1_1["StockID"].isin(sl)]
        data2_1 = data2_1[data2_1["StockID"].isin(sl)]

    for cols in ['cum_amount', 'close', "bid1p", "bid2p", "bid3p", "bid4p", "bid5p", "ask1p", "ask2p", "ask3p", "ask4p", "ask5p", "openPrice"]:
        data1_1[cols] = (data1_1[cols] * 10000).round(0)

    test = pd.merge(data1_1, data2_1, left_on=columns, right_on=columns, how="outer")
    n1 = test["sequenceNo_x"].count()
    n2 = test["sequenceNo_y"].count()
    len1 = len(test)
    if (n1 == len1) & (n2 < len1):
        print('zs96 SH snapshot contains more data')
        print(test[np.isnan(test["sequenceNo_y"])])
        print(len(test[np.isnan(test["sequenceNo_y"])]) / n1)
        print(len(test[np.isnan(test["sequenceNo_y"])]["time"].unique()))
        print(test[np.isnan(test["sequenceNo_y"])]["time"].unique())
        print(len(test[np.isnan(test["sequenceNo_y"])]["StockID"].unique()))
        print(test[np.isnan(test["sequenceNo_y"])]["StockID"].unique())
    elif (n2 == len1) & (n1 < len1):
        print('zs96 SH snapshot is not complete')
        print(test[np.isnan(test["sequenceNo_x"])])
        print(n2 - n1)
        print((n2 - n1) / n1)
    elif (n2 == len1) & (n1 == len1):
        print('zs96 SH snapshot is complete')
    else:
        print('SH snapshot data unmatched')
    del data1
    del data2
    del test
    del data1_1
    del data2_1
    if 'KR_data' not in com['data'].values:
        logSH2 = logSH2[~logSH2["StockID"].isin(in_dex) & (logSH2['source'] == 13)]
        logSH1 = logSH1[~logSH1["StockID"].isin(in_dex)]
        for cols in ['cum_amount', 'close', "bid1p", "bid2p", "bid3p", "bid4p", "bid5p", "ask1p", "ask2p", "ask3p", "ask4p", "ask5p", "openPrice"]:
            logSH2[cols] = (logSH2[cols] * 10000).round(0)
        logSH2 = logSH2[["clockAtArrival", "sequenceNo", "StockID", "time", "cum_volume", "cum_amount", "close",
                         "bid1p", "bid2p", "bid3p", "bid4p", "bid5p", "bid1q", "bid2q", "bid3q", "bid4q", "bid5q", "ask1p", "ask2p",
                         "ask3p", "ask4p", "ask5p", "ask1q", "ask2q", "ask3q", "ask4q", "ask5q", "openPrice", "numTrades"]]
        assert(logSH1['time'].min() < 91500000)
        assert(logSH1['time'].max() > 150000000)
        SHs = pd.merge(logSH1, logSH2[(logSH2['time'] >= logSH1['time'].min()) & (logSH2['time'] <= logSH1['time'].max())], on=columns, how='outer')
        sl = list(set(logSH1['StockID'].unique()) - set(logSH2['StockID'].unique()))
        assert ((SHs[SHs['sequenceNo_x'].isnull()]['cum_volume'].unique() == [0]) & (SHs[SHs['sequenceNo_x'].isnull()]['StockID'].min() > 688000) & \
                (SHs[SHs['sequenceNo_x'].isnull()]['time'].min() > 150000000))
        assert ((SHs[(SHs['sequenceNo_y'].isnull()) & (~SHs['StockID'].isin(sl))]['StockID'].min() > 688000) &
                (SHs[(SHs['sequenceNo_y'].isnull()) & (~SHs['StockID'].isin(sl))]['time'].min() > 150000000))
        SHs = pd.merge(logSH1, logSH2[(logSH2['time'] >= logSH1['time'].min()) & (logSH2['time'] <= logSH1['time'].max())], on=columns, how='left')
        assert((logSH1[logSH1.duplicated('sequenceNo')].shape[0] == 0) & (logSH2[logSH2.duplicated('sequenceNo')].shape[0] == 0))
        p11 = SHs[~SHs['sequenceNo_y'].isnull()][SHs[~SHs['sequenceNo_y'].isnull()].duplicated(['sequenceNo_x'], keep=False)]
        p12 = SHs[~SHs['sequenceNo_y'].isnull()].drop_duplicates(['sequenceNo_x'], keep=False)
        p11 = p11.sort_values(by=['sequenceNo_x', 'sequenceNo_y'])
        p11['order1'] = p11.groupby('sequenceNo_x').cumcount()
        p11['order2'] = p11.groupby('sequenceNo_y').cumcount()
        num1 = p11['sequenceNo_x'].nunique()
        p11 = p11[p11['order1'] == p11['order2']]
        try:
            assert(p11.shape[0] == num1)
            p11.drop(['order1', 'order2'], axis=1, inplace=True)
        except:
            print('There are duplicates ticks only in baseline source!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!')
            p11_1 = SHs[~SHs['sequenceNo_y'].isnull()][SHs[~SHs['sequenceNo_y'].isnull()].duplicated(['sequenceNo_x'], keep=first)]
            p11_1 = pd.merge(p11_1, p11[['sequenceNo_x', 'order1']], on='sequenceNo_x', how='left')
            p11_1 = p11_1[p11_1['order1'].isnull()]
            p11_1['sequenceNo_y'] = np.nan
            p11_1['clockAtArrival_y'] = np.nan
            p11_1.drop(['order1'], axis=1, inplace=True)
            p11.drop(['order1', 'order2'], axis=1, inplace=True)
            p11 = pd.concat([p11, p11_1])
            del p11_1
        SHs = pd.concat([p11, p12, SHs[SHs['sequenceNo_y'].isnull()]])
        del p11
        del p12
        assert((SHs.shape[0] == logSH1.shape[0]) & (SHs[SHs.duplicated('sequenceNo_x')].shape[0] == 0))

        SHs = SHs.sort_values(by='sequenceNo_x')
        SHs['seq1'] = SHs.groupby('StockID')['sequenceNo_y'].ffill().bfill()
        SHs.loc[SHs['StockID'].isin(sl), 'seq1'] = np.nan
        SHs['count'] = SHs.groupby(['StockID', 'seq1']).cumcount()
        SHs = SHs[['StockID', 'sequenceNo_x', 'sequenceNo_y', 'clockAtArrival_y', 'seq1', 'count']]
        SHs['tag'] = 'SH'

        assert((len(set(sl) - set(SHs[SHs['seq1'].isnull()]['StockID'].unique())) == 0) &
                   (len(set(SHs[SHs['seq1'].isnull()]['StockID'].unique()) - set(sl)) == 0))
    del logSH1
    del logSH2

    print('----------------------------------------------------------------')
    print('SH index data:')

    server = com[com['tag'] == 'SH index']['complete'].iloc[0]
    if server == 'nan':
        print('SH snapshot server unmatched')

    readPath = 'F:\\data\\' + y + '\\logs_' + y + '_' + server + '_day_pcap\\mdIndexPcap_SH_***'
    dataPathLs = np.array(glob.glob(readPath))

    index = pd.read_csv(dataPathLs[0])

    index["StockID"] = index["ID"] - 1000000
    index = index.rename(columns={"open": "openPrice"})

    in_dex = [16, 300, 852, 905]
    index = index[index["StockID"].isin(in_dex)]

    data1 = logSH[(logSH["time"] >= 91500000) & (logSH["time"] <= 150000000) & (logSH['source'] == 13)]
    data2 = index[(index["time"] >= 91500000) & (index["time"] <= 150000000)]

    columns = ["StockID", "cum_volume", "cum_amount", "close", "openPrice"]
    data1_1 = data1.drop_duplicates(subset=columns, keep="first").reset_index()
    data2_1 = data2.drop_duplicates(subset=columns, keep="first").reset_index()

    for cols in ['cum_amount', 'close', "openPrice"]:
        data1_1[cols] = (data1_1[cols] * 10000).round(0)

    test = pd.merge(data1_1, data2_1, left_on=columns, right_on=columns, how="outer")
    n1 = test["sequenceNo_x"].count()
    n2 = test["sequenceNo_y"].count()
    len1 = len(test)

    if (n1 == len1) & (n2 < len1):
        print('zs96 SH index contains more data')
        print(test[np.isnan(test["sequenceNo_y"])])
    elif (n2 == len1) & (n1 < len1):
        print('zs96 SH index is not complete')
        print(test[np.isnan(test["sequenceNo_x"])])
    elif (n2 == len1) & (n1 == len1):
        print('zs96 SH index is complete')
    else:
        print('SH index data unmatched')
    del data1
    del data2
    del test
    del data1_1
    del data2_1

    if 'KR_data' not in com['data'].values:
        for cols in ['cum_amount', 'close', "openPrice"]:
            logSH[cols] = (logSH[cols] * 10000).round(0)
        logSH = logSH[logSH['source'] == 13]
        logSH = logSH[["clockAtArrival", "sequenceNo", "StockID", "time", "cum_volume", "cum_amount", "close",
                       "openPrice"]]
        assert(index['time'].min() < 91500000)
        assert(index['time'].max() > 150000000)
        SHindex = pd.merge(index, logSH, on=columns, how='outer')
        if SHindex[SHindex['sequenceNo_x'].isnull()].shape[0] != 0:
            print(SHindex[SHindex['sequenceNo_x'].isnull()])
            SHindex = SHindex[~SHindex['sequenceNo_x'].isnull()]
        assert(SHindex[SHindex['sequenceNo_y'].isnull()].shape[0] == 0)
        assert((index[index.duplicated('sequenceNo')].shape[0] == 0) & (logSH[logSH.duplicated('sequenceNo')].shape[0] == 0))
        p11 = SHindex[SHindex.duplicated(['sequenceNo_x'], keep=False)]
        p12 = SHindex.drop_duplicates(['sequenceNo_x'], keep=False)
        p11 = p11.sort_values(by=['sequenceNo_x', 'sequenceNo_y'])
        p11['order1'] = p11.groupby('sequenceNo_x').cumcount()
        p11['order2'] = p11.groupby('sequenceNo_y').cumcount()
        num1 = p11['sequenceNo_x'].nunique()
        t1 = p11.groupby('cum_volume')['order1', 'time_x'].max().reset_index()
        t1 = pd.merge(t1, p11.groupby('cum_volume')['time_x'].min().reset_index(), on='cum_volume')
        t2 = p11.groupby('cum_volume')['order2', 'time_y'].max().reset_index()
        t2 = pd.merge(t2, p11.groupby('cum_volume')['time_y'].min().reset_index(), on='cum_volume')
        t3 = pd.merge(t1, t2, on='cum_volume')
        print(t3[t3['order1'] != t3['order2']])
        p11 = p11[p11['order1'] == p11['order2']]
        del t1, t2, t3
        try:
            assert(p11.shape[0] == num1)
            p11.drop(['order1', 'order2'], axis=1, inplace=True)
        except:
            print('There are duplicates ticks only in baseline source!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!')
            p11_1 = SHindex[SHindex.duplicated(['sequenceNo_x'], keep=False)].drop_duplicates(['sequenceNo_x'])
            p11_1 = pd.merge(p11_1, p11[['sequenceNo_x', 'order1']], on='sequenceNo_x', how='left')
            p11_1 = p11_1[p11_1['order1'].isnull()]
            p11_1['sequenceNo_y'] = np.nan
            p11_1['clockAtArrival_y'] = np.nan
            p11_1.drop(['order1'], axis=1, inplace=True)
            p11.drop(['order1', 'order2'], axis=1, inplace=True)
            p11 = pd.concat([p11, p11_1])
            del p11_1
        SHindex = pd.concat([p11, p12])
        del p11
        del p12
        assert((SHindex.shape[0] == index.shape[0]) & (SHindex[SHindex.duplicated('sequenceNo_x')].shape[0] == 0))

        if SHindex[SHindex['sequenceNo_y'].isnull()].shape[0] != 0:
            SHindex = SHindex.sort_values(by='sequenceNo_x')
            SHindex['seq1'] = SHindex.groupby('StockID')['sequenceNo_y'].ffill().bfill()
            SHindex['count'] = SHindex.groupby(['StockID', 'seq1']).cumcount()
            SHindex = SHindex[['StockID', 'sequenceNo_x', 'sequenceNo_y', 'clockAtArrival_y', 'seq1', 'count']]
        else:
            SHindex['seq1'] = SHindex['sequenceNo_y']
            SHindex['count'] = SHindex.groupby(['StockID', 'seq1']).cumcount()
            SHindex = SHindex[['StockID', 'sequenceNo_x', 'sequenceNo_y', 'clockAtArrival_y', 'seq1', 'count']]
        SHindex['tag'] = 'SHindex'
    del index
    del logSH


    print('----------------------------------------------------------------')
    print('SZ lv2 data:')

    server = com[com['tag'] == 'SZ snapshot']['complete'].iloc[0]
    if server == 'nan':
        print('SZ snapshot server unmatched')
    readPath = 'F:\\data\\' + y + '\\logs_' + y + '_' + server + '_day_pcap\\mdL2Pcap_SZ_***'
    dataPathLs = np.array(glob.glob(readPath))

    logSZ1 = pd.read_csv(dataPathLs[0])

    logSZ1 = logSZ1.loc[:, ["clockAtArrival", "sequenceNo", "ID", "time", "cum_volume", "cum_amount", "close",
                            "bid1p", "bid2p", "bid3p", "bid4p", "bid5p", "bid1q",
                            "bid2q", "bid3q", "bid4q", "bid5q", "ask1p", "ask2p",
                            "ask3p", "ask4p", "ask5p", "ask1q", "ask2q", "ask3q",
                            "ask4q", "ask5q", "open", "cum_tradesCnt"]]
    logSZ1 = logSZ1.rename(columns={"open": "openPrice", "cum_tradesCnt": "numTrades"})
    logSZ1["StockID"] = logSZ1["ID"] - 2000000
    logSZ1 = logSZ1[(logSZ1['StockID'] < 4000) | ((logSZ1['StockID'] > 300000) & (logSZ1['StockID'] < 310000))]

    logSZ = pd.read_csv('F:\\data\\20200803\\zt88\\mdLog_SZ_20200803_0833_test.csv').loc[:, ["clockAtArrival", "sequenceNo", "source", "StockID",
                                              "exchange", "time", "cum_volume", "cum_amount", "close",
                                              "bid1p", "bid2p", "bid3p", "bid4p", "bid5p", "bid1q",
                                              "bid2q", "bid3q", "bid4q", "bid5q", "ask1p", "ask2p",
                                              "ask3p", "ask4p", "ask5p", "ask1q", "ask2q", "ask3q",
                                              "ask4q", "ask5q", "openPrice", "numTrades"]]
    logSZ = logSZ[(logSZ['StockID'] < 4000) | ((logSZ['StockID'] > 300000) & (logSZ['StockID'] < 310000))]
    logSZ["time"] = logSZ["time"].apply(lambda x: int(x.replace(':', "")) * 1000)
    logSZ = logSZ[logSZ['source'] == 12]

    data1 = logSZ[(logSZ["time"] >= 91500000) & (logSZ["time"] < 150000000)]
    data2 = logSZ1[(logSZ1["time"] >= 91500000) & (logSZ1["time"] < 150000000)]

    columns = ["StockID", "cum_volume", "cum_amount", "close", "bid1p", "bid2p", "bid3p", "bid4p", "bid5p", "bid1q",
               "bid2q",
               "bid3q", "bid4q", "bid5q", "ask1p", "ask2p", "ask3p", "ask4p", "ask5p", "ask1q", "ask2q", "ask3q",
               "ask4q", "ask5q", "openPrice", "numTrades", "time"]
    data1_1 = data1.drop_duplicates(subset=columns, keep="first").reset_index()
    data2_1 = data2.drop_duplicates(subset=columns, keep="first").reset_index()

    n1 = len(data1_1["StockID"].unique())
    n2 = len(data2_1["StockID"].unique())
    print(n1)
    print(n2)
    print(set(data1_1['StockID'].unique()) - set(data2_1['StockID'].unique()))
    print(set(data2_1['StockID'].unique()) - set(data1_1['StockID'].unique()))
    if n1 != n2:
        sl = list(set(data1_1["StockID"].unique()) & set(data2_1["StockID"].unique()))
        data1_1 = data1_1[data1_1["StockID"].isin(sl)]
        data2_1 = data2_1[data2_1["StockID"].isin(sl)]

    for cols in ['cum_amount', 'close', "bid1p", "bid2p", "bid3p", "bid4p", "bid5p", "ask1p", "ask2p", "ask3p", "ask4p", "ask5p", "openPrice"]:
        data1_1[cols] = (data1_1[cols] * 10000).round(0)

    columns = ["StockID", "cum_volume", "close", "bid1p", "bid2p", "bid3p", "bid4p", "bid5p", "bid1q",
               "bid2q",
               "bid3q", "bid4q", "bid5q", "ask1p", "ask2p", "ask3p", "ask4p", "ask5p", "ask1q", "ask2q", "ask3q",
               "ask4q", "ask5q", "openPrice", "numTrades", "time"]

    test = pd.merge(data1_1, data2_1, left_on=columns, right_on=columns, how="outer")
    n1 = test["sequenceNo_x"].count()
    n2 = test["sequenceNo_y"].count()
    len1 = len(test)

    if (n1 == len1) & (n2 < len1):
        print('zt88 SZ snapshot contains more data')
        print(test[np.isnan(test["sequenceNo_y"])])
    elif (n2 == len1) & (n1 < len1):
        print('zt88 SZ snapshot is not complete')
        print(test[np.isnan(test["sequenceNo_x"])])
    elif (n2 == len1) & (n1 == len1):
        print('zt88 SZ snapshot is complete')
    else:
        print('SZ snapshot data unmatched')

    del data1
    del data2
    del test
    del data1_1
    del data2_1


    if 'KR_data' not in com['data'].values:
        for cols in ['cum_amount', 'close', "bid1p", "bid2p", "bid3p", "bid4p", "bid5p", "ask1p", "ask2p", "ask3p", "ask4p", "ask5p", "openPrice"]:
            logSZ[cols] = (logSZ[cols] * 10000).round(0)
        logSZ = logSZ[["clockAtArrival", "sequenceNo", "StockID", "time", "cum_volume", "cum_amount", "close",
                         "bid1p", "bid2p", "bid3p", "bid4p", "bid5p", "bid1q", "bid2q", "bid3q", "bid4q", "bid5q", "ask1p", "ask2p",
                         "ask3p", "ask4p", "ask5p", "ask1q", "ask2q", "ask3q", "ask4q", "ask5q", "openPrice", "numTrades"]]
        assert(logSZ1['time'].min() < 91500000)
        assert(logSZ1['time'].max() > 150000000)
        SZ = pd.merge(logSZ1, logSZ[(logSZ['time'] >= logSZ1['time'].min()) & (logSZ['time'] <= logSZ1['time'].max())], on=columns, how='outer')
        sl = list(set(logSZ1['StockID'].unique()) - set(logSZ['StockID'].unique()))
        assert(SZ[SZ['sequenceNo_x'].isnull()].shape[0] == 0)
        assert(SZ[(SZ['sequenceNo_y'].isnull()) & (~SZ['StockID'].isin(sl))].shape[0] == 0)
        assert((logSZ1[logSZ1.duplicated('sequenceNo')].shape[0] == 0) & (logSZ[logSZ.duplicated('sequenceNo')].shape[0] == 0))
        assert((SZ.shape[0] == logSZ1.shape[0]) & (SZ[SZ.duplicated('sequenceNo_x')].shape[0] == 0))

        SZ['seq1'] = SZ['sequenceNo_y']
        SZ.loc[SZ['StockID'].isin(sl), 'seq1'] = np.nan
        SZ['count'] = SZ.groupby(['StockID', 'seq1']).cumcount()
        SZ = SZ[['StockID', 'sequenceNo_x', 'sequenceNo_y', 'clockAtArrival_y', 'seq1', 'count']]
        SZ['tag'] = 'SZ'

        assert((len(set(sl) - set(SZ[SZ['seq1'].isnull()]['StockID'].unique())) == 0) &
                   (len(set(SZ[SZ['seq1'].isnull()]['StockID'].unique()) - set(sl)) == 0))
    del logSZ
    del logSZ1



    server = com[com['tag'] == 'SZ order']['complete'].iloc[0]
    if server == 'nan':
        print('SZ snapshot server unmatched')
    readPath = 'F:\\data\\' + y + '\\logs_' + y + '_' + server + '_day_pcap\\mdOrderPcap_SZ_***'
    dataPathLs = np.array(glob.glob(readPath))

    OrderLogSZ1 = pd.read_csv(dataPathLs[0])

    OrderLogSZ1["SecurityID"] = OrderLogSZ1["ID"] - 2000000
    OrderLogSZ1 = OrderLogSZ1.rename(columns={"time": 'TransactTime'})

    OrderLogSZ1["OrderType"] = np.where(OrderLogSZ1["OrderType"] == 2, '2', np.where(
        OrderLogSZ1["OrderType"] == 1, '1', OrderLogSZ1['OrderType']))
    OrderLogSZ1 = OrderLogSZ1[(OrderLogSZ1['SecurityID'] < 4000) | ((OrderLogSZ1['SecurityID'] > 300000)
                                                                    & (OrderLogSZ1['SecurityID'] < 310000))]

    OrderLogSZ = pd.read_csv('F:\\data\\20200803\\zt88\\mdOrderLog_20200803_0833_test.csv').loc[:, ["clockAtArrival", "sequenceNo", "exchId", "TransactTime",
                                                 "ApplSeqNum", "SecurityID", "Side", "OrderType", "Price",
                                                 "OrderQty"]]
    OrderLogSZ["OrderType"] = OrderLogSZ["OrderType"].apply(lambda x: str(x))
    OrderLogSZ = OrderLogSZ[(OrderLogSZ['SecurityID'] < 4000) | ((OrderLogSZ['SecurityID'] > 300000)
                                                                    & (OrderLogSZ['SecurityID'] < 310000))]

    print(set(OrderLogSZ["SecurityID"].unique()) - set(OrderLogSZ1["SecurityID"].unique()))
    print(set(OrderLogSZ1["SecurityID"].unique()) - set(OrderLogSZ["SecurityID"].unique()))
    sl = list(set(OrderLogSZ["SecurityID"].unique()) & set(OrderLogSZ1['SecurityID'].unique()))
    OrderLogSZ = OrderLogSZ[OrderLogSZ["SecurityID"].isin(sl)]
    OrderLogSZ1 = OrderLogSZ1[OrderLogSZ1["SecurityID"].isin(sl)]

    print('----------------------------------------------------------------')
    print('SZ order data:')

    columns = ["ApplSeqNum", "TransactTime", "Side", 'OrderType', 'Price', 'OrderQty', "SecurityID"]
    ree = pd.merge(OrderLogSZ, OrderLogSZ1, on=columns, how="outer", validate='one_to_one')
    n1 = ree["sequenceNo_x"].count()
    n2 = ree["sequenceNo_y"].count()
    len1 = len(ree)
    if (n1 == len1) & (n2 < len1):
        print('zt88 SZ order contains more data')
        print(test[np.isnan(test["sequenceNo_y"])])
    elif (n2 == len1) & (n1 < len1):
        print('zt88 SZ order is not complete')
        print(test[np.isnan(test["sequenceNo_x"])])
    elif (n2 == len1) & (n1 == len1):
        print('zt88 SZ order is complete')
    else:
        print('SZ order data unmatched')
    del ree
    if 'KR_data' not in com['data'].values:
        order = pd.merge(OrderLogSZ1, OrderLogSZ[["ApplSeqNum", "TransactTime", "Side", 'OrderType', 'Price',
                                                   'OrderQty', "SecurityID", "clockAtArrival", 'sequenceNo']],
                          on=columns, how="outer")
        sl = list(set(OrderLogSZ1['SecurityID'].unique()) - set(OrderLogSZ['SecurityID'].unique()))
        assert((order[order['sequenceNo_x'].isnull()].shape[0] == 0) & (order[(order['sequenceNo_y'].isnull()) & (~order['SecurityID'].isin(sl))].shape[0] == 0))
        assert((OrderLogSZ[OrderLogSZ.duplicated('sequenceNo')].shape[0] == 0) & (OrderLogSZ1[OrderLogSZ1.duplicated('sequenceNo')].shape[0] == 0))
        assert((order.shape[0] == OrderLogSZ.shape[0]) & (order[order.duplicated('sequenceNo_x')].shape[0] == 0))

        order['seq1'] = order['sequenceNo_y']
        order.loc[order['SecurityID'].isin(sl), 'seq1'] = np.nan
        order['count'] = order.groupby(['SecurityID', 'seq1']).cumcount()
        order = order[['SecurityID', 'sequenceNo_x', 'sequenceNo_y', 'clockAtArrival_y', 'seq1', 'count']]
        order['tag'] = 'order'

        assert((len(set(sl) - set(order[order['seq1'].isnull()]['SecurityID'].unique())) == 0) &
                   (len(set(order[order['seq1'].isnull()]['SecurityID'].unique()) - set(sl)) == 0))
    del OrderLogSZ
    del OrderLogSZ1




    server = com[com['tag'] == 'SH trade']['complete'].iloc[0]
    if server == 'nan':
        print('SZ snapshot server unmatched')
    readPath = 'F:\\data\\' + y + '\\logs_' + y + '_' + server + '_day_pcap\\mdTradePcap_SH_***'
    dataPathLs = np.array(glob.glob(readPath))

    SH1 = pd.read_csv(dataPathLs[0])

    SH1["SecurityID"] = SH1["ID"] - 1000000
    SH1 = SH1.rename(columns={"time": 'TransactTime'})
    SH1 = SH1[(SH1['SecurityID'] >= 600000) & (SH1['SecurityID'] <= 700000)]

    SH = pd.read_csv('F:\\data\\20200803\\zs96\\mdTradeLog_20200803_0833_test.csv')
    SH = SH[SH['ChannelNo'] != 103]
    SH = SH.loc[:, ["clockAtArrival", "sequenceNo", "exchId", "TransactTime", 'ExecType',
                                                 "ApplSeqNum", "SecurityID", "TradeBSFlag",
                                                 "TradePrice", "TradeQty", "TradeMoney", "BidApplSeqNum",
                                                 "OfferApplSeqNum"]]
    SH = SH[SH["exchId"] == 1]
    SH["ExecType"] = 'F'
    SH1["ExecType"] = 'F'

    print(set(SH["SecurityID"].unique()) - set(SH1['SecurityID'].unique()))
    print(set(SH1["SecurityID"].unique()) - set(SH['SecurityID'].unique()))

    sl = list(set(SH["SecurityID"].unique()) & set(SH1['SecurityID'].unique()))
    data1 = SH[SH["SecurityID"].isin(sl)]
    data2 = SH1[SH1["SecurityID"].isin(sl)]

    print('----------------------------------------------------------------')
    print('SH trade data:')

    data1["ExecType"] = 'F'
    data2["ExecType"] = 'F'
    columns = ["TransactTime", "ApplSeqNum", "SecurityID", "TradePrice", "TradeQty", "TradeMoney", "TradeBSFlag",
               "ExecType", "BidApplSeqNum", "OfferApplSeqNum"]
    ree = pd.merge(data1, data2, left_on=columns, right_on=columns, how="outer", validate='one_to_one')
    n1 = ree["sequenceNo_x"].count()
    n2 = ree["sequenceNo_y"].count()
    len1 = len(ree)
    if (n1 == len1) & (n2 < len1):
        print('zs96 SH trade contains more data')
        print(test[np.isnan(test["sequenceNo_y"])])
    elif (n2 == len1) & (n1 < len1):
        print('zs96 SH trade is not complete')
        print(test[np.isnan(test["sequenceNo_x"])])
    elif (n2 == len1) & (n1 == len1):
        print('zs96 SH trade is complete')
    else:
        print('SH trade data unmatched')
    del ree
    del data1
    del data2
    if 'KR_data' not in com['data'].values:
        tradeSH = pd.merge(SH1, SH[["TransactTime", "ApplSeqNum", "SecurityID", "TradePrice", "TradeQty", "TradeMoney", "TradeBSFlag", \
               "ExecType", "BidApplSeqNum", "OfferApplSeqNum",  "clockAtArrival", 'sequenceNo']], on=columns, how="outer")
        sl = list(set(SH1['SecurityID'].unique()) - set(SH['SecurityID'].unique()))
        assert((tradeSH[tradeSH['sequenceNo_x'].isnull()].shape[0] == 0) & (tradeSH[(tradeSH['sequenceNo_y'].isnull()) & (~tradeSH['SecurityID'].isin(sl))].shape[0] == 0))
        assert((SH1[SH1.duplicated('sequenceNo')].shape[0] == 0) & (SH[SH.duplicated('sequenceNo')].shape[0] == 0))
        assert((tradeSH.shape[0] == SH1.shape[0]) & (tradeSH[tradeSH.duplicated('sequenceNo_x')].shape[0] == 0))

        tradeSH['seq1'] = tradeSH['sequenceNo_y']
        tradeSH.loc[tradeSH['SecurityID'].isin(sl), 'seq1'] = np.nan
        tradeSH['count'] = tradeSH.groupby(['SecurityID', 'seq1']).cumcount()
        tradeSH = tradeSH[['SecurityID', 'sequenceNo_x', 'sequenceNo_y', 'clockAtArrival_y', 'seq1', 'count']]
        tradeSH['tag'] = 'tradeSH'

        assert((len(set(sl) - set(tradeSH[tradeSH['seq1'].isnull()]['SecurityID'].unique())) == 0) &
                   (len(set(tradeSH[tradeSH['seq1'].isnull()]['SecurityID'].unique()) - set(sl)) == 0))
    del SH
    del SH1













    server = com[com['tag'] == 'SZ trade']['complete'].iloc[0]
    if server == 'nan':
        print('SZ snapshot server unmatched')
    readPath = 'F:\\data\\' + y + '\\logs_' + y + '_' + server + '_day_pcap\\mdTradePcap_SZ_***'
    dataPathLs = np.array(glob.glob(readPath))

    TradeLogSZ1 = pd.read_csv(dataPathLs[0])

    TradeLogSZ1["SecurityID"] = TradeLogSZ1["ID"] - 2000000
    TradeLogSZ1 = TradeLogSZ1.rename(columns={"time": 'TransactTime'})
    TradeLogSZ1["TradeBSFlag"] = 'N'
    TradeLogSZ1 = TradeLogSZ1[(TradeLogSZ1['SecurityID'] < 4000) | ((TradeLogSZ1['SecurityID'] > 300000)
                                                                    & (TradeLogSZ1['SecurityID'] < 310000))]

    TradeLogSZ = pd.read_csv('F:\\data\\20200803\\zt88\\mdTradeLog_20200803_0833_test.csv')
    TradeLogSZ = TradeLogSZ[TradeLogSZ['ChannelNo'] != 103]
    TradeLogSZ = TradeLogSZ.loc[:, ["clockAtArrival", "sequenceNo", "exchId", "TransactTime",
                                                 "ApplSeqNum", "SecurityID", "ExecType", "TradeBSFlag",
                                                 "TradePrice", "TradeQty", "TradeMoney", "BidApplSeqNum",
                                                 "OfferApplSeqNum"]]
    TradeLogSZ = TradeLogSZ[TradeLogSZ["exchId"] != 1]
    TradeLogSZ = TradeLogSZ[(TradeLogSZ['SecurityID'] < 4000) | ((TradeLogSZ['SecurityID'] > 300000)
                                                                    & (TradeLogSZ['SecurityID'] < 310000))]

    print(set(TradeLogSZ["SecurityID"].unique()) - set(TradeLogSZ1['SecurityID'].unique()))
    print(set(TradeLogSZ1["SecurityID"].unique()) - set(TradeLogSZ['SecurityID'].unique()))
    sl = list(set(TradeLogSZ["SecurityID"].unique()) & set(TradeLogSZ1['SecurityID'].unique()))
    TradeLogSZ = TradeLogSZ[TradeLogSZ["SecurityID"].isin(sl)]
    TradeLogSZ1 = TradeLogSZ1[TradeLogSZ1["SecurityID"].isin(sl)]

    print('----------------------------------------------------------------')
    print('SZ trade data:')

    TradeLogSZ["ExecType"] = TradeLogSZ["ExecType"].apply(lambda x: str(x))
    TradeLogSZ1["ExecType"] = TradeLogSZ1["ExecType"].apply(lambda x: str(x))

    columns = ["TransactTime", "ApplSeqNum", "SecurityID", "ExecType", "TradeBSFlag", "TradePrice", "TradeQty",
               "TradeMoney", "BidApplSeqNum", "OfferApplSeqNum"]
    ree = pd.merge(TradeLogSZ, TradeLogSZ1, left_on=columns, right_on=columns, how="outer")
    n1 = ree["sequenceNo_x"].count()
    n2 = ree["sequenceNo_y"].count()
    len1 = len(ree)
    if (n1 == len1) & (n2 < len1):
        print('zt88 SZ trade contains more data')
        print(test[np.isnan(test["sequenceNo_y"])])
    elif (n2 == len1) & (n1 < len1):
        print('zt88 SZ trade is not complete')
        print(test[np.isnan(test["sequenceNo_x"])])
    elif (n2 == len1) & (n1 == len1):
        print('zt88 SZ trade is complete')
    else:
        print('SZ trade data unmatched')
    del ree
    del TradeLogSZ
    del TradeLogSZ1
    if 'KR_data' not in com['data'].values:
        readPath = 'F:\\data\\' + y + '\\logs_' + y + '_' + server + '_day_pcap\\mdTradePcap_SZ_***'
        dataPathLs = np.array(glob.glob(readPath))

        TradeLogSZ1 = pd.read_csv(dataPathLs[0])

        TradeLogSZ1["SecurityID"] = TradeLogSZ1["ID"] - 2000000
        TradeLogSZ1 = TradeLogSZ1.rename(columns={"time": 'TransactTime'})
        TradeLogSZ1["TradeBSFlag"] = 'N'
        TradeLogSZ1 = TradeLogSZ1[(TradeLogSZ1['SecurityID'] < 4000) | ((TradeLogSZ1['SecurityID'] > 300000)
                                                                        & (TradeLogSZ1['SecurityID'] < 310000))]

        TradeLogSZ = pd.read_csv('F:\\data\\20200803\\zt88\\mdTradeLog_20200803_0833_test.csv')
        TradeLogSZ = TradeLogSZ[TradeLogSZ['ChannelNo'] != 103]
        TradeLogSZ = TradeLogSZ.loc[:, ["clockAtArrival", "sequenceNo", "exchId", "TransactTime",
                                        "ApplSeqNum", "SecurityID", "ExecType", "TradeBSFlag",
                                        "TradePrice", "TradeQty", "TradeMoney", "BidApplSeqNum",
                                        "OfferApplSeqNum"]]
        TradeLogSZ = TradeLogSZ[TradeLogSZ["exchId"] != 1]
        TradeLogSZ = TradeLogSZ[(TradeLogSZ['SecurityID'] < 4000) | ((TradeLogSZ['SecurityID'] > 300000)
                                                                     & (TradeLogSZ['SecurityID'] < 310000))]
        TradeLogSZ["ExecType"] = TradeLogSZ["ExecType"].apply(lambda x: str(x))
        TradeLogSZ1["ExecType"] = TradeLogSZ1["ExecType"].apply(lambda x: str(x))
        tradeSZ = pd.merge(TradeLogSZ1, TradeLogSZ[["TransactTime", "ApplSeqNum", "SecurityID", "TradePrice", "TradeQty", "TradeMoney", "TradeBSFlag", \
               "ExecType", "BidApplSeqNum", "OfferApplSeqNum",  "clockAtArrival", 'sequenceNo']], on=columns, how="outer")
        sl = list(set(TradeLogSZ1['SecurityID'].unique()) - set(TradeLogSZ['SecurityID'].unique()))
        assert((tradeSZ[tradeSZ['sequenceNo_x'].isnull()].shape[0] == 0) & (tradeSZ[(tradeSZ['sequenceNo_y'].isnull()) & (~tradeSZ['SecurityID'].isin(sl))].shape[0] == 0))
        assert((TradeLogSZ1[TradeLogSZ1.duplicated('sequenceNo')].shape[0] == 0) & (TradeLogSZ[TradeLogSZ.duplicated('sequenceNo')].shape[0] == 0))
        assert((tradeSZ.shape[0] == TradeLogSZ1.shape[0]) & (tradeSZ[tradeSZ.duplicated('sequenceNo_x')].shape[0] == 0))

        tradeSZ['seq1'] = tradeSZ['sequenceNo_y']
        tradeSZ.loc[tradeSZ['SecurityID'].isin(sl), 'seq1'] = np.nan
        tradeSZ['count'] = tradeSZ.groupby(['SecurityID', 'seq1']).cumcount()
        tradeSZ = tradeSZ[['SecurityID', 'sequenceNo_x', 'sequenceNo_y', 'clockAtArrival_y', 'seq1', 'count']]
        tradeSZ['tag'] = 'tradeSZ'

        assert((len(set(sl) - set(tradeSZ[tradeSZ['seq1'].isnull()]['SecurityID'].unique())) == 0) &
                   (len(set(tradeSZ[tradeSZ['seq1'].isnull()]['SecurityID'].unique()) - set(sl)) == 0))
    del TradeLogSZ
    del TradeLogSZ1

    fr = pd.concat([SHs, SHindex, SZ, order, tradeSH, tradeSZ])
    # print(fr[~fr['seq1'].isnull()][fr[~fr['seq1'].isnull()].duplicated(['seq1', 'count'], keep=False)])
    # fr1 = fr[~fr['seq1'].isnull()]
    # fr2 = fr[fr['seq1'].isnull()]
    # fr1 = fr1.sort_values(by=['seq1', 'count']).reset_index(drop=True)
    # fr2 = fr2.sort_values(by=['seq1', 'count']).reset_index(drop=True)
    # fr1['sequenceNo_y'] = fr1.index
    # fr2['sequenceNo'] = range(int(fr1['sequenceNo'].max()) + 1, int(fr1['sequenceNo'].max()) + 1 + fr2.shape[0])
    # fr = pd.concat([fr1, fr2])
    # assert(fr[fr.duplicated('sequenceNo_y', keep=False)].shape[0] == 0)
    # assert(fr[fr.duplicated('sequenceNo_x', keep=False)].shape[0] == 0)
    fr.to_pickle(r'G:\2020_new_data.pkl')





if __name__ == '__main__':
    new_record_data_check()

