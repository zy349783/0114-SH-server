#!/usr/bin/env python
# coding: utf-8


import numpy as np
import pandas as pd
import statsmodels.api as sm
import pickle
from matplotlib import pyplot as plt
import statsmodels.api as sm
from matplotlib.ticker import Formatter
import collections
import glob
import os
import datetime
pd.set_option("max_columns", 200)


def new_record_data_check():
    y = (datetime.datetime.today() + datetime.timedelta(days=-1)).strftime('%Y%m%d')
    print('----------------------------------------------------------------')
    print(y)
    re = {}
    for col in ['date', 'data', 'baseline', 'test', 'merge', 'time', 'stock_list']:
        re[col] = []
    
    path1 = np.array(glob.glob('/mnt/Kevin_zhenyu/rawData/logs_' + y + '_zs_92_01_day_data/mdLog_SH_*'))[0]
    path2 = np.array(glob.glob('/mnt/Kevin_zhenyu/rawData/logs_' + y + '_zs_92_01_day_data/mdLog_SZ_*'))[0]
    path3 = np.array(glob.glob('/mnt/Kevin_zhenyu/rawData/logs_' + y + '_zt_96_04_day_96data/mdLog_SH_*'))[0]
        
        
    startTm = datetime.datetime.now()
    logSH2 = pd.read_csv(path3,
                        encoding="utf-8").loc[:, ["clockAtArrival", "sequenceNo", "source", "StockID",
                                                  "exchange", "time", "cum_volume", "cum_amount", "close",
                                                  "bid1p", "bid2p", "bid3p", "bid4p", "bid5p", "bid1q",
                                                  "bid2q", "bid3q", "bid4q", "bid5q", "ask1p", "ask2p",
                                                  "ask3p", "ask4p", "ask5p", "ask1q", "ask2q", "ask3q",
                                                  "ask4q", "ask5q", "openPrice", "numTrades"]]
    print(datetime.datetime.now() - startTm)
    logSH2["time"] = logSH2["time"].apply(lambda x: int((x.replace(':', "")).replace(".", "")))
    
    readPath = '/mnt/dailyRawData/logs_' + y + '_zt_***_day_pcap/mdL2Pcap_SH_***'
    dataPathLs = np.array(glob.glob(readPath))
    for pp in dataPathLs:
        startTm = datetime.datetime.now()
        logSH1 = pd.read_csv(pp)
        print(datetime.datetime.now() - startTm)
        
        logSH1["StockID"] = logSH1["ID"] - 1000000
        logSH1 = logSH1[["sequenceNo", "StockID", "time", "cum_volume", "cum_amount", "close",
                         "bid1p", "bid2p", "bid3p", "bid4p", "bid5p", "bid1q", "bid2q", "bid3q", 
                         "bid4q", "bid5q", "ask1p", "ask2p", "ask3p", "ask4p", "ask5p", "ask1q", 
                         "ask2q", "ask3q", "ask4q", "ask5q", "open", "cum_tradesCnt"]]
        logSH1 = logSH1.rename(columns={"open":"openPrice", "cum_tradesCnt":"numTrades"})
                
        print('----------------------------------------------------------------')
        print('SH lv2 data:')
        print(pp.split('/')[3][14:22])
        in_dex = [16, 300, 852, 905]
        data1 = logSH2[~logSH2["StockID"].isin(in_dex) & (logSH2["time"] >= 91500000) & (logSH2["time"] <= 150000000) & (logSH2["source"] == 13)]
        data2 = logSH1[~logSH1["StockID"].isin(in_dex) & (logSH1["time"] >= 91500000) & (logSH1["time"] <= 150000000)]
        columns = ["StockID", "cum_volume", "cum_amount", "close", "bid1p", "bid2p", "bid3p", "bid4p", "bid5p", "bid1q", "bid2q",
               "bid3q", "bid4q", "bid5q", "ask1p", "ask2p", "ask3p", "ask4p", "ask5p", "ask1q", "ask2q", "ask3q",
               "ask4q", "ask5q", "openPrice", "time", "numTrades"]
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
        
        for cols in ["cum_amount", "close", "bid1p", "bid2p", "bid3p", "bid4p", "bid5p","ask1p", "ask2p", "ask3p", "ask4p", "ask5p", "openPrice"]:
            data1_1[cols] = (data1_1[cols]*10000).round(0)
        
        test = pd.merge(data1_1, data2_1, left_on=columns, right_on=columns, how="outer")
        n1 = test["sequenceNo_x"].count()
        n2 = test["sequenceNo_y"].count()
        len1 = len(test)
        print(n1)
        print(n2)
        print(len1)
        re['date'].append(y)
        re['data'].append('SH lv2 data ' + pp.split('/')[3][14:22])
        re['baseline'].append(n1)
        re['test'].append(n2)
        re['merge'].append(len1)
        if (n1 == len1) & (n2 == len1):
            re['time'].append(0)
            re['stock_list'].append(0)
        print("-----------------------------------------------")
        if n2 < len1:
            print("test is not complete:")
            print(test[np.isnan(test["sequenceNo_y"])])
            print(len(test[np.isnan(test["sequenceNo_y"])])/n1)
            print(len(test[np.isnan(test["sequenceNo_y"])]["time"].unique()))
            print(test[np.isnan(test["sequenceNo_y"])]["time"].unique())
            print(len(test[np.isnan(test["sequenceNo_y"])]["StockID"].unique()))
            print(test[np.isnan(test["sequenceNo_y"])]["StockID"].unique())
            re['time'].append(np.sort(test[np.isnan(test["sequenceNo_y"])]["time"].unique()))
            re['stock_list'].append(np.sort(test[np.isnan(test["sequenceNo_y"])]["StockID"].unique()))
        if (len1 == n2) & (n1 < len1):
            print("baseline is not complete:")
            print(test[np.isnan(test["sequenceNo_x"])])
            print(n2-n1)
            print((n2-n1)/n1)
            re['time'].append(np.sort(test[np.isnan(test["sequenceNo_x"])]["time"].unique()))
            re['stock_list'].append(np.sort(test[np.isnan(test["sequenceNo_x"])]["StockID"].unique()))
        del logSH1
        del data1
        del data2
        del test
        del data1_1
        del data2_1
    del logSH2
    
    
    print('----------------------------------------------------------------')
    print('SH index data:')
    
    startTm = datetime.datetime.now()
    logSH = pd.read_csv(path1,
                        encoding="utf-8").loc[:, ["clockAtArrival", "sequenceNo", "source", "StockID",
                                                  "exchange", "time", "cum_volume", "cum_amount", "close",
                                                  "bid1p", "bid2p", "bid3p", "bid4p", "bid5p", "bid1q",
                                                  "bid2q", "bid3q", "bid4q", "bid5q", "ask1p", "ask2p",
                                                  "ask3p", "ask4p", "ask5p", "ask1q", "ask2q", "ask3q",
                                                  "ask4q", "ask5q", "openPrice"]]
    print(datetime.datetime.now() - startTm)
    logSH["time"] = logSH["time"].apply(lambda x: int((x.replace(':', "")).replace(".", "")))
        
    readPath = '/mnt/dailyRawData/logs_' + y + '_zt_***_day_pcap/mdIndexPcap_SH_***'
    dataPathLs = np.array(glob.glob(readPath))
    for pp in dataPathLs:
        print(pp.split('/')[3][14:22])
        startTm = datetime.datetime.now()
        index = pd.read_csv(pp)
        print(datetime.datetime.now() - startTm)
        
        index["StockID"] = index["ID"] - 1000000
        index = index.rename(columns={"open":"openPrice"})
    
        
        in_dex = [16, 300, 852, 905]
        index = index[index["StockID"].isin(in_dex)]
        print(index["StockID"].unique())
        
        data1 = logSH[(logSH["StockID"].isin(in_dex)) & (logSH["time"] >= 91500000) & (logSH["time"] <= 150000000)]
        data2 = index[(index["time"] >= 91500000) & (index["time"] <= 150000000)]
        data1['cum_amount'] = data1['cum_amount'].round(1)
    
        columns = ["StockID", "cum_volume", "cum_amount", "close", "openPrice"]
        data1_1 = data1.drop_duplicates(subset=columns, keep="first").reset_index()
        data2_1 = data2.drop_duplicates(subset=columns, keep="first").reset_index()
        
        for cols in ["cum_amount", "close", "openPrice"]:
            data1_1[cols] = (data1_1[cols]*10000).round(0)
            
            
        test = pd.merge(data1_1, data2_1, left_on=columns, right_on=columns, how="outer")
        n1 = test["sequenceNo_x"].count()
        n2 = test["sequenceNo_y"].count()
        len1 = len(test)
        print(n1)
        print(n2)
        print(len1)
        re['date'].append(y)
        re['data'].append('SH index data without time column ' + pp.split('/')[3][14:22])
        re['baseline'].append(n1)
        re['test'].append(n2)
        re['merge'].append(len1)
        if (n1 == len1) & (n2 == len1):
            re['time'].append(0)
            re['stock_list'].append(0)
        if n2 < len1:
            print("test is not complete:")
            print(test[np.isnan(test["sequenceNo_y"])])
            re['time'].append(np.sort(test[np.isnan(test["sequenceNo_y"])]["time_x"].unique()))
            re['stock_list'].append(np.sort(test[np.isnan(test["sequenceNo_y"])]["StockID"].unique()))
        if (n2 == len1) & (n1 < len1):
            print("baseline is not complete::")
            print(test[np.isnan(test["sequenceNo_x"])])
            re['time'].append(np.sort(test[np.isnan(test["sequenceNo_x"])]["time_y"].unique()))
            re['stock_list'].append(np.sort(test[np.isnan(test["sequenceNo_x"])]["StockID"].unique()))
        
        columns = ["StockID", "cum_volume", "cum_amount", "close", "openPrice", "time"]
        test = pd.merge(data1_1, data2_1, left_on=columns, right_on=columns, how="outer")
        n1 = test["sequenceNo_x"].count()
        n2 = test["sequenceNo_y"].count()
        len1 = len(test)
        print(n1)
        print(n2)
        print(len1)
        re['date'].append(y)
        re['data'].append('SH index data with time column ' + pp.split('/')[3][14:22])
        re['baseline'].append(n1)
        re['test'].append(n2)
        re['merge'].append(len1)
        re['time'].append(0)
        re['stock_list'].append(0)
        if n2 < len1:
            print("test is not complete:")
            print(test[np.isnan(test["sequenceNo_y"])])
        if (n2 == len1) & (n1 < len1):
            print("baseline is not complete::")
            print(test[np.isnan(test["sequenceNo_x"])])
    
        
        del index
        del data1
        del data2
        del test
        del data1_1
        del data2_1
    del logSH

    
    print('----------------------------------------------------------------')
    print('SZ lv2 data:')
    
    startTm = datetime.datetime.now()
    logSZ = pd.read_csv(path2,
                        encoding="utf-8").loc[:, ["clockAtArrival", "sequenceNo", "source", "StockID",
                                                  "exchange", "time", "cum_volume", "cum_amount", "close",
                                                  "bid1p", "bid2p", "bid3p", "bid4p", "bid5p", "bid1q",
                                                  "bid2q", "bid3q", "bid4q", "bid5q", "ask1p", "ask2p",
                                                  "ask3p", "ask4p", "ask5p", "ask1q", "ask2q", "ask3q",
                                                  "ask4q", "ask5q", "openPrice", "numTrades"]]
    logSZ["time"] = logSZ["time"].apply(lambda x: int((x.replace(':', "")).replace(".", "")))
    print(datetime.datetime.now() - startTm)
        
    readPath = '/mnt/dailyRawData/logs_' + y + '_zs_***_day_pcap/mdL2Pcap_SZ_***'
    dataPathLs = np.array(glob.glob(readPath))
    for pp in dataPathLs:
        print(pp.split('/')[3][14:22])
        startTm = datetime.datetime.now()
        logSZ1 = pd.read_csv(pp)
        print(datetime.datetime.now() - startTm)
        
        logSZ1 = logSZ1.loc[:, ["clockAtArrival", "sequenceNo", "ID", "time", "cum_volume", "cum_amount", "close",
                                                  "bid1p", "bid2p", "bid3p", "bid4p", "bid5p", "bid1q",
                                                  "bid2q", "bid3q", "bid4q", "bid5q", "ask1p", "ask2p",
                                                  "ask3p", "ask4p", "ask5p", "ask1q", "ask2q", "ask3q",
                                                  "ask4q", "ask5q", "open", "cum_tradesCnt"]]
        logSZ1 = logSZ1.rename(columns={"open":"openPrice", "cum_tradesCnt":"numTrades"})
        logSZ1["StockID"] = logSZ1["ID"] - 2000000
    
        
        startTm = datetime.datetime.now()
        data1 = logSZ[(logSZ["time"] >= 91500000) & (logSZ["time"] < 150000000) & (logSZ["source"] == 4)]
        data2 = logSZ1[(logSZ1["time"] >= 91500000) & (logSZ1["time"] < 150000000)]
    
        columns = ["StockID", "cum_volume", "cum_amount", "close", "bid1p", "bid2p", "bid3p", "bid4p", "bid5p", "bid1q", "bid2q",
               "bid3q", "bid4q", "bid5q", "ask1p", "ask2p", "ask3p", "ask4p", "ask5p", "ask1q", "ask2q", "ask3q",
               "ask4q", "ask5q", "openPrice", "numTrades", "time"]
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
        for i in ["cum_amount", "close", "bid1p", "bid2p", "bid3p", "bid4p", "bid5p", "ask1p", "ask2p", "ask3p", "ask4p", "ask5p", "openPrice"]:
            data1_1[i] = (data1_1[i] * 10000).round(0)
    
        test = pd.merge(data1_1, data2_1, left_on=columns, right_on=columns, how="outer")
        print(datetime.datetime.now() - startTm)
        n1 = test["sequenceNo_x"].count()
        n2 = test["sequenceNo_y"].count()
        len1 = len(test)
        print(n1)
        print(n2)
        print(len1)
        re['date'].append(y)
        re['data'].append('SZ lv2 data ' + pp.split('/')[3][14:22])
        re['baseline'].append(n1)
        re['test'].append(n2)
        re['merge'].append(len1)
        if (n1 == len1) & (n2 == len1):
            re['time'].append(0)
            re['stock_list'].append(0)
        print("-----------------------------------------------")
        if n2 < len1:
            print("test is not complete:")
            print(test[np.isnan(test["sequenceNo_y"])])
            print(len(test[np.isnan(test["sequenceNo_y"])])/n1)
            print(np.sort(test[np.isnan(test["sequenceNo_y"])]["time"].unique()))
            print(len(np.sort(test[np.isnan(test["sequenceNo_y"])]["StockID"].unique())))
            print(np.sort(test[np.isnan(test["sequenceNo_y"])]["StockID"].unique()))
            re['time'].append(np.sort(test[np.isnan(test["sequenceNo_y"])]["time"].unique()))
            re['stock_list'].append(np.sort(test[np.isnan(test["sequenceNo_y"])]["StockID"].unique()))
        if (len1 == n2) & (n1 < len1):
            print("baseline is not complete:")
            print(test[np.isnan(test["sequenceNo_x"])])
            print(n2-n1)
            re['time'].append(np.sort(test[np.isnan(test["sequenceNo_x"])]["time"].unique()))
            re['stock_list'].append(np.sort(test[np.isnan(test["sequenceNo_x"])]["StockID"].unique()))
        del logSZ1
        del data1
        del data2
        del test
        del data1_1
        del data2_1
    del logSZ
    
    
    
    
    readPath = '/mnt/dailyRawData/logs_' + y + '_zs_***_day_pcap/mdOrderPcap_SZ_***'
    dataPathLs = np.array(glob.glob(readPath))
    path1 = np.array(glob.glob('/mnt/Kevin_zhenyu/rawData/logs_' + y + '_zs_92_01_day_data/mdOrderLog_*'))[0]
    OrderLogSZ = pd.read_csv(path1,
                        encoding="utf-8").loc[:, ["clockAtArrival", "sequenceNo", "exchId", "TransactTime",
                                                     "ApplSeqNum", "SecurityID", "Side", "OrderType", "Price",
                                                     "OrderQty"]]
    OrderLogSZ["OrderType"] = OrderLogSZ["OrderType"].apply(lambda x: str(x))
    
    for pp in dataPathLs:
        startTm = datetime.datetime.now()
        OrderLogSZ1 = pd.read_csv(pp)
        print(datetime.datetime.now() - startTm)
        
        OrderLogSZ1["SecurityID"] = OrderLogSZ1["ID"] - 2000000
        OrderLogSZ1 = OrderLogSZ1.rename(columns={"time":'TransactTime'})
              
        OrderLogSZ1["OrderType"] = np.where(OrderLogSZ1["OrderType"] == 2, '2', np.where(
            OrderLogSZ1["OrderType"] == 1, '1', OrderLogSZ1['OrderType']))
        
    
        print(len(OrderLogSZ["SecurityID"].unique()))
        print(len(OrderLogSZ1["SecurityID"].unique()))
        print(len(set(OrderLogSZ["SecurityID"].unique()) - set(OrderLogSZ1["SecurityID"].unique())))
        print(set(OrderLogSZ["SecurityID"].unique()) - set(OrderLogSZ1["SecurityID"].unique()))
    
    
        sl = list(set(OrderLogSZ["SecurityID"].unique()) & set(OrderLogSZ1['SecurityID'].unique()))
        OrderLogSZZ = OrderLogSZ[OrderLogSZ["SecurityID"].isin(sl)]
        OrderLogSZ1 = OrderLogSZ1[OrderLogSZ1["SecurityID"].isin(sl)]
        print(len(OrderLogSZZ["SecurityID"].unique()))
        print(len(OrderLogSZ1["SecurityID"].unique()))
        
        print('----------------------------------------------------------------')
        print('SZ order data:')
        print(pp.split('/')[3][14:22])
        
        columns = ["ApplSeqNum", "TransactTime", "Side", 'OrderType', 'Price', 'OrderQty', "SecurityID"]
        ree = pd.merge(OrderLogSZZ, OrderLogSZ1, on=columns, how="outer", validate='one_to_one')
        n1 = ree["sequenceNo_x"].count()
        n2 = ree["sequenceNo_y"].count()
        len1 = len(ree)
        print(n1)
        print(n2)
        print(len1)
        re['date'].append(y)
        re['data'].append('SZ order data ' + pp.split('/')[3][14:22])
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
            re['stock_list'].append(ree[np.isnan(ree["sequenceNo_y"])]["SecurityID"].unique())
        if (len1 == n2) & (n1 < len1):
            print("test is complete, baseline is not complete:")
            print(ree[np.isnan(ree["sequenceNo_x"])])
            print(np.sort(ree[np.isnan(ree["sequenceNo_x"])]["TransactTime"].unique()))
            print(len(ree[np.isnan(ree["sequenceNo_x"])]["SecurityID"].unique()))
            print(ree[np.isnan(ree["sequenceNo_x"])]["SecurityID"].unique())
            print(n2-n1)
            re['time'].append(np.sort(ree[np.isnan(ree["sequenceNo_x"])]["TransactTime"].unique()))
            re['stock_list'].append(ree[np.isnan(ree["sequenceNo_x"])]["SecurityID"].unique())
        del OrderLogSZ1
        del OrderLogSZZ
        del ree
    del OrderLogSZ    
    
    
    
    
    readPath = '/mnt/dailyRawData/logs_' + y + '_zt_***_day_pcap/mdTradePcap_SH_***'
    dataPathLs = np.array(glob.glob(readPath))
    
    path1 = np.array(glob.glob('/mnt/Kevin_zhenyu/rawData/logs_' + y + '_zs_92_01_day_data/mdTradeLog_*'))[0]
    SH = pd.read_csv(path1,
                        encoding="utf-8").loc[:, ["clockAtArrival", "sequenceNo", "exchId", "TransactTime",
                                                     "ApplSeqNum", "SecurityID", "ExecType", "TradeBSFlag",
                                                     "TradePrice", "TradeQty", "TradeMoney", "BidApplSeqNum",
                                                     "OfferApplSeqNum"]]
    SH = SH[SH["exchId"] == 1]
        
    for pp in dataPathLs:
        
        startTm = datetime.datetime.now()
        SH1 = pd.read_csv(pp)
        print(datetime.datetime.now() - startTm)
        
        SH1["SecurityID"] = SH1["ID"] - 1000000
        SH1 = SH1.rename(columns={"time":'TransactTime'})
        
        print(len(SH["SecurityID"].unique()))
        print(len(SH1["SecurityID"].unique()))
        print(len(set(SH["SecurityID"].unique()) - set(SH1["SecurityID"].unique())))
        print(set(SH["SecurityID"].unique()) - set(SH1["SecurityID"].unique()))
    
        
        sl = list(set(SH["SecurityID"].unique()) & set(SH1['SecurityID'].unique()))
        SHH = SH[SH["SecurityID"].isin(sl)]
        SH1 = SH1[SH1["SecurityID"].isin(sl)]
        print(len(SHH["SecurityID"].unique()))
        print(len(SH1["SecurityID"].unique()))
    
        print(SH1.columns)
        
        print('----------------------------------------------------------------')
        print('SH trade data:')
        print(pp.split('/')[3][14:22])
        
        SHH["ExecType"] = SHH["ExecType"].apply(lambda x: str(x))
        SH1["ExecType"] = 'F'
        columns = ["TransactTime", "ApplSeqNum", "SecurityID", "TradePrice", "TradeQty", "TradeMoney", "TradeBSFlag","ExecType",
               "BidApplSeqNum", "OfferApplSeqNum"]
        ree = pd.merge(SHH, SH1, left_on=columns, right_on=columns, how="outer", validate='one_to_one')
        n1 = ree["sequenceNo_x"].count()
        n2 = ree["sequenceNo_y"].count()
        len1 = len(ree)
        print(n1)
        print(n2)
        print(len1)
        re['date'].append(y)
        re['data'].append('SH trade data ' + pp.split('/')[3][14:22])
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
            re['stock_list'].append(ree[np.isnan(ree["sequenceNo_y"])]["SecurityID"].unique())
        if (len1 == n2) & (n1 < len1):
            print("baseline is not complete:")
            print(ree[np.isnan(ree["sequenceNo_x"])])
            print(np.sort(ree[np.isnan(ree["sequenceNo_x"])]["TransactTime"].unique()))
            print(len(ree[np.isnan(ree["sequenceNo_x"])]["SecurityID"].unique()))
            print(ree[np.isnan(ree["sequenceNo_x"])]["SecurityID"].unique())
            print(n2-n1)
            re['time'].append(np.sort(ree[np.isnan(ree["sequenceNo_x"])]["TransactTime"].unique()))
            re['stock_list'].append(ree[np.isnan(ree["sequenceNo_x"])]["SecurityID"].unique())
        del SHH
        del SH1
        del ree
    del SH
    
    
    
    
    
    readPath = '/mnt/dailyRawData/logs_' + y + '_zs_***_day_pcap/mdTradePcap_SZ_***'
    dataPathLs = np.array(glob.glob(readPath))
    startTm = datetime.datetime.now()
    path1 = np.array(glob.glob('/mnt/Kevin_zhenyu/rawData/logs_' + y + '_zs_92_01_day_data/mdTradeLog_*'))[0]
    
    TradeLogSZ = pd.read_csv(path1,
                        encoding="utf-8").loc[:, ["clockAtArrival", "sequenceNo", "exchId", "TransactTime",
                                                     "ApplSeqNum", "SecurityID", "ExecType", "TradeBSFlag",
                                                     "TradePrice", "TradeQty", "TradeMoney", "BidApplSeqNum",
                                                     "OfferApplSeqNum"]]
    TradeLogSZ = TradeLogSZ[TradeLogSZ["exchId"] != 1]
    print(datetime.datetime.now() - startTm)
    for pp in dataPathLs:
        startTm = datetime.datetime.now()
        TradeLogSZ1 = pd.read_csv(pp)
        print(datetime.datetime.now() - startTm)
        
        TradeLogSZ1["SecurityID"] = TradeLogSZ1["ID"] - 2000000
        TradeLogSZ1 = TradeLogSZ1.rename(columns={"time":'TransactTime'})
        TradeLogSZ1["TradeBSFlag"] = 'N'
          
        
        print(len(TradeLogSZ["SecurityID"].unique()))
        print(len(TradeLogSZ1["SecurityID"].unique()))
        print(len(set(TradeLogSZ["SecurityID"].unique()) - set(TradeLogSZ1["SecurityID"].unique())))
        print(set(TradeLogSZ["SecurityID"].unique()) - set(TradeLogSZ1["SecurityID"].unique()))
    
    
        sl = list(set(TradeLogSZ["SecurityID"].unique()) & set(TradeLogSZ1['SecurityID'].unique()))
        TradeLogSZZ = TradeLogSZ[TradeLogSZ["SecurityID"].isin(sl)]
        TradeLogSZ1 = TradeLogSZ1[TradeLogSZ1["SecurityID"].isin(sl)]
        print(len(TradeLogSZZ["SecurityID"].unique()))
        print(len(TradeLogSZ1["SecurityID"].unique()))
    
        print(TradeLogSZ1.columns)
        
        
        print('----------------------------------------------------------------')
        print('SZ trade data:')
        print(pp.split('/')[3][14:22])
        
        TradeLogSZZ["ExecType"] = TradeLogSZZ["ExecType"].apply(lambda x: str(x))
        TradeLogSZ1["ExecType"] = TradeLogSZ1["ExecType"].apply(lambda x: str(x))
    
        columns = ["TransactTime","ApplSeqNum", "SecurityID", "ExecType", "TradeBSFlag","TradePrice", "TradeQty", "TradeMoney", "BidApplSeqNum","OfferApplSeqNum"]
        ree = pd.merge(TradeLogSZZ, TradeLogSZ1, left_on=columns, right_on=columns, how="outer", validate='one_to_one')
        n1 = ree["sequenceNo_x"].count()
        n2 = ree["sequenceNo_y"].count()
        len1 = len(ree)
        print(n1)
        print(n2)
        print(len1)
        re['date'].append(y)
        re['data'].append('SZ trade data ' + pp.split('/')[3][14:22])
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
            re['stock_list'].append(ree[np.isnan(ree["sequenceNo_y"])]["SecurityID"].unique())
        if (len1 == n2) & (n1 < len1):
            print("baseline is not complete:")
            print(ree[np.isnan(ree["sequenceNo_x"])])
            print(np.sort(ree[np.isnan(ree["sequenceNo_x"])]["TransactTime"].unique()))
            print(len(ree[np.isnan(ree["sequenceNo_x"])]["SecurityID"].unique()))
            print(ree[np.isnan(ree["sequenceNo_x"])]["SecurityID"].unique())
            print(n2-n1)
            re['time'].append(np.sort(ree[np.isnan(ree["sequenceNo_x"])]["TransactTime"].unique()))
            re['stock_list'].append(ree[np.isnan(ree["sequenceNo_x"])]["SecurityID"].unique())
        del TradeLogSZZ
        del TradeLogSZ1
        del ree
    del TradeLogSZ
    
    print(re)
    print(re["date"])
    print(re["data"])
    print(re['baseline'])
    print(re['test'])
    print(re['merge'])
    print(re['time'])
    print(re['stock_list'])
    re = pd.DataFrame(re) 
    re.to_csv('/home/work516/result/new_record_data/' + y + ' 92.csv') 


if __name__ == '__main__':
    new_record_data_check()


