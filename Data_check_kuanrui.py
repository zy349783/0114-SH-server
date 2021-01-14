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

def KR_check():
    for y in [(datetime.datetime.today() + datetime.timedelta(days=-1)).strftime('%Y%m%d')]:
        print('----------------------------------------------------------------')
        print(y)
        
        re = {}
        for col in ['date', 'data', 'baseline', 'test', 'merge', 'time', 'stock_list']:
            re[col] = []
      
        readPath = '/mnt/Kevin_zhenyu/KR_daily_data/' + y + '/SH/snapshot/Level2/***'
        dataPathLs = np.array(glob.glob(readPath))
        dateLs = np.array([int(os.path.basename(i).split('.')[0]) for i in dataPathLs])
        dataPathLs = dataPathLs[(dateLs >= 600000) & (dateLs <= 700000)]
        logSH1 = []
        ll = []
        startTm = datetime.datetime.now()
        for i in dataPathLs:
            try:
                df = pd.read_csv(i)
            except:
                print("empty data")
                print(i)
                ll.append(int(os.path.basename(i).split('.')[0]))
                continue
            df["StockID"] = int(os.path.basename(i).split('.')[0])
            logSH1 += [df]
        del df
        logSH1 = pd.concat(logSH1).reset_index(drop=True)
        print(datetime.datetime.now() - startTm)
        
        for i in range(1, 6):
            if i == 1:
                logSH1["bid" + str(i) + "p"] = logSH1["BidPrice"].apply(lambda x: float(x.split(',')[0][1:]))
                logSH1["ask" + str(i) + "p"] = logSH1["OfferPrice"].apply(lambda x: float(x.split(',')[0][1:]))
                logSH1["bid" + str(i) + "q"] = logSH1["BidOrderQty"].apply(lambda x: int(x.split(',')[0][1:]))
                logSH1["ask" + str(i) + "q"] = logSH1["OfferOrderQty"].apply(lambda x: int(x.split(',')[0][1:]))
            else:
                logSH1["bid" + str(i) + "p"] = logSH1["BidPrice"].apply(lambda x: float(x.split(',')[i-1]))
                logSH1["ask" + str(i) + "p"] = logSH1["OfferPrice"].apply(lambda x: float(x.split(',')[i-1]))
                logSH1["bid" + str(i) + "q"] = logSH1["BidOrderQty"].apply(lambda x: int(x.split(',')[i-1]))
                logSH1["ask" + str(i) + "q"] = logSH1["OfferOrderQty"].apply(lambda x: int(x.split(',')[i-1]))
        logSH1 = logSH1.rename(columns={"Volume":"cum_volume", "Amount":"cum_amount", "LastPx":"close", "OpenPx":"openPrice",
                                        "NumTrades":"numTrades"})
        logSH1["time"] = (logSH1["QuotTime"] - int(y)*1000000000).astype(np.int64)
            
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
        print(len(ll))
        print(len(set(logSH2["StockID"].unique()) & set(ll)))
        
        print('----------------------------------------------------------------')
        print('SH lv2 data:')
        in_dex = [16, 300, 852, 905]
        data1 = logSH2[~logSH2["StockID"].isin(in_dex) & (logSH2["time"] >= 91500000) & (logSH2["time"] <= 150000000)               & (logSH2["source"] == 13)]
        data1["cum_amount"] = data1["cum_amount"].round(2)
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
        print(len(set(data1_1["StockID"].unique()) - set(data2_1["StockID"].unique()) - set(ll)))
        print(set(data1_1["StockID"].unique()) - set(data2_1["StockID"].unique()) - set(ll))
        print(len(set(data2_1["StockID"].unique()) - set(data1_1["StockID"].unique())))
        print(set(data2_1["StockID"].unique()) - set(data1_1["StockID"].unique()))
        if n1 != n2:
            sl = list(set(data1_1["StockID"].unique()) & set(data2_1["StockID"].unique()))
            data1_1 = data1_1[data1_1["StockID"].isin(sl)]
            data2_1 = data2_1[data2_1["StockID"].isin(sl)]
        
        test = pd.merge(data1_1, data2_1, left_on=columns, right_on=columns, how="outer")
        n1 = test["source"].count()
        n2 = test["IOPV"].count()
        len1 = len(test)
        re['date'].append(y)
        re['data'].append('SH lv2 data')
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
            print(test[np.isnan(test["IOPV"])])
            print(len(test[np.isnan(test["IOPV"])])/n1)
            print(len(test[np.isnan(test["IOPV"])]["time"].unique()))
            print(test[np.isnan(test["IOPV"])]["time"].unique())
            print(len(test[np.isnan(test["IOPV"])]["StockID"].unique()))
            print(test[np.isnan(test["IOPV"])]["StockID"].unique())
            re['time'].append(np.sort(test[np.isnan(test["IOPV"])]["time"].unique()))
            re['stock_list'].append(np.sort(test[np.isnan(test["IOPV"])]["StockID"].unique()))
        if (len1 == n2) & (n1 < len1):
            print("baseline is not complete:")
            print(test[np.isnan(test["source"])])
            print(n2-n1)
            re['time'].append(np.sort(test[np.isnan(test["source"])]["time"].unique()))
            re['stock_list'].append(np.sort(test[np.isnan(test["source"])]["StockID"].unique()))
            print((n2-n1)/n1)
        del logSH2
        del data1
        del data2
        del test
        del data1_1
        del data2_1
        
        
        print('----------------------------------------------------------------')
        print('SH index data:')
    
        readPath = '/mnt/Kevin_zhenyu/KR_daily_data/' + y + '/SH/snapshot/Level2/***'
        dataPathLs = np.array(glob.glob(readPath))
        dateLs = np.array([int(os.path.basename(i).split('.')[0]) for i in dataPathLs])
        dataPathLs = dataPathLs[(dateLs == 16) | (dateLs == 300) | (dateLs == 852) | (dateLs == 905)]
        index = []
        
        startTm = datetime.datetime.now()
        for i in dataPathLs:
            df = pd.read_csv(i)
            df["StockID"] = int(os.path.basename(i).split('.')[0])
            index += [df]
        index = pd.concat(index).reset_index(drop=True)
        print(datetime.datetime.now() - startTm)
        
        for i in range(1, 6):
            if i == 1:
                index["bid" + str(i) + "p"] = index["BidPrice"].apply(lambda x: float(x.split(',')[0][1:]))
                index["ask" + str(i) + "p"] = index["OfferPrice"].apply(lambda x: float(x.split(',')[0][1:]))
                index["bid" + str(i) + "q"] = index["BidOrderQty"].apply(lambda x: int(x.split(',')[0][1:]))
                index["ask" + str(i) + "q"] = index["OfferOrderQty"].apply(lambda x: int(x.split(',')[0][1:]))
            else:
                index["bid" + str(i) + "p"] = index["BidPrice"].apply(lambda x: float(x.split(',')[i-1]))
                index["ask" + str(i) + "p"] = index["OfferPrice"].apply(lambda x: float(x.split(',')[i-1]))
                index["bid" + str(i) + "q"] = index["BidOrderQty"].apply(lambda x: int(x.split(',')[i-1]))
                index["ask" + str(i) + "q"] = index["OfferOrderQty"].apply(lambda x: int(x.split(',')[i-1]))
        index = index.rename(columns={"Volume":"cum_volume", "Amount":"cum_amount", "LastPx":"close", "OpenPx":"openPrice",
                                        "NumTrades":"numTrades"})
        index["time"] = (index["SendingTime"] - int(y)*1000000000).astype(np.int64)
    #     index["time1"] = (index["SendingTime"] - int(y)*1000000000).astype(np.int64)
        
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
        
        print(index["StockID"].unique())
        
        in_dex = [16, 300, 852, 905]
        data1 = logSH[(logSH["StockID"].isin(in_dex)) & (logSH["time"] >= 91500000) & (logSH["time"] <= 150000000)]
        data2 = index[(index["StockID"].isin(in_dex)) & (index["time"] >= 91500000) & (index["time"] <= 150000000)]
    
        data2["close"] = data2["close"].round(4)
        data1["close"] = data1["close"].round(4)
        data2["openPrice"] = data2["openPrice"].round(4)
        data1["openPrice"] = data1["openPrice"].round(4)
        data2["cum_amount"] = data2["cum_amount"].round(1)
        data1["cum_amount"] = data1["cum_amount"].round(1)
    
        columns = ["StockID", "cum_volume", "cum_amount", "close", "openPrice"]
        data1_1 = data1.drop_duplicates(subset=columns, keep="first").reset_index()
        data2_1 = data2.drop_duplicates(subset=columns, keep="first").reset_index()
        
        test = pd.merge(data1_1, data2_1, left_on=columns, right_on=columns, how="outer")
        n1 = test["source"].count()
        n2 = test["IOPV"].count()
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
            print(test[np.isnan(test["IOPV"])])
            re['time'].append(np.sort(test[np.isnan(test["IOPV"])]['time_x'].unique()))
            re['stock_list'].append(np.sort(test[np.isnan(test['IOPV'])]['StockID'].unique()))
        if (n2 == len1) & (n1 < len1):
            print("baseline is not complete::")
            print(test[np.isnan(test["source"])])
            re['time'].append(np.sort(test[np.isnan(test['source'])]['time_y'].unique()))
            re['stock_list'].append(np.sort(test[np.isnan(test['source'])]['StockID'].unique()))
        
        columns = ["StockID", "cum_volume", "cum_amount", "close", "openPrice", "time"]
        test = pd.merge(data1_1, data2_1, left_on=columns, right_on=columns, how="outer")
        n1 = test["source"].count()
        n2 = test["IOPV"].count()
        len1 = len(test)
        print(n1)
        print(n2)
        print(len1)
        re['date'].append(y)
        re['data'].append('SH index data with time column')
        re['baseline'].append(n1)
        re['test'].append(n2)
        re['merge'].append(len1)
        if (n1 == len1) & (n2 == len1):
            re['time'].append(0)
            re['stock_list'].append(0)
        if n2 < len1:
            print("test is not complete:")
            print(test[np.isnan(test["IOPV"])])
            re['time'].append(np.sort(test[np.isnan(test["IOPV"])]['time'].unique()))
            re['stock_list'].append(np.sort(test[np.isnan(test['IOPV'])]['StockID'].unique()))
        if (n2 == len1) & (n1 < len1):
            print("baseline is not complete::")
            print(test[np.isnan(test["source"])])
            re['time'].append(np.sort(test[np.isnan(test['source'])]['time'].unique()))
            re['stock_list'].append(np.sort(test[np.isnan(test['source'])]['StockID'].unique()))
        
        del index
        del logSH
        del data1
        del data2
        del test
        del data1_1
        del data2_1
    
        
        print('----------------------------------------------------------------')
        print('SZ lv2 data:')
        
        readPath = '/mnt/Kevin_zhenyu/KR_daily_data/' + y + '/SZ/snapshot/Level2/***'
        dataPathLs = np.array(glob.glob(readPath))
        dateLs = np.array([int(os.path.basename(i).split('.')[0]) for i in dataPathLs])
        dataPathLs = dataPathLs[(dateLs < 4000) | ((dateLs > 300000) & (dateLs < 310000))]
        logSZ1 = []
        ll = []
        startTm = datetime.datetime.now()
        for i in dataPathLs:
            try:
                df = pd.read_csv(i)
            except:
                print("empty data")
                print(i)
                ll.append(int(os.path.basename(i).split('.')[0]))
                continue
            df["StockID"] = int(os.path.basename(i).split('.')[0])
            logSZ1 += [df]
        del df
        logSZ1 = pd.concat(logSZ1).reset_index(drop=True)
        print(datetime.datetime.now() - startTm)
        for i in range(1, 6):
            if i == 1:
                logSZ1["bid" + str(i) + "p"] = logSZ1["BidPrice"].apply(lambda x: float(x.split(',')[0][1:]))
                logSZ1["ask" + str(i) + "p"] = logSZ1["OfferPrice"].apply(lambda x: float(x.split(',')[0][1:]))
                logSZ1["bid" + str(i) + "q"] = logSZ1["BidOrderQty"].apply(lambda x: int(x.split(',')[0][1:]))
                logSZ1["ask" + str(i) + "q"] = logSZ1["OfferOrderQty"].apply(lambda x: int(x.split(',')[0][1:]))
            else:
                logSZ1["bid" + str(i) + "p"] = logSZ1["BidPrice"].apply(lambda x: float(x.split(',')[i-1]))
                logSZ1["ask" + str(i) + "p"] = logSZ1["OfferPrice"].apply(lambda x: float(x.split(',')[i-1]))
                logSZ1["bid" + str(i) + "q"] = logSZ1["BidOrderQty"].apply(lambda x: int(x.split(',')[i-1]))
                logSZ1["ask" + str(i) + "q"] = logSZ1["OfferOrderQty"].apply(lambda x: int(x.split(',')[i-1]))
        logSZ1 = logSZ1.rename(columns={"Volume":"cum_volume", "Amount":"cum_amount", "LastPx":"close", "OpenPx":"openPrice",
                                        "NumTrades":"numTrades"})
        logSZ1["time"] = (logSZ1["QuotTime"] - int(y)*1000000000).astype(np.int64)
        print(datetime.datetime.now() - startTm)
        
        startTm = datetime.datetime.now()
        logSZ = pd.read_csv(path2,
                        encoding="utf-8").loc[:, ["clockAtArrival", "sequenceNo", "source", "StockID",
                                                  "exchange", "time", "cum_volume", "cum_amount", "close",
                                                  "bid1p", "bid2p", "bid3p", "bid4p", "bid5p", "bid1q",
                                                  "bid2q", "bid3q", "bid4q", "bid5q", "ask1p", "ask2p",
                                                  "ask3p", "ask4p", "ask5p", "ask1q", "ask2q", "ask3q",
                                                  "ask4q", "ask5q", "openPrice", "numTrades"]]
        logSZ["time"] = logSZ["time"].apply(lambda x: int((x.replace(':', "")).replace(".", "")))
        print(len(ll))
        print(len(set(logSZ["StockID"].unique()) & set(ll)))
        print(datetime.datetime.now() - startTm)
        
        startTm = datetime.datetime.now()
        data1 = logSZ[(logSZ["time"] >= 91500000) & (logSZ["time"] < 150000000) & (logSZ["source"] == 4)]
        data2 = logSZ1[(logSZ1["time"] >= 91500000) & (logSZ1["time"] < 150000000)]
    
        columns = ["StockID", "cum_volume", "cum_amount", "close", "bid1p", "bid2p", "bid3p", "bid4p", "bid5p", "bid1q", "bid2q",
                "bid3q", "bid4q", "bid5q", "ask1p", "ask2p", "ask3p", "ask4p", "ask5p", "ask1q", "ask2q", "ask3q",
                "ask4q", "ask5q", "openPrice", "numTrades", "time"]
        data1_1 = data1.drop_duplicates(subset=columns, keep="first").reset_index()
        data2_1 = data2.drop_duplicates(subset=columns, keep="first").reset_index()
    #     data1_1["cum_amount"] = (data1_1["cum_amount"]*1000).round(0)
    #     data1_1["bid1p"] = (data1_1["bid1p"]*1000).round(0)
    #     data1_1["bid2p"] = (data1_1["bid2p"]*1000).round(0)
    #     data1_1["bid3p"] = (data1_1["bid3p"]*1000).round(0)
    #     data1_1["bid4p"] = (data1_1["bid4p"]*1000).round(0)
    #     data1_1["bid5p"] = (data1_1["bid5p"]*1000).round(0)
    #     data1_1["ask1p"] = (data1_1["ask1p"]*1000).round(0)
    #     data1_1["ask2p"] = (data1_1["ask2p"]*1000).round(0)
    #     data1_1["ask3p"] = (data1_1["ask3p"]*1000).round(0)
    #     data1_1["ask4p"] = (data1_1["ask4p"]*1000).round(0)
    #     data1_1["ask5p"] = (data1_1["ask5p"]*1000).round(0)
    #     data1_1["close"] = (data1_1["close"]*1000).round(0)
    #     data1_1["openPrice"] = (data1_1["openPrice"]*1000).round(0)
        data1_1["cum_amount"] = data1_1["cum_amount"].round(2)
        n1 = len(data1_1["StockID"].unique())
        n2 = len(data2_1["StockID"].unique())
        print(n1)
        print(n2)
        print(len(set(data1_1["StockID"].unique()) - set(data2_1["StockID"].unique()) - set(ll)))
        print(set(data1_1["StockID"].unique()) - set(data2_1["StockID"].unique()) - set(ll))
        print(len(set(data2_1["StockID"].unique()) - set(data1_1["StockID"].unique())))
        print(set(data2_1["StockID"].unique()) - set(data1_1["StockID"].unique()))
        if n1 != n2:
            sl = list(set(data1_1["StockID"].unique()) & set(data2_1["StockID"].unique()))
            data1_1 = data1_1[data1_1["StockID"].isin(sl)]
            data2_1 = data2_1[data2_1["StockID"].isin(sl)]
        test = pd.merge(data1_1, data2_1, left_on=columns, right_on=columns, how="outer")
        print(datetime.datetime.now() - startTm)
        n1 = test["source"].count()
        n2 = test["ImageStatus"].count()
        len1 = len(test)
        print(n1)
        print(n2)
        print(len1)
        re['date'].append(y)
        re['data'].append('SZ lv2 data')
        re['baseline'].append(n1)
        re['test'].append(n2)
        re['merge'].append(len1)
        if (n1 == len1) & (n2 == len1):
            re['time'].append(0)
            re['stock_list'].append(0)
        print("-----------------------------------------------")
        if n2 < len1:
            print("test is not complete:")
            print(test[np.isnan(test["ImageStatus"])])
            print(len(test[np.isnan(test["ImageStatus"])])/n1)
            print(np.sort(test[np.isnan(test["ImageStatus"])]["time"].unique()))
            print(len(np.sort(test[np.isnan(test["ImageStatus"])]["StockID"].unique())))
            print(np.sort(test[np.isnan(test["ImageStatus"])]["StockID"].unique()))
            re['time'].append(np.sort(test[np.isnan(test["ImageStatus"])]["time"].unique()))
            re['stock_list'].append(np.sort(test[np.isnan(test["ImageStatus"])]["StockID"].unique()))
        if (len1 == n2) & (n1 < len1):
            print("baseline is not complete:")
            print(test[np.isnan(test["source"])])
            print(n2-n1)
            re['time'].append(np.sort(test[np.isnan(test["source"])]["time"].unique()))
            re['stock_list'].append(np.sort(test[np.isnan(test["source"])]["StockID"].unique()))
        del logSZ
        del logSZ1
        del data1
        del data2
        del test
        del data1_1
        del data2_1
        
        
        
        
        
        readPath = '/mnt/Kevin_zhenyu/KR_daily_data/' + y + '/SZ/order/***'
        dataPathLs = np.array(glob.glob(readPath))
        dateLs = np.array([int(os.path.basename(i).split('.')[0]) for i in dataPathLs])
        dataPathLs = dataPathLs[(dateLs < 4000) | ((dateLs > 300000) & (dateLs < 310000))]
        OrderLogSZ1 = []
        ll = []
        
        startTm = datetime.datetime.now()
        for i in dataPathLs:
            try:
                df = pd.read_csv(i, encoding='GBK')
            except:
                print("empty data")
                print(i)
                ll.append(int(os.path.basename(i).split('.')[0]))
                continue
            df["SecurityID"] = int(os.path.basename(i).split('.')[0])
            OrderLogSZ1 += [df]
        OrderLogSZ1 = pd.concat(OrderLogSZ1).reset_index(drop=True)
        print(datetime.datetime.now() - startTm)
        OrderLogSZ1 = OrderLogSZ1.rename(columns={"OrdType": "OrderType"})
        OrderLogSZ1["Price"] = OrderLogSZ1["Price"]*10000
        OrderLogSZ1["Price"] = OrderLogSZ1["Price"].round(0)
        OrderLogSZ1["TransactTime"] = (OrderLogSZ1["TransactTime"] - int(y) * 1000000000).astype(np.int64)
    #     OrderLogSZ1 = OrderLogSZ1[OrderLogSZ1["Side"] != 'F']
        print(OrderLogSZ1["Side"].unique())
        print(OrderLogSZ1["ChannelNo"].unique())
        OrderLogSZ1["Side"] = np.where(OrderLogSZ1["Side"] == '1', 1, np.where(
        OrderLogSZ1["Side"] == '2', 2, OrderLogSZ1["Side"]))
        print(OrderLogSZ1[((OrderLogSZ1["Side"] != 1) & (OrderLogSZ1["Side"] != 2)) | (OrderLogSZ1["OrderType"].isnull())])
    
    
        path1 = np.array(glob.glob('/mnt/Kevin_zhenyu/rawData/logs_' + y + '_zs_92_01_day_data/mdOrderLog_*'))[0]
        OrderLogSZ = pd.read_csv(path1,
                        encoding="utf-8").loc[:, ["clockAtArrival", "sequenceNo", "exchId", "TransactTime",
                                                      "ApplSeqNum", "SecurityID", "Side", "OrderType", "Price",
                                                      "OrderQty"]]
        OrderLogSZ["OrderType"] = OrderLogSZ["OrderType"].apply(lambda x: str(x))
        
        OrderLogSZ1["OrderType"] = np.where(OrderLogSZ1["OrderType"] == 2, '2', np.where(
            OrderLogSZ1["OrderType"] == 1, '1', OrderLogSZ1['OrderType']))
        
        print(len(ll))
        print(len(set(OrderLogSZ["SecurityID"].unique()) & set(ll)))
        print(len(set(OrderLogSZ["SecurityID"].unique()) - set(OrderLogSZ1["SecurityID"].unique()) - set(ll)))
        print(len(OrderLogSZ["SecurityID"].unique()))
        print(len(OrderLogSZ1["SecurityID"].unique()))
        print(set(OrderLogSZ["SecurityID"].unique()) - set(OrderLogSZ1["SecurityID"].unique()) - set(ll))
        print(set(OrderLogSZ1["SecurityID"].unique()) - set(OrderLogSZ["SecurityID"].unique()))
    
    
        sl = list(set(OrderLogSZ["SecurityID"].unique()) & set(OrderLogSZ1['SecurityID'].unique()))
        OrderLogSZ = OrderLogSZ[OrderLogSZ["SecurityID"].isin(sl)]
        OrderLogSZ1 = OrderLogSZ1[OrderLogSZ1["SecurityID"].isin(sl)]
        print(len(OrderLogSZ["SecurityID"].unique()))
        print(len(OrderLogSZ1["SecurityID"].unique()))
        
        print('----------------------------------------------------------------')
        print('SZ order data:')
        
        OrderLogSZ1 = OrderLogSZ1[OrderLogSZ1["ChannelNo"] != 4001]
        columns = ["ApplSeqNum", "TransactTime","Side",'OrderType', 'Price', 'OrderQty', "SecurityID"]
        ree = pd.merge(OrderLogSZ, OrderLogSZ1, on=columns, how="outer", validate='one_to_one')
        n1 = ree["sequenceNo"].count()
        n2 = ree["SendingTime"].count()
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
            print(ree[np.isnan(ree["SendingTime"])])
            print(len(ree[np.isnan(ree["SendingTime"])]))
            print(np.sort(ree[np.isnan(ree["SendingTime"])]["TransactTime"].unique()))
            print(len(ree[np.isnan(ree["SendingTime"])]["SecurityID"].unique()))
            print(ree[np.isnan(ree["SendingTime"])]["SecurityID"].unique())
            re['time'].append(np.sort(ree[np.isnan(ree["SendingTime"])]["TransactTime"].unique()))
            re['stock_list'].append(np.sort(ree[np.isnan(ree["SendingTime"])]["SecurityID"].unique()))
        if (len1 == n2) & (n1 < len1):
            print("test is complete, baseline is not complete:")
            print(ree[np.isnan(ree["sequenceNo"]) & (~ree["OrderType"].isnull())])
            print(ree[np.isnan(ree["sequenceNo"])])
            print(np.sort(ree[np.isnan(ree["sequenceNo"]) & (~ree["OrderType"].isnull())]["TransactTime"].unique()))
            print(len(ree[np.isnan(ree["sequenceNo"]) & (~ree["OrderType"].isnull())]["SecurityID"].unique()))
            print(ree[np.isnan(ree["sequenceNo"]) & (~ree["OrderType"].isnull())]["SecurityID"].unique())
            re['time'].append(np.sort(ree[np.isnan(ree["sequenceNo"]) & (~ree["OrderType"].isnull())]["TransactTime"].unique()))
            re['stock_list'].append(np.sort(ree[np.isnan(ree["sequenceNo"]) & (~ree["OrderType"].isnull())]["SecurityID"].unique()))
            print(n2-n1)
        del OrderLogSZ
        del OrderLogSZ1
        del ree
        
        
        
        
        
        readPath = '/mnt/Kevin_zhenyu/KR_daily_data/' + y + '/SH/tick/***'
        dataPathLs = np.array(glob.glob(readPath))
        dateLs = np.array([int(os.path.basename(i).split('.')[0]) for i in dataPathLs])
        dataPathLs = dataPathLs[(dateLs >= 600000) & (dateLs <= 700000)]
        SH1 = []
        ll = []
        
        startTm = datetime.datetime.now()
        for i in dataPathLs:
            try:
                df = pd.read_csv(i)
            except:
                print("empty data")
                print(i)
                ll.append(int(os.path.basename(i).split('.')[0]))
                continue
            df["SecurityID"] = int(os.path.basename(i).split('.')[0])
            SH1 += [df]
        SH1 = pd.concat(SH1).reset_index(drop=True)
        print(datetime.datetime.now() - startTm)
        SH1["TransactTime"] = (SH1["TradeTime"] - int(y) * 1000000000).astype(np.int64)
        SH1["TradePrice"] = SH1["TradePrice"] * 10000
        SH1["TradePrice"] = SH1["TradePrice"].round(0)
        SH1["TradeMoney"] = SH1["TradeAmount"] * 10000
        SH1["TradeMoney"] = SH1["TradeMoney"].round(0)
        SH1["ExecType"] = 'F'
        SH1 = SH1.rename(columns={"TradeIndex":"ApplSeqNum", "BuyNo":"BidApplSeqNum", "SellNo":"OfferApplSeqNum"})
    
        path1 = np.array(glob.glob('/mnt/Kevin_zhenyu/rawData/logs_' + y + '_zs_92_01_day_data/mdTradeLog_*'))[0]
        SH = pd.read_csv(path1,
                        encoding="utf-8").loc[:, ["clockAtArrival", "sequenceNo", "exchId", "TransactTime",
                                                      "ApplSeqNum", "SecurityID", "ExecType", "TradeBSFlag",
                                                      "TradePrice", "TradeQty", "TradeMoney", "BidApplSeqNum",
                                                      "OfferApplSeqNum"]]
        SH = SH[SH["exchId"] == 1]
        
        print(len(ll))
        print(len(set(SH["SecurityID"].unique()) & set(ll)))
        print(len(SH["SecurityID"].unique()))
        print(len(SH1["SecurityID"].unique()))
        print(len(set(SH["SecurityID"].unique()) - set(SH1["SecurityID"].unique()) - set(ll)))
        print(set(SH["SecurityID"].unique()) - set(SH1["SecurityID"].unique()) - set(ll))
        print(len(set(SH1["SecurityID"].unique()) - set(SH["SecurityID"].unique())))
        print(set(SH1["SecurityID"].unique()) - set(SH["SecurityID"].unique()))
        
        sl = list(set(SH["SecurityID"].unique()) & set(SH1['SecurityID'].unique()))
        SH = SH[SH["SecurityID"].isin(sl)]
        SH1 = SH1[SH1["SecurityID"].isin(sl)]
        print(len(SH["SecurityID"].unique()))
        print(len(SH1["SecurityID"].unique()))
    
        print(SH1.columns)
        
        print('----------------------------------------------------------------')
        print('SH trade data:')
        
        SH["ExecType"] = SH["ExecType"].apply(lambda x: str(x))
        SH1["ExecType"] = SH1["ExecType"].apply(lambda x: str(x))
        columns = ["TransactTime", "ApplSeqNum", "SecurityID", "TradePrice", "TradeQty", "TradeMoney", "TradeBSFlag","ExecType",
                "BidApplSeqNum", "OfferApplSeqNum"]
        ree = pd.merge(SH, SH1, left_on=columns, right_on=columns, how="outer", validate='one_to_one')
        n1 = ree["sequenceNo"].count()
        n2 = ree["TradeTime"].count()
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
            print(ree[np.isnan(ree["TradeTime"])])
            print(len(ree[np.isnan(ree["TradeTime"])]))
            print(np.sort(ree[np.isnan(ree["TradeTime"])]["TransactTime"].unique()))
            print(len(ree[np.isnan(ree["TradeTime"])]["SecurityID"].unique()))
            print(ree[np.isnan(ree["TradeTime"])]["SecurityID"].unique())
            re['time'].append(np.sort(ree[np.isnan(ree["TradeTime"])]["TransactTime"].unique()))
            re['stock_list'].append(np.sort(ree[np.isnan(ree["TradeTime"])]["SecurityID"].unique()))
        if (len1 == n2) & (n1 < len1):
            print("baseline is not complete:")
            print(ree[np.isnan(ree["sequenceNo"])])
            print(np.sort(ree[np.isnan(ree["sequenceNo"])]["TransactTime"].unique()))
            print(len(ree[np.isnan(ree["sequenceNo"])]["SecurityID"].unique()))
            print(ree[np.isnan(ree["sequenceNo"])]["SecurityID"].unique())
            print(n2-n1)
            re['time'].append(np.sort(ree[np.isnan(ree["sequenceNo"])]["TransactTime"].unique()))
            re['stock_list'].append(np.sort(ree[np.isnan(ree["sequenceNo"])]["SecurityID"].unique()))
        del SH
        del SH1
        del ree
        
        
        
        
        
        
        readPath = '/mnt/Kevin_zhenyu/KR_daily_data/' + y + '/SZ/tick/***'
        dataPathLs = np.array(glob.glob(readPath))
        dateLs = np.array([int(os.path.basename(i).split('.')[0]) for i in dataPathLs])
        dataPathLs = dataPathLs[(dateLs < 4000) | ((dateLs > 300000) & (dateLs < 310000))]
        TradeLogSZ1 = []
        ll = []
        
        startTm = datetime.datetime.now()
        for i in dataPathLs:
            try:
                df = pd.read_csv(i)
            except:
                print("empty data")
                print(i)
                ll.append(int(os.path.basename(i).split('.')[0]))
                continue
            df["SecurityID"] = int(os.path.basename(i).split('.')[0])
            TradeLogSZ1 += [df]
        TradeLogSZ1 = pd.concat(TradeLogSZ1).reset_index(drop=True)
        print(datetime.datetime.now() - startTm)
        TradeLogSZ1["TransactTime"] = (TradeLogSZ1["TransactTime"] - int(y) * 1000000000).astype(np.int64)
        TradeLogSZ1["TradePrice"] = TradeLogSZ1["Price"] * 10000
        TradeLogSZ1["TradePrice"] = TradeLogSZ1["TradePrice"].round(0)
        TradeLogSZ1 = TradeLogSZ1.rename(columns={"Qty":"TradeQty"})
        TradeLogSZ1["TradeMoney"] = (TradeLogSZ1["TradePrice"] * TradeLogSZ1["TradeQty"]).astype(np.int64)
        TradeLogSZ1["TradeBSFlag"] = 'N'
        
        startTm = datetime.datetime.now()
        path1 = np.array(glob.glob('/mnt/Kevin_zhenyu/rawData/logs_' + y + '_zs_92_01_day_data/mdTradeLog_*'))[0]
    
        TradeLogSZ = pd.read_csv(path1,
                        encoding="utf-8").loc[:, ["clockAtArrival", "sequenceNo", "exchId", "TransactTime",
                                                     "ApplSeqNum", "SecurityID", "ExecType", "TradeBSFlag",
                                                     "TradePrice", "TradeQty", "TradeMoney", "BidApplSeqNum",
                                                     "OfferApplSeqNum"]]
        TradeLogSZ = TradeLogSZ[TradeLogSZ["exchId"] != 1]
        print(datetime.datetime.now() - startTm)
        
        
        print(len(ll))
        print(len(set(TradeLogSZ["SecurityID"].unique()) & set(ll)))
        print(len(TradeLogSZ["SecurityID"].unique()))
        print(len(TradeLogSZ1["SecurityID"].unique()))
        print(len(set(TradeLogSZ["SecurityID"].unique()) - set(TradeLogSZ1["SecurityID"].unique()) - set(ll)))
        print(set(TradeLogSZ["SecurityID"].unique()) - set(TradeLogSZ1["SecurityID"].unique()) - set(ll))
        print(len(set(TradeLogSZ1["SecurityID"].unique()) - set(TradeLogSZ["SecurityID"].unique())))
        print(set(TradeLogSZ1["SecurityID"].unique()) - set(TradeLogSZ["SecurityID"].unique()))
    
        sl = list(set(TradeLogSZ["SecurityID"].unique()) & set(TradeLogSZ1['SecurityID'].unique()))
        TradeLogSZ = TradeLogSZ[TradeLogSZ["SecurityID"].isin(sl)]
        TradeLogSZ1 = TradeLogSZ1[TradeLogSZ1["SecurityID"].isin(sl)]
        print(len(TradeLogSZ["SecurityID"].unique()))
        print(len(TradeLogSZ1["SecurityID"].unique()))
    
        print(TradeLogSZ1.columns)
        
        
        print('----------------------------------------------------------------')
        print('SZ trade data:')
        
        TradeLogSZ["ExecType"] = TradeLogSZ["ExecType"].apply(lambda x: str(x))
        TradeLogSZ1["ExecType"] = TradeLogSZ1["ExecType"].apply(lambda x: str(x))
    
        columns = ["TransactTime","ApplSeqNum", "SecurityID", "ExecType", "TradeBSFlag","TradePrice", "TradeQty", "TradeMoney", "BidApplSeqNum","OfferApplSeqNum"]
        ree = pd.merge(TradeLogSZ, TradeLogSZ1, left_on=columns, right_on=columns, how="outer", validate='one_to_one')
        n1 = ree["sequenceNo"].count()
        n2 = ree["Price"].count()
        len1 = len(ree)
        print(n1)
        print(n2)
        print(len1)
        re['date'].append(y)
        re['data'].append('SZ trade data')
        re['baseline'].append(n1)
        re['test'].append(n2)
        re['merge'].append(len1)
        if (n1 == len1) & (n2 == len1):
            re['time'].append(0)
            re['stock_list'].append(0)
        print("-----------------------------------------------")
        if n2 < len1:
            print("test is not complete:")
            print(ree[np.isnan(ree["Price"])])
            print(len(ree[np.isnan(ree["Price"])]))
            print(np.sort(ree[np.isnan(ree["Price"])]["TransactTime"].unique()))
            print(len(ree[np.isnan(ree["Price"])]["SecurityID"].unique()))
            print(ree[np.isnan(ree["Price"])]["SecurityID"].unique())
            re['time'].append(np.sort(ree[np.isnan(ree["Price"])]["TransactTime"].unique()))
            re['stock_list'].append(ree[np.isnan(ree["Price"])]["SecurityID"].unique())
        if (len1 == n2) & (n1 < len1):
            print("baseline is not complete:")
            print(ree[np.isnan(ree["sequenceNo"])])
            print(np.sort(ree[np.isnan(ree["sequenceNo"])]["TransactTime"].unique()))
            print(len(ree[np.isnan(ree["sequenceNo"])]["SecurityID"].unique()))
            print(ree[np.isnan(ree["sequenceNo"])]["SecurityID"].unique())
            print(n2-n1)
            re['time'].append(np.sort(ree[np.isnan(ree["sequenceNo"])]["TransactTime"].unique()))
            re['stock_list'].append(ree[np.isnan(ree["sequenceNo"])]["SecurityID"].unique())
        del TradeLogSZ
        del TradeLogSZ1
        del ree
    
    re = pd.DataFrame(re) 
    re.to_csv('/home/work516/result/KR_data/' + y + ' 92.csv') 


if __name__ == '__main__':
    KR_check()
