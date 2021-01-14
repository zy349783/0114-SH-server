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

for y in ['20200817']:
    print('----------------------------------------------------------------')
    print(y)

    re = {}
    for col in ['date', 'data', 'baseline', 'test', 'merge', 'time', 'stock_list']:
        re[col] = []

    # readPath = 'F:\\data\\' + y + '\\***_zs_96_03_day_96data\\mdLog_SH_***'
    # dataPathLs = np.array(glob.glob(readPath))
    # startTm = datetime.datetime.now()
    # logSH1 = pd.read_csv(dataPathLs[0])
    # print(datetime.datetime.now() - startTm)
    #
    # logSH1 = logSH1[["sequenceNo", "StockID", "source", "time", "cum_volume", "cum_amount", "close",
    #                  "bid1p", "bid2p", "bid3p", "bid4p", "bid5p", "bid1q", "bid2q", "bid3q",
    #                  "bid4q", "bid5q", "ask1p", "ask2p", "ask3p", "ask4p", "ask5p", "ask1q",
    #                  "ask2q", "ask3q", "ask4q", "ask5q", "openPrice", "numTrades"]]
    # logSH1["time"] = logSH1["time"].apply(lambda x: int(x.replace(':', "").replace('.', "")))

    # readPath = 'A:\\KR_daily_data\\' + y + '\\SH\\snapshot\\Level2\\***'
    # dataPathLs = np.array(glob.glob(readPath))
    # dateLs = np.array([int(os.path.basename(i).split('.')[0]) for i in dataPathLs])
    # dataPathLs = dataPathLs[(dateLs >= 600000) & (dateLs <= 700000)]
    # logSH2 = []
    # ll = []
    # startTm = datetime.datetime.now()
    # for i in dataPathLs:
    #     try:
    #         df = pd.read_csv(i)
    #     except:
    #         print("empty data")
    #         print(i)
    #         ll.append(int(os.path.basename(i).split('.')[0]))
    #         continue
    #     df["StockID"] = int(os.path.basename(i).split('.')[0])
    #     logSH2 += [df]
    # del df
    # logSH2 = pd.concat(logSH2).reset_index(drop=True)
    # print(datetime.datetime.now() - startTm)
    #
    # for i in range(1, 6):
    #     if i == 1:
    #         logSH2["bid" + str(i) + "p"] = logSH2["BidPrice"].apply(lambda x: float(x.split(',')[0][1:]))
    #         logSH2["ask" + str(i) + "p"] = logSH2["OfferPrice"].apply(lambda x: float(x.split(',')[0][1:]))
    #         logSH2["bid" + str(i) + "q"] = logSH2["BidOrderQty"].apply(lambda x: int(x.split(',')[0][1:]))
    #         logSH2["ask" + str(i) + "q"] = logSH2["OfferOrderQty"].apply(lambda x: int(x.split(',')[0][1:]))
    #     else:
    #         logSH2["bid" + str(i) + "p"] = logSH2["BidPrice"].apply(lambda x: float(x.split(',')[i - 1]))
    #         logSH2["ask" + str(i) + "p"] = logSH2["OfferPrice"].apply(lambda x: float(x.split(',')[i - 1]))
    #         logSH2["bid" + str(i) + "q"] = logSH2["BidOrderQty"].apply(lambda x: int(x.split(',')[i - 1]))
    #         logSH2["ask" + str(i) + "q"] = logSH2["OfferOrderQty"].apply(lambda x: int(x.split(',')[i - 1]))
    # logSH2 = logSH2.rename(
    #     columns={"Volume": "cum_volume", "Amount": "cum_amount", "LastPx": "close", "OpenPx": "openPrice",
    #              "NumTrades": "numTrades"})
    # logSH2["time"] = (logSH2["QuotTime"] - int(y) * 1000000000).astype(np.int64)
    #
    # print('----------------------------------------------------------------')
    # print('SH lv2 data:')
    # in_dex = [16, 300, 852, 905]
    # data1 = logSH2[~logSH2["StockID"].isin(in_dex) & (logSH2["time"] >= 91500000) & (logSH2["time"] <= 150000000)]
    # data2 = logSH1[~logSH1["StockID"].isin(in_dex) & (logSH1["time"] >= 91500000) & (logSH1["time"] <= 150000000) & (
    #             logSH1['source'] == 13)]
    # columns = ["StockID", "cum_volume", "cum_amount", "close", "bid1p", "bid2p", "bid3p", "bid4p", "bid5p", "bid1q",
    #            "bid2q",
    #            "bid3q", "bid4q", "bid5q", "ask1p", "ask2p", "ask3p", "ask4p", "ask5p", "ask1q", "ask2q", "ask3q",
    #            "ask4q", "ask5q", "openPrice", "time", "numTrades", "time"]
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
    # data1_1['openPrice'] = data1_1.groupby('StockID')['openPrice'].transform('max')
    # data2_1['openPrice'] = data2_1.groupby('StockID')['openPrice'].transform('max')
    #
    # data2_1 = data2_1[~data2_1['bid1p'].isnull()]
    # test = pd.merge(data1_1, data2_1, left_on=columns, right_on=columns, how="outer")
    # n1 = test["IOPV"].count()
    # n2 = test["sequenceNo"].count()
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
    #     print(test[np.isnan(test["sequenceNo"])])
    #     print(len(test[np.isnan(test["sequenceNo"])]) / n1)
    #     print(len(test[np.isnan(test["sequenceNo"])]["time"].unique()))
    #     print(test[np.isnan(test["sequenceNo"])]["time"].unique())
    #     print(len(test[np.isnan(test["sequenceNo"])]["StockID"].unique()))
    #     print(test[np.isnan(test["sequenceNo"])]["StockID"].unique())
    #     re['time'].append(np.sort(test[np.isnan(test["sequenceNo"])]["time"].unique()))
    #     re['stock_list'].append(np.sort(test[np.isnan(test["sequenceNo"])]["StockID"].unique()))
    # if (len1 == n2) & (n1 < len1):
    #     print("baseline is not complete:")
    #     print(test[np.isnan(test["IOPV"])])
    #     print(n2 - n1)
    #     re['time'].append(np.sort(test[np.isnan(test["IOPV"])]["time"].unique()))
    #     re['stock_list'].append(np.sort(test[np.isnan(test["IOPV"])]["StockID"].unique()))
    #     print((n2 - n1) / n1)
    # del logSH2
    # del data1
    # del data2
    # del test
    # del data1_1
    # del data2_1

    # readPath = 'F:\\data\\' + y + '\\***_zt_88_03_day_88data\\mdLog_SH_***'
    # dataPathLs = np.array(glob.glob(readPath))
    # startTm = datetime.datetime.now()
    # logSH2 = pd.read_csv(dataPathLs[0])
    # print(datetime.datetime.now() - startTm)
    #
    # logSH2 = logSH2[["sequenceNo", "StockID", "source", "time", "cum_volume", "cum_amount", "close",
    #                  "bid1p", "bid2p", "bid3p", "bid4p", "bid5p", "bid1q", "bid2q", "bid3q",
    #                  "bid4q", "bid5q", "ask1p", "ask2p", "ask3p", "ask4p", "ask5p", "ask1q",
    #                  "ask2q", "ask3q", "ask4q", "ask5q", "openPrice", "numTrades"]]
    # logSH2["time"] = logSH2["time"].apply(lambda x: int(x.replace(':', "").replace('.', "")))
    #
    # print('----------------------------------------------------------------')
    # print('SH lv1 data:')
    # in_dex = [16, 300, 852, 905]
    # data1 = logSH2[~logSH2["StockID"].isin(in_dex) & (logSH2["time"] <= 150000000) & (
    #             logSH2['source'] == 22)]
    # data2 = logSH1[~logSH1["StockID"].isin(in_dex) & (logSH1["time"] <= 150000000) & (
    #             logSH1['source'] == 9)]
    # columns = ["StockID", "cum_volume", "cum_amount", "close", "bid1p", "bid2p", "bid3p", "bid4p", "bid5p", "bid1q",
    #            "bid2q",  "bid3q", "bid4q", "bid5q", "ask1p", "ask2p", "ask3p", "ask4p", "ask5p", "ask1q", "ask2q", "ask3q",
    #            "ask4q", "ask5q", "openPrice"]
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
    # data1_1['cum_amount'] = data1_1['cum_amount'].round(2)
    # data2_1['cum_amount'] = data2_1['cum_amount'].round(2)
    #
    # data2_1 = data2_1[(data2_1['bid1p'] != 0) | (data2_1['ask1p'] != 0) | (data2_1['cum_volume'] != 0)]
    # data1_1 = data1_1[(data1_1['bid1p'] != 0) | (data1_1['ask1p'] != 0) | (data1_1['cum_volume'] != 0)]
    #
    # test = pd.merge(data1_1, data2_1, left_on=columns, right_on=columns, how="outer")
    # n1 = test["sequenceNo_x"].count()
    # n2 = test["sequenceNo_y"].count()
    # len1 = len(test)
    # re['date'].append(y)
    # re['data'].append('SH lv1 data')
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
    #     print(test[np.isnan(test["sequenceNo_x"])])
    #     print(len(test[np.isnan(test["sequenceNo_x"])]) / n1)
    #     print(len(test[np.isnan(test["sequenceNo_x"])]["time"].unique()))
    #     print(test[np.isnan(test["sequenceNo_x"])]["time"].unique())
    #     print(len(test[np.isnan(test["sequenceNo_x"])]["StockID"].unique()))
    #     print(test[np.isnan(test["sequenceNo_x"])]["StockID"].unique())
    #     re['time'].append(np.sort(test[np.isnan(test["sequenceNo"])]["time"].unique()))
    #     re['stock_list'].append(np.sort(test[np.isnan(test["sequenceNo"])]["StockID"].unique()))
    # if (len1 == n2) & (n1 < len1):
    #     print("baseline is not complete:")
    #     print(test[np.isnan(test["sequenceNo_y"])])
    #     print(n2 - n1)
    #     re['time'].append(np.sort(test[np.isnan(test["sequenceNo_y"])]["time"].unique()))
    #     re['stock_list'].append(np.sort(test[np.isnan(test["sequenceNo_y"])]["StockID"].unique()))
    #     print((n2 - n1) / n1)
    # del logSH1
    # del logSH2
    # del data1
    # del data2
    # del test
    # del data1_1
    # del data2_1

    # print('----------------------------------------------------------------')
    # print('SH index data:')
    # readPath = 'F:\\data\\' + y + '\\***_zs_92_01_day_data\\mdLog_SH_***'
    # dataPathLs = np.array(glob.glob(readPath))
    # startTm = datetime.datetime.now()
    # logSH = pd.read_csv(dataPathLs[0])
    # logSH["time"] = logSH["time"].apply(lambda x: int(x.replace(':', "").replace('.', "")))
    # print(datetime.datetime.now() - startTm)
    #
    # readPath = 'F:\\data\\' + y + '\\***_zs_96_03_day_96data\\mdLog_SH_***'
    # dataPathLs = np.array(glob.glob(readPath))
    # startTm = datetime.datetime.now()
    # index = pd.read_csv(dataPathLs[0])
    # index["time"] = index["time"].apply(lambda x: int(x.replace(':', "").replace('.', "")))
    # print(datetime.datetime.now() - startTm)
    #
    #
    # in_dex = [16, 300, 852, 905]
    # index = index[index["StockID"].isin(in_dex)]
    # print(index["StockID"].unique())
    #
    # data1 = logSH[(logSH["StockID"].isin(in_dex)) & (logSH["time"] >= 91500000) & (logSH["time"] <= 150000000)]
    # data2 = index[(index["time"] >= 91500000) & (index["time"] <= 150000000) & (index['source'] == 13)]
    #
    # columns = ["StockID", "cum_volume", "cum_amount", "close", "openPrice"]
    # data1_1 = data1.drop_duplicates(subset=columns, keep="first").reset_index()
    # data2_1 = data2.drop_duplicates(subset=columns, keep="first").reset_index()
    #
    # for cols in ['close', 'openPrice']:
    #     data1_1[cols] = data1_1[cols].round(4)
    #     data2_1[cols] = data2_1[cols].round(4)
    # for cols in ['cum_amount']:
    #     data1_1[cols] = data1_1[cols].round(1)
    #     data2_1[cols] = data2_1[cols].round(1)
    #
    # test = pd.merge(data1_1, data2_1, left_on=columns, right_on=columns, how="outer")
    # n1 = test["sequenceNo_x"].count()
    # n2 = test["sequenceNo_y"].count()
    # len1 = len(test)
    # print(n1)
    # print(n2)
    # print(len1)
    # re['date'].append(y)
    # re['data'].append('SH index data without time column')
    # re['baseline'].append(n1)
    # re['test'].append(n2)
    # re['merge'].append(len1)
    # if (n1 == len1) & (n2 == len1):
    #     re['time'].append(0)
    #     re['stock_list'].append(0)
    # if n2 < len1:
    #     print("test is not complete:")
    #     print(test[np.isnan(test["sequenceNo_y"])])
    #     re['time'].append(np.sort(test[np.isnan(test['sequenceNo_y'])]['time_x'].unique()))
    #     re['stock_list'].append(np.sort(test[np.isnan(test['sequenceNo_y'])]['StockID'].unique()))
    # if (n2 == len1) & (n1 < len1):
    #     print("baseline is not complete::")
    #     print(test[np.isnan(test["sequenceNo_x"])])
    #     re['time'].append(np.sort(test[np.isnan(test["sequenceNo_x"])]['time_y'].unique()))
    #     re['stock_list'].append(np.sort(test[np.isnan(test['sequenceNo_x'])]['StockID'].unique()))
    #
    # del index
    # del logSH
    # del data1
    # del data2
    # del test
    # del data1_1
    # del data2_1

    # print('----------------------------------------------------------------')
    # print('SZ lv2 data:')
    #
    # readPath = 'F:\\data\\' + y + '\\***_zs_96_03_day_96data\\mdLog_SZ_***'
    # dataPathLs = np.array(glob.glob(readPath))
    # startTm = datetime.datetime.now()
    # logSZ1 = pd.read_csv(dataPathLs[0])
    # logSZ1["time"] = logSZ1["time"].apply(lambda x: int(x.replace(':', "").replace('.', "")))
    # print(datetime.datetime.now() - startTm)
    #
    # logSZ1 = logSZ1.loc[:, ["clockAtArrival", "sequenceNo", "StockID", "source", "time", "cum_volume", "cum_amount", "close",
    #                         "bid1p", "bid2p", "bid3p", "bid4p", "bid5p", "bid1q",
    #                         "bid2q", "bid3q", "bid4q", "bid5q", "ask1p", "ask2p",
    #                         "ask3p", "ask4p", "ask5p", "ask1q", "ask2q", "ask3q",
    #                         "ask4q", "ask5q", "openPrice", "numTrades"]]
    # logSZ1 = logSZ1[(logSZ1['StockID'] < 4000) | ((logSZ1['StockID'] > 300000) & (logSZ1['StockID'] < 310000))]
    #
    # readPath = 'A:\\KR_daily_data\\' + y + '\\SZ\\snapshot\\Level2\\***'
    # dataPathLs = np.array(glob.glob(readPath))
    # dateLs = np.array([int(os.path.basename(i).split('.')[0]) for i in dataPathLs])
    # dataPathLs = dataPathLs[(dateLs < 4000) | ((dateLs > 300000) & (dateLs < 310000))]
    # logSZ = []
    # ll = []
    # startTm = datetime.datetime.now()
    # for i in dataPathLs:
    #     try:
    #         df = pd.read_csv(i)
    #     except:
    #         print("empty data")
    #         print(i)
    #         ll.append(int(os.path.basename(i).split('.')[0]))
    #         continue
    #     df["StockID"] = int(os.path.basename(i).split('.')[0])
    #     logSZ += [df]
    # del df
    # logSZ = pd.concat(logSZ).reset_index(drop=True)
    # print(datetime.datetime.now() - startTm)
    # for i in range(1, 6):
    #     if i == 1:
    #         logSZ["bid" + str(i) + "p"] = logSZ["BidPrice"].apply(lambda x: float(x.split(',')[0][1:]))
    #         logSZ["ask" + str(i) + "p"] = logSZ["OfferPrice"].apply(lambda x: float(x.split(',')[0][1:]))
    #         logSZ["bid" + str(i) + "q"] = logSZ["BidOrderQty"].apply(lambda x: int(x.split(',')[0][1:]))
    #         logSZ["ask" + str(i) + "q"] = logSZ["OfferOrderQty"].apply(lambda x: int(x.split(',')[0][1:]))
    #     else:
    #         logSZ["bid" + str(i) + "p"] = logSZ["BidPrice"].apply(lambda x: float(x.split(',')[i - 1]))
    #         logSZ["ask" + str(i) + "p"] = logSZ["OfferPrice"].apply(lambda x: float(x.split(',')[i - 1]))
    #         logSZ["bid" + str(i) + "q"] = logSZ["BidOrderQty"].apply(lambda x: int(x.split(',')[i - 1]))
    #         logSZ["ask" + str(i) + "q"] = logSZ["OfferOrderQty"].apply(lambda x: int(x.split(',')[i - 1]))
    # logSZ = logSZ.rename(
    #     columns={"Volume": "cum_volume", "Amount": "cum_amount", "LastPx": "close", "OpenPx": "openPrice",
    #              "NumTrades": "numTrades"})
    # logSZ["time"] = (logSZ["QuotTime"] - int(y) * 1000000000).astype(np.int64)
    # print(datetime.datetime.now() - startTm)
    #
    # startTm = datetime.datetime.now()
    # data1 = logSZ[(logSZ["time"] >= 91500000) & (logSZ["time"] < 150000000)]
    # data2 = logSZ1[(logSZ1["time"] >= 91500000) & (logSZ1["time"] < 150000000) & (logSZ1['source'] == 24)]
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
    # print(n1)
    # print(n2)
    # print(len(set(data1_1["StockID"].unique()) - set(data2_1["StockID"].unique())))
    # print(set(data1_1["StockID"].unique()) - set(data2_1["StockID"].unique()))
    # if n1 != n2:
    #     sl = list(set(data1_1["StockID"].unique()) & set(data2_1["StockID"].unique()))
    #     data1_1 = data1_1[data1_1["StockID"].isin(sl)]
    #     data2_1 = data2_1[data2_1["StockID"].isin(sl)]
    # for cols in ['close', 'cum_amount']:
    #     data1_1[cols] = data1_1[cols].round(2)
    #     data2_1[cols] = data2_1[cols].round(2)
    # test = pd.merge(data1_1, data2_1, left_on=columns, right_on=columns, how="outer")
    # print(datetime.datetime.now() - startTm)
    # n1 = test["ImageStatus"].count()
    # n2 = test["sequenceNo"].count()
    # len1 = len(test)
    # re['date'].append(y)
    # re['data'].append('SZ lv2 data')
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
    #     print(test[np.isnan(test["sequenceNo"])])
    #     print(len(test[np.isnan(test["sequenceNo"])]) / n1)
    #     print(np.sort(test[np.isnan(test["sequenceNo"])]["time"].unique()))
    #     print(len(np.sort(test[np.isnan(test["sequenceNo"])]["StockID"].unique())))
    #     print(np.sort(test[np.isnan(test["sequenceNo"])]["StockID"].unique()))
    #     re['time'].append(np.sort(test[np.isnan(test["sequenceNo"])]["time"].unique()))
    #     re['stock_list'].append(np.sort(test[np.isnan(test["sequenceNo"])]["StockID"].unique()))
    # if (len1 == n2) & (n1 < len1):
    #     print("baseline is not complete:")
    #     print(test[np.isnan(test["ImageStatus"])])
    #     print(n2 - n1)
    #     re['time'].append(np.sort(test[np.isnan(test["ImageStatus"])]["time"].unique()))
    #     re['stock_list'].append(np.sort(test[np.isnan(test["ImageStatus"])]["StockID"].unique()))
    # del logSZ
    # del logSZ1
    # del data1
    # del data2
    # del test
    # del data1_1
    # del data2_1
    #
    # readPath = 'F:\\data\\' + y + '\\***_zs_96_03_day_96data\\mdOrderLog_***'
    # dataPathLs = np.array(glob.glob(readPath))
    # startTm = datetime.datetime.now()
    # OrderLogSZ1 = pd.read_csv(dataPathLs[0])
    # print(datetime.datetime.now() - startTm)
    #
    # readPath = 'A:\\KR_daily_data\\' + y + '\\SZ\\order\\***'
    # dataPathLs = np.array(glob.glob(readPath))
    # dateLs = np.array([int(os.path.basename(i).split('.')[0]) for i in dataPathLs])
    # dataPathLs = dataPathLs[(dateLs < 4000) | ((dateLs > 300000) & (dateLs < 310000))]
    # OrderLogSZ = []
    # ll = []
    #
    # startTm = datetime.datetime.now()
    # for i in dataPathLs:
    #     try:
    #         df = pd.read_csv(i, encoding='GBK')
    #     except:
    #         print("empty data")
    #         print(i)
    #         ll.append(int(os.path.basename(i).split('.')[0]))
    #         continue
    #     df["SecurityID"] = int(os.path.basename(i).split('.')[0])
    #     OrderLogSZ += [df]
    # OrderLogSZ = pd.concat(OrderLogSZ).reset_index(drop=True)
    # print(datetime.datetime.now() - startTm)
    # OrderLogSZ = OrderLogSZ.rename(columns={"OrdType": "OrderType"})
    # OrderLogSZ["TransactTime"] = (OrderLogSZ["TransactTime"] - int(y) * 1000000000).astype(np.int64)
    # #     OrderLogSZ1 = OrderLogSZ1[OrderLogSZ1["Side"] != 'F']
    # print(OrderLogSZ["Side"].unique())
    # print(OrderLogSZ["ChannelNo"].unique())
    # OrderLogSZ["Side"] = np.where(OrderLogSZ["Side"] == '1', 1, np.where(
    #     OrderLogSZ["Side"] == '2', 2, OrderLogSZ["Side"]))
    # print(OrderLogSZ[((OrderLogSZ["Side"] != 1) & (OrderLogSZ["Side"] != 2)) | (OrderLogSZ["OrderType"].isnull())])
    # OrderLogSZ["OrderType"] = np.where(OrderLogSZ["OrderType"] == 2, '2', np.where(
    #     OrderLogSZ["OrderType"] == 1, '1', OrderLogSZ['OrderType']))
    #
    # OrderLogSZ1["OrderType"] = np.where(OrderLogSZ1["OrderType"] == 2, '2', np.where(
    #     OrderLogSZ1["OrderType"] == 1, '1', OrderLogSZ1['OrderType']))
    #
    # OrderLogSZ = OrderLogSZ[OrderLogSZ["ChannelNo"] != 4001]
    # print(len(OrderLogSZ["SecurityID"].unique()))
    # print(len(OrderLogSZ1["SecurityID"].unique()))
    # print(len(set(OrderLogSZ["SecurityID"].unique()) - set(OrderLogSZ1["SecurityID"].unique())))
    # print(set(OrderLogSZ["SecurityID"].unique()) - set(OrderLogSZ1["SecurityID"].unique()))
    #
    # sl = list(set(OrderLogSZ["SecurityID"].unique()) & set(OrderLogSZ1['SecurityID'].unique()))
    # OrderLogSZ = OrderLogSZ[OrderLogSZ["SecurityID"].isin(sl)]
    # OrderLogSZ1 = OrderLogSZ1[OrderLogSZ1["SecurityID"].isin(sl)]
    # print(len(OrderLogSZ["SecurityID"].unique()))
    # print(len(OrderLogSZ1["SecurityID"].unique()))
    #
    # print('----------------------------------------------------------------')
    # print('SZ order data:')
    #
    # OrderLogSZ1['Price'] = (OrderLogSZ1['Price'] / 10000).round(2)
    # columns = ["ApplSeqNum", "TransactTime", "Side", 'OrderType', 'Price', 'OrderQty', "SecurityID"]
    # ree = pd.merge(OrderLogSZ, OrderLogSZ1, on=columns, how="outer", validate='one_to_one')
    # n1 = ree["TradeTime"].count()
    # n2 = ree["sequenceNo"].count()
    # len1 = len(ree)
    # print(n1)
    # print(n2)
    # print(len1)
    # re['date'].append(y)
    # re['data'].append('SZ order data')
    # re['baseline'].append(n1)
    # re['test'].append(n2)
    # re['merge'].append(len1)
    # if (n1 == len1) & (n2 == len1):
    #     re['time'].append(0)
    #     re['stock_list'].append(0)
    #
    # print("-----------------------------------------------")
    # if n2 < len1:
    #     print("test is not complete:")
    #     print(ree[np.isnan(ree["sequenceNo"])])
    #     print(len(ree[np.isnan(ree["sequenceNo"])]))
    #     print(np.sort(ree[np.isnan(ree["sequenceNo"])]["TransactTime"].unique()))
    #     print(len(ree[np.isnan(ree["sequenceNo"])]["SecurityID"].unique()))
    #     print(ree[np.isnan(ree["sequenceNo"])]["SecurityID"].unique())
    #     re['time'].append(
    #         np.sort(ree[np.isnan(ree["sequenceNo"]) & (~ree["OrderType"].isnull())]["TransactTime"].unique()))
    #     re['stock_list'].append(
    #         np.sort(ree[np.isnan(ree["sequenceNo"]) & (~ree["OrderType"].isnull())]["SecurityID"].unique()))
    # if (len1 == n2) & (n1 < len1):
    #     print("test is complete, baseline is not complete:")
    #     print(ree[np.isnan(ree["TradeTime"])])
    #     print(np.sort(ree[np.isnan(ree["TradeTime"])]["TransactTime"].unique()))
    #     print(len(ree[np.isnan(ree["TradeTime"])]["SecurityID"].unique()))
    #     print(ree[np.isnan(ree["TradeTime"])]["SecurityID"].unique())
    #     print(n2 - n1)
    #     re['time'].append(np.sort(ree[np.isnan(ree["TradeTime"])]["TransactTime"].unique()))
    #     re['stock_list'].append(np.sort(ree[np.isnan(ree["TradeTime"])]["SecurityID"].unique()))
    # del OrderLogSZ
    # del OrderLogSZ1
    # del ree

    readPath = 'F:\\data\\' + y + '\\***_zs_96_03_day_96data\\mdTradeLog_***'
    dataPathLs = np.array(glob.glob(readPath))

    startTm = datetime.datetime.now()
    SH1 = pd.read_csv(dataPathLs[0])
    print(datetime.datetime.now() - startTm)

    readPath = 'A:\\KR_daily_data\\' + y + '\\SH\\tick\\***'
    dataPathLs = np.array(glob.glob(readPath))
    dateLs = np.array([int(os.path.basename(i).split('.')[0]) for i in dataPathLs])
    dataPathLs = dataPathLs[(dateLs >= 600000) & (dateLs <= 700000)]
    SH = []
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
        SH += [df]
    SH = pd.concat(SH).reset_index(drop=True)
    print(datetime.datetime.now() - startTm)
    SH["TransactTime"] = (SH["TradeTime"] - int(y) * 1000000000).astype(np.int64)
    SH["ExecType"] = 'F'
    SH = SH.rename(columns={"TradeIndex": "ApplSeqNum", "BuyNo": "BidApplSeqNum", "SellNo": "OfferApplSeqNum",
                            "TradeAmount":"TradeMoney"})

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

    SH["ExecType"] = SH["ExecType"].apply(lambda x: str(x))
    SH1["ExecType"] = 'F'
    columns = ["TransactTime", "ApplSeqNum", "SecurityID", "TradePrice", "TradeQty", "TradeMoney", "TradeBSFlag",
               "ExecType",  "BidApplSeqNum", "OfferApplSeqNum"]
    SH1['TradePrice'] = (SH1['TradePrice'] / 10000).round(2)
    SH1['TradeMoney'] = (SH1['TradeMoney'] / 10000).round(2)
    ree = pd.merge(SH, SH1, left_on=columns, right_on=columns, how="outer", validate='one_to_one')
    n1 = ree["TradeTime"].count()
    n2 = ree["sequenceNo"].count()
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
        print(ree[np.isnan(ree["sequenceNo"])])
        print(len(ree[np.isnan(ree["sequenceNo"])]))
        print(np.sort(ree[np.isnan(ree["sequenceNo"])]["TransactTime"].unique()))
        print(len(ree[np.isnan(ree["sequenceNo"])]["SecurityID"].unique()))
        print(ree[np.isnan(ree["sequenceNo"])]["SecurityID"].unique())
        re['time'].append(np.sort(ree[np.isnan(ree["sequenceNo"])]["TransactTime"].unique()))
        re['stock_list'].append(np.sort(ree[np.isnan(ree["sequenceNo"])]["SecurityID"].unique()))
    if (len1 == n2) & (n1 < len1):
        print("baseline is not complete:")
        print(ree[np.isnan(ree["TradeTime"])])
        print(np.sort(ree[np.isnan(ree["TradeTime"])]["TransactTime"].unique()))
        print(len(ree[np.isnan(ree["TradeTime"])]["SecurityID"].unique()))
        print(ree[np.isnan(ree["TradeTime"])]["SecurityID"].unique())
        print(n2 - n1)
        re['time'].append(np.sort(ree[np.isnan(ree["TradeTime"])]["TransactTime"].unique()))
        re['stock_list'].append(np.sort(ree[np.isnan(ree["TradeTime"])]["SecurityID"].unique()))
    del SH
    del SH1
    del ree

    readPath = 'F:\\data\\' + y + '\\***_zs_96_03_day_96data\\mdTradeLog_***'
    dataPathLs = np.array(glob.glob(readPath))

    startTm = datetime.datetime.now()
    TradeLogSZ1 = pd.read_csv(dataPathLs[0])
    print(datetime.datetime.now() - startTm)
    TradeLogSZ1["TradeBSFlag"] = 'N'

    readPath = 'A:\\KR_daily_data\\' + y + '\\SZ\\tick\\***'
    dataPathLs = np.array(glob.glob(readPath))
    dateLs = np.array([int(os.path.basename(i).split('.')[0]) for i in dataPathLs])
    dataPathLs = dataPathLs[(dateLs < 4000) | ((dateLs > 300000) & (dateLs < 310000))]
    TradeLogSZ = []
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
        TradeLogSZ += [df]
    TradeLogSZ = pd.concat(TradeLogSZ).reset_index(drop=True)
    print(datetime.datetime.now() - startTm)
    TradeLogSZ["TransactTime"] = (TradeLogSZ["TransactTime"] - int(y) * 1000000000).astype(np.int64)
    TradeLogSZ = TradeLogSZ.rename(columns={"Qty": "TradeQty", "Price": "TradePrice"})
    TradeLogSZ["TradeMoney"] = (TradeLogSZ["TradePrice"] * TradeLogSZ["TradeQty"]).round(2)
    TradeLogSZ["TradeBSFlag"] = 'N'

    TradeLogSZ = TradeLogSZ[TradeLogSZ['ChannelNo'] != 4001]
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
    TradeLogSZ1['TradePrice'] = (TradeLogSZ1['TradePrice'] / 10000).round(2)
    TradeLogSZ1['TradeMoney'] = (TradeLogSZ1['TradeMoney'] / 10000).round(2)
    columns = ["TransactTime", "ApplSeqNum", "SecurityID", "ExecType", "TradeBSFlag", "TradePrice", "TradeQty",
               "TradeMoney", "BidApplSeqNum", "OfferApplSeqNum"]
    ree = pd.merge(TradeLogSZ, TradeLogSZ1, left_on=columns, right_on=columns, how="outer", validate='one_to_one')
    n1 = ree["Amt"].count()
    n2 = ree["sequenceNo"].count()
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
        print(ree[np.isnan(ree["sequenceNo"])])
        print(len(ree[np.isnan(ree["sequenceNo"])]))
        print(np.sort(ree[np.isnan(ree["sequenceNo"])]["TransactTime"].unique()))
        print(len(ree[np.isnan(ree["sequenceNo"])]["SecurityID"].unique()))
        print(ree[np.isnan(ree["sequenceNo"])]["SecurityID"].unique())
        re['time'].append(np.sort(ree[np.isnan(ree["sequenceNo"])]["TransactTime"].unique()))
        re['stock_list'].append(ree[np.isnan(ree["sequenceNo"])]["SecurityID"].unique())
    if (len1 == n2) & (n1 < len1):
        print("baseline is not complete:")
        print(ree[np.isnan(ree["Amt"])])
        print(np.sort(ree[np.isnan(ree["Amt"])]["TransactTime"].unique()))
        print(len(ree[np.isnan(ree["Amt"])]["SecurityID"].unique()))
        print(ree[np.isnan(ree["Amt"])]["SecurityID"].unique())
        print(n2 - n1)
        re['time'].append(np.sort(ree[np.isnan(ree["Amt"])]["TransactTime"].unique()))
        re['stock_list'].append(ree[np.isnan(ree["Amt"])]["SecurityID"].unique())
    del TradeLogSZ
    del TradeLogSZ1
    del ree

    re = pd.DataFrame(re)
    re.to_csv('D:\\work\\project 7 snapshot data\\zs_96_03\\' + y + '.csv')