import numpy as np
import pandas as pd
import pickle
from matplotlib import pyplot as plt
from matplotlib.ticker import Formatter
import collections
import glob
import os
import datetime


y = '20200803'
print('----------------------------------------------------------------')
print(y)



print('~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~SH snapshot data~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~')
startTm = datetime.datetime.now()
readPath = r'\\192.168.10.30\Kevin_zhenyu\day_stock\***'
dataPathLs = np.array(glob.glob(readPath))
dataPathLs = dataPathLs[[np.array([os.path.basename(i).split('.')[0][:2] == 'SH' for i in dataPathLs])]]
db = pd.DataFrame()
for p in dataPathLs:
    dayData = pd.read_csv(p, compression='gzip')
    db = pd.concat([db, dayData])
print(datetime.datetime.now() - startTm)

readPath = '\\\\mentos\\dailyRawData\\logs_' + y + '_zt_88_03_day_pcap/mdL2Pcap_SH_***'
dataPathLs = np.array(glob.glob(readPath))
startTm = datetime.datetime.now()
logSH = pd.read_csv(dataPathLs[0])
print(datetime.datetime.now() - startTm)

logSH = logSH.rename(columns={'ID': 'skey'})
logSH = logSH[(logSH['skey'] >= 1600000) & (logSH['skey'] < 1700000)]
logSH = logSH[["sequenceNo", 'clockAtArrival', "skey", "time", "cum_volume", "cum_amount", "cum_tradesCnt", "prevClose",
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
logSH.columns = ["sequenceNo", 'clockAtArrival', "skey", "time", "cum_volume", "cum_amount", "cum_trades_cnt", "prev_close",
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
logSH["time"] = logSH['time'].astype('int64') * 1000
logSH["clockAtArrival"] = logSH["time"].astype(str).apply(
    lambda x: np.int64(datetime.datetime.strptime(x, '%Y%m%d%H%M%S%f').timestamp() * 1e6))
logSH['datetime'] = logSH["clockAtArrival"].apply(lambda x: datetime.datetime.fromtimestamp(x / 1e6))
logSH = logSH.fillna(0)
logSH["ordering"] = logSH.groupby("skey").cumcount() + 1

logSH["has_missing"] = 0
for col in ["skey", "date", "cum_trades_cnt", "total_bid_orders",
            'total_ask_orders', 'total_bid_levels', 'total_ask_levels', 'cum_canceled_buy_orders',
            'cum_canceled_sell_orders',
            "ordering", 'bid_trade_max_duration', 'ask_trade_max_duration', 'has_missing']:
    logSH[col] = logSH[col].astype('int32')

for cols in ['total_bid_vwap', "total_ask_vwap"]:
    logSH[cols] = logSH[cols].apply(lambda x: round(x, 3))

assert (sum(logSH[logSH["open"] != 0].groupby("skey")["open"].nunique() != 1) == 0)
assert (sum(logSH[logSH["prev_close"] != 0].groupby("skey")["prev_close"].nunique() != 1) == 0)
logSH["prev_close"] = np.where(logSH["time"] >= 91500000000, logSH.groupby("skey")["prev_close"].transform("max"),
                            logSH["prev_close"])
logSH["open"] = np.where(logSH["cum_volume"] > 0, logSH.groupby("skey")["open"].transform("max"), logSH["open"])
assert (sum(logSH[logSH["open"] != 0].groupby("skey")["open"].nunique() != 1) == 0)
assert (sum(logSH[logSH["prev_close"] != 0].groupby("skey")["prev_close"].nunique() != 1) == 0)
assert (logSH[logSH["cum_volume"] > 0]["open"].min() > 0)

da_te = str(logSH["date"].iloc[0])
da_te = da_te[:4] + '-' + da_te[4:6] + '-' + da_te[6:8]
db1 = db[db["date"] == da_te]
db1["ID"] = db1["ID"].str[2:].astype(int) + 1000000
db1["date"] = (db1["date"].str[:4] + db1["date"].str[5:7] + db1["date"].str[8:]).astype(int)
logSH["cum_max"] = logSH.groupby("skey")["cum_volume"].transform(max)
s2 = logSH[logSH["cum_volume"] == logSH["cum_max"]].groupby("skey").first().reset_index()
dd = logSH[logSH["cum_volume"] == logSH["cum_max"]].groupby("skey")["time"].first().reset_index()
logSH.drop("cum_max", axis=1, inplace=True)
s2 = s2.rename(columns={"skey": "ID", 'open': "d_open", "prev_close": "d_yclose", "high": "d_high", "low": "d_low",
                        "close": "d_close", "cum_volume": "d_volume", "cum_amount": "d_amount"})
if logSH["date"].iloc[0] < 20180820:
    s2["auction"] = 0
else:
    dd["auction"] = np.where(dd["time"] <= 145700000000, 0, 1)
    dd = dd.rename(columns={"skey": "ID"})
    s2 = pd.merge(s2, dd[["ID", "auction"]], on="ID")
s2 = s2[["ID", "date", "d_open", "d_yclose", "d_high", "d_low", "d_close", "d_volume", "d_amount", "auction"]]
re = pd.merge(db1, s2, on=["ID", "date", "d_open", "d_yclose", "d_high", "d_low", "d_volume"], how="outer")
try:
    assert (sum(re["d_amount_y"].isnull()) == 0)
except:
    print("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!")
    print(re[re["d_amount_y"].isnull()])

# check 2
# first part
date = pd.DataFrame(pd.date_range(start='2019-06-10 08:30:00', end='2019-06-10 18:00:00', freq='s'), columns=["Orig"])
date["time"] = date["Orig"].apply(lambda x: int(x.strftime("%H%M%S")) * 1000)
date["group"] = date["time"] // 10000
logSH["group"] = logSH["time"] // 10000000
gl = date[((date["time"] >= 93000000) & (date["time"] < 113000000)) | (
            (date["time"] >= 130000000) & (date["time"] < 150000000))]["group"].unique()
l = set(gl) - set(logSH["group"].unique())
logSH["has_missing1"] = 0
if len(l) != 0:
    print("massive missing")
    print(l)
    logSH["order"] = logSH.groupby(["skey", "time"]).cumcount()
    for i in l:
        logSH["t"] = logSH[logSH["group"] > i].groupby("skey")["time"].transform("min")
        logSH["has_missing1"] = np.where((logSH["time"] == logSH["t"]) & (logSH["order"] == 0), 1, logSH["has_missing1"])
    logSH.drop(["order", "t", "group"], axis=1, inplace=True)
else:
    print("no massive missing")
    logSH.drop(["group"], axis=1, inplace=True)

# second part

logSH["time_interval"] = logSH.groupby("skey")["datetime"].apply(lambda x: x - x.shift(1))
logSH["time_interval"] = logSH["time_interval"].apply(lambda x: x.seconds)
logSH["tn_update"] = logSH.groupby("skey")["cum_trades_cnt"].apply(lambda x: x - x.shift(1))

f1 = logSH[(logSH["time"] >= 93000000000) & (logSH["tn_update"] != 0)].groupby("skey")["time"].min().reset_index()
f1 = f1.rename(columns={"time": "time1"})
f2 = logSH[(logSH["time"] >= 130000000000) & (logSH["tn_update"] != 0)].groupby("skey")["time"].min().reset_index()
f2 = f2.rename(columns={"time": "time2"})
f3 = logSH[(logSH["time"] >= 150000000000) & (logSH["tn_update"] != 0)].groupby("skey")["time"].min().reset_index()
f3 = f3.rename(columns={"time": "time3"})
logSH = pd.merge(logSH, f1, on="skey", how="left")
del f1
logSH = pd.merge(logSH, f2, on="skey", how="left")
del f2
logSH = pd.merge(logSH, f3, on="skey", how="left")
del f3
p99 = \
logSH[(logSH["time"] > 93000000000) & (logSH["time"] < 145700000000) & (logSH["time"] != logSH["time2"]) & (logSH["tn_update"] != 0)] \
    .groupby("skey")["tn_update"].apply(lambda x: x.describe([0.99])["99%"]).reset_index()
p99 = p99.rename(columns={"tn_update": "99%"})
logSH = pd.merge(logSH, p99, on="skey", how="left")

logSH["has_missing2"] = 0
logSH["has_missing2"] = np.where((logSH["time_interval"] > 60) & (logSH["tn_update"] > logSH["99%"]) &
                              (logSH["time"] > logSH["time1"]) & (logSH["time"] != logSH["time2"]) & (logSH["time"] != logSH["time3"]) & (
                                          logSH["time"] != 100000000000), 1, 0)
logSH.drop(["time_interval", "tn_update", "time1", "time2", "time3", "99%"], axis=1, inplace=True)

logSH["has_missing"] = np.where((logSH["has_missing1"] == 1) | (logSH["has_missing2"] == 1), 1, 0)
logSH.drop(["has_missing1", "has_missing2"], axis=1, inplace=True)
if logSH[logSH["has_missing"] == 1].shape[0] != 0:
    print("has missing!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!")
    print(logSH[logSH["has_missing"] == 1].shape[0])

logSH["has_missing"] = logSH["has_missing"].astype('int32')
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

display(logSH["date"].iloc[0])
print("SH finished")

# database_name = 'com_md_eq_cn'
# user = "zhenyuy"
# password = "bnONBrzSMGoE"
#
# db1 = DB("192.168.10.178", database_name, user, password)
# db1.write('md_snapshot_l2', logSH)

del logSH








readPath = '/mnt/dailyRawData/logs_' + y + '_zt_88_03_day_pcap/mdIndexPcap_SH_***'
dataPathLs = np.array(glob.glob(readPath))

index = pd.read_csv(dataPathLs[0])

index["StockID"] = index["ID"] - 1000000
in_dex = [16, 300, 852, 905]
index = index[index["StockID"].isin(in_dex)]
print(index["StockID"].unique())

index = index.rename(columns={"ID": "skey"})
index["date"] = int(y)
index["time"] = index['time'].astype(np.int64) * 1000
index["clockAtArrival"] = index["time"].astype(str).apply(
    lambda x: np.int64(datetime.datetime.strptime(x, '%Y%m%d%H%M%S%f').timestamp() * 1e6))
index['datetime'] = index["clockAtArrival"].apply(lambda x: datetime.datetime.fromtimestamp(x / 1e6))

index.columns = ['cum_volume', 'open', 'high', 'prev_close', 'low', 'close', 'cum_amount', 'skey',
              'date', 'time', 'clockAtArrival', 'datetime']
index = index.fillna(0)
index["ordering"] = index.groupby("skey").cumcount()
index["ordering"] = index["ordering"] + 1

assert (sum(index[index["open"] != 0].groupby("skey")["open"].nunique() != 1) == 0)
assert (sum(index[index["prev_close"] != 0].groupby("skey")["prev_close"].nunique() != 1) == 0)
index["prev_close"] = np.where(index["time"] >= 91500000000, index.groupby("skey")["prev_close"].transform("max"),
                            index["prev_close"])
index["open"] = np.where(index["cum_volume"] > 0, index.groupby("skey")["open"].transform("max"), index["open"])
assert (sum(index[index["open"] != 0].groupby("skey")["open"].nunique() != 1) == 0)
assert (sum(index[index["prev_close"] != 0].groupby("skey")["prev_close"].nunique() != 1) == 0)
assert (index[index["cum_volume"] > 0]["open"].min() > 0)

for cols in ['open', 'high', 'prev_close', 'low', 'close']:
    index[cols] = index[cols].apply(lambda x: round(x, 4)).astype('float64')

index = index[["skey", "date", "time", "clockAtArrival", "datetime", "ordering", "cum_volume", "cum_amount",
         "prev_close", "open", "high", "low", "close"]]

display(index["date"].iloc[0])
print("index finished")

# database_name = 'com_md_eq_cn'
# user = "zhenyuy"
# password = "bnONBrzSMGoE"
#
# db1 = DB("192.168.10.178", database_name, user, password)
# db1.write('md_index', index)















readPath = '/mnt/dailyRawData/logs_' + y + '_zs_***_day_pcap/mdL2Pcap_SZ_***'
dataPathLs = np.array(glob.glob(readPath))

startTm = datetime.datetime.now()
logSZ = pd.read_csv(dataPathLs[0])
print(datetime.datetime.now() - startTm)

logSZ = logSZ.loc[:, ["sequenceNo", "ID", "time", "cum_volume", "cum_amount", "cum_tradesCnt", "prevClose",
                        "open", "high", "low", "close", 'bid10p', 'bid9p', 'bid8p', 'bid7p', 'bid6p', 'bid5p', 'bid4p',
                        'bid3p', 'bid2p', 'bid1p',
                        'ask1p', 'ask2p', 'ask3p', 'ask4p', 'ask5p', 'ask6p', 'ask7p', 'ask8p', 'ask9p', 'ask10p',
                        'bid10q', 'bid9q', 'bid8q',
                        'bid7q', 'bid6q', 'bid5q', 'bid4q', 'bid3q', 'bid2q', 'bid1q', 'ask1q', 'ask2q', 'ask3q',
                        'ask4q', 'ask5q', 'ask6q',
                        'ask7q', 'ask8q', 'ask9q', 'ask10q', 'bid10n', 'bid9n', 'bid8n', 'bid7n', 'bid6n', 'bid5n',
                        'bid4n', 'bid3n', 'bid2n', 'bid1n',
                        'ask1n', 'ask2n', 'ask3n', 'ask4n', 'ask5n', 'ask6n', 'ask7n', 'ask8n', 'ask9n', 'ask10n',
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
                        'ask1Top50q', "totalBidQuantity", "totalAskQuantity",
                        "vwapBid", "vwapAsk"]]
logSZ = logSZ.rename(columns={"open": "openPrice", "cum_tradesCnt": "numTrades"})
logSZ["StockID"] = logSZ["ID"] - 2000000
logSZ = logSZ[(logSZ['StockID'] < 4000) | ((logSZ['StockID'] > 300000) & (logSZ['StockID'] < 310000))]











readPath = '/mnt/dailyRawData/logs_' + y + '_zs_***_day_pcap/mdOrderPcap_SZ_***'
dataPathLs = np.array(glob.glob(readPath))

OrderLogSZ = pd.read_csv(dataPathLs[0])
print(datetime.datetime.now() - startTm)

OrderLogSZ["SecurityID"] = OrderLogSZ["ID"] - 2000000
OrderLogSZ = OrderLogSZ.rename(columns={"time": 'TransactTime'})

OrderLogSZ["OrderType"] = np.where(OrderLogSZ["OrderType"] == 2, '2', np.where(
    OrderLogSZ["OrderType"] == 1, '1', OrderLogSZ['OrderType']))
OrderLogSZ = OrderLogSZ[(OrderLogSZ['SecurityID'] < 4000) | ((OrderLogSZ['SecurityID'] > 300000)
                                                                & (OrderLogSZ['SecurityID'] < 310000))]











readPath = '/mnt/dailyRawData/logs_' + y + '_zt_***_day_pcap/mdTradePcap_SH_***'
dataPathLs = np.array(glob.glob(readPath))

startTm = datetime.datetime.now()
SH = pd.read_csv(dataPathLs[0])
print(datetime.datetime.now() - startTm)

SH["SecurityID"] = SH["ID"] - 1000000
SH = SH.rename(columns={"time": 'TransactTime'})
SH = SH[(SH1['SecurityID'] >= 600000) & (SH1['SecurityID'] <= 700000)]

readPath = '/mnt/dailyRawData/logs_' + y + '_zs_***_day_pcap/mdTradePcap_SZ_***'
dataPathLs = np.array(glob.glob(readPath))

startTm = datetime.datetime.now()
TradeLogSZ = pd.read_csv(dataPathLs[1])
print(datetime.datetime.now() - startTm)

TradeLogSZ["SecurityID"] = TradeLogSZ["ID"] - 2000000
TradeLogSZ = TradeLogSZ.rename(columns={"time": 'TransactTime'})
TradeLogSZ["TradeBSFlag"] = 'N'
TradeLogSZ = TradeLogSZ[(TradeLogSZ['SecurityID'] < 4000) | ((TradeLogSZ['SecurityID'] > 300000)
                                                             & (TradeLogSZ['SecurityID'] < 310000))]



db = DB("192.168.10.178", 'com_md_eq_cn', 'zhenyuy', 'bnONBrzSMGoE')