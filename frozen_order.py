#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri Sep 18 13:13:57 2020

@author: work11
"""
import os
import glob
import datetime
import numpy as np
import pandas as pd

class generateMBD():
    
    def __init__(self, skey, date, hasAuction):
        
        self.date = date
        self.skey = skey
        
        self.bidDict = {}
        self.askDict = {}
        self.bidp = []
        self.bidq = []
        self.askp = []
        self.askq = []
        self.orderDict = {}
        self.hasTempOrder = False
        self.isAuction = hasAuction
        
        self.cum_volume = 0
        self.cum_amount = 0
        self.close = 0
        self.bid1p = 0
        self.ask1p = 0
        self.cum_volume_ls = []
        self.cum_amount_ls = []
        self.closeLs = []
        self.caaLs = []
        self.exchgTmLs = []
        self.ApplSeqNumLs = []
        self.bboImproveLs = []
        self.hasUnfrozenLs = []
        
        self.frozenOrderDict = {}
        self.frozenApplSeqNumDict = {}
        self.frozenBidDict = {}
        self.frozenAskDict = {}
        self.maxLmtBuyP = 1000000
        self.minLmtSellP = 0
        
        self.aggSide = 0
        self.prevMktApplSeqNum = 0
        
        self.outputLv = 10
        
        self.EPSILON = 1e-6
    
    
    def insertAuctionOrder(self, caa, exchgTm, ApplSeqNum, side, orderType, price, qty):
                
        if side == 1:
            if price in self.bidDict:
                self.bidDict[price] += qty
            else:
                self.bidDict[price] = qty
        else:
            if price in self.askDict:
                self.askDict[price] += qty
            else:
                self.askDict[price] = qty

        self.orderDict[ApplSeqNum] = (price, qty, side, orderType, 0)
        self.caaLs.append(caa)
        self.exchgTmLs.append(exchgTm)
        self.ApplSeqNumLs.append(ApplSeqNum)
    
    
    def removeOrderByAuctionCancel(self, caa, exchgTm, ApplSeqNum, cancelQty, BidApplSeqNum, OfferApplSeqNum):
        
        cancelApplSeqNum = max(BidApplSeqNum, OfferApplSeqNum)
        cancelPrice, cancelOrderQty, cancelSide, cancelOrderType, hasTrade = self.orderDict[cancelApplSeqNum]
        assert(cancelOrderQty == cancelQty)
        self.orderDict.pop(cancelApplSeqNum)

        if cancelSide == 1:
            remain = self.bidDict[cancelPrice] - cancelQty
            if remain == 0:
                self.bidDict.pop(cancelPrice)
            else:
                self.bidDict[cancelPrice] = remain
        elif cancelSide == 2:
            remain = self.askDict[cancelPrice] - cancelQty
            if remain == 0:
                self.askDict.pop(cancelPrice)
            else:
                self.askDict[cancelPrice] = remain
                
        self.caaLs.append(caa)
        self.exchgTmLs.append(exchgTm)
        self.ApplSeqNumLs.append(ApplSeqNum)

    
    def removeOrderByAuctionTrade(self, caa, exchgTm, ApplSeqNum, price, qty, BidApplSeqNum, OfferApplSeqNum):
        
        bidOrderPrice, bidOrderQty, bidOrderSide, bidOrderType, hasTrade = self.orderDict[BidApplSeqNum]
        askOrderPrice, askOrderQty, askOrderSide, askOrderType, hasTrade = self.orderDict[OfferApplSeqNum]

        bidRemain = self.bidDict[bidOrderPrice] - qty
        if bidRemain == 0:
            self.bidDict.pop(bidOrderPrice)
        elif bidRemain > 0:
            self.bidDict[bidOrderPrice] = bidRemain
        bidOrderQty -= qty
        if bidOrderQty == 0:
            self.orderDict.pop(BidApplSeqNum)
        else:
            self.orderDict[BidApplSeqNum] = (bidOrderPrice, bidOrderQty, bidOrderSide, bidOrderType, 1)
        
        askRemain = self.askDict[askOrderPrice] - qty
        if askRemain == 0:
            self.askDict.pop(askOrderPrice)
        elif askRemain > 0:
            self.askDict[askOrderPrice] = askRemain
        askOrderQty -= qty
        if askOrderQty == 0:
            self.orderDict.pop(OfferApplSeqNum)
        else:
            self.orderDict[OfferApplSeqNum] = (askOrderPrice, askOrderQty, askOrderSide, askOrderType, 1)
            
        self.cum_volume += qty
        self.cum_amount += price*qty
        self.close = price
        
        self.caaLs.append(caa)
        self.exchgTmLs.append(exchgTm)
        self.ApplSeqNumLs.append(ApplSeqNum)
        
        
    def insertOrder(self, caa, exchgTm, ApplSeqNum, side, orderType, price, qty):
                
        if self.isAuction:
            auctionCaa, auctionExchgTm, auctionApplSeqNum = self.caaLs[-1], self.exchgTmLs[-1], self.ApplSeqNumLs[-1]
            self.caaLs, self.exchgTmLs, self.ApplSeqNumLs = [], [], []
            self.updateMktInfo(auctionCaa, auctionExchgTm, auctionApplSeqNum, 1, record=True)
            self.isAuction = False
            
        if self.prevMktApplSeqNum > 0:
            hasUnfrozen = self.guessNBBO(caa, exchgTm, ApplSeqNum, self.aggSide, record=False)        
        
        if orderType == 2:
            if side == 1 and price < self.ask1p:
                if price in self.bidDict:
                    self.bidDict[price] += qty
                else:
                    self.bidDict[price] = qty
                isImprove = 1 if price > self.bid1p or self.prevMktApplSeqNum > 0 else 0
                self.updateMktInfo(caa, exchgTm, ApplSeqNum, isImprove, record=True)
                self.orderDict[ApplSeqNum] = (price, qty, side, orderType, 0)
                self.aggSide = 0
                    
            elif side == 2 and price > self.bid1p:
                if price in self.askDict:
                    self.askDict[price] += qty
                else:
                    self.askDict[price] = qty
                isImprove = 1 if price < self.ask1p or self.prevMktApplSeqNum > 0 else 0
                self.updateMktInfo(caa, exchgTm, ApplSeqNum, isImprove, record=True)
                self.orderDict[ApplSeqNum] = (price, qty, side, orderType, 0)
                self.aggSide = 0

            elif (side == 1 and price >= self.ask1p and price <= self.maxLmtBuyP) or \
                 (side == 2 and price <= self.bid1p and price >= self.minLmtSellP):
                if side == 1:
                    self.bidDict[price] = qty
                    self.aggSide = 1
                else:
                    self.askDict[price] = qty
                    self.aggSide = 2
                self.guessNBBO(caa, exchgTm, ApplSeqNum, self.aggSide, record=True)
                self.orderDict[ApplSeqNum] = (price, qty, side, orderType, 0)
                                
            elif (side == 1 and price > self.maxLmtBuyP) or (side == 2 and price < self.minLmtSellP):
                self.frozenOrderDict[ApplSeqNum] = (price, qty, side)
                if price in self.frozenApplSeqNumDict:
                    self.frozenApplSeqNumDict[price].append(ApplSeqNum)
                else:
                    self.frozenApplSeqNumDict[price] = [ApplSeqNum]
                if side == 1:
                    if price in self.frozenBidDict:
                        self.frozenBidDict[price] += qty
                    else:
                        self.frozenBidDict[price] = qty
                elif side == 2:
                    if price in self.frozenAskDict:
                        self.frozenAskDict[price] += qty
                    else:
                        self.frozenAskDict[price] = qty
                self.orderDict[ApplSeqNum] = (price, qty, side, 4, 0)
                isImprove = 1 if self.prevMktApplSeqNum > 0 else 0
                self.updateMktInfo(caa, exchgTm, ApplSeqNum, isImprove, record=True)
                self.orderDict[ApplSeqNum] = (price, qty, side, orderType, 0)
                self.aggSide = 0
            else:
                print('no suitable order ', ApplSeqNum, price, side)
            
        elif orderType == 1:
            if side == 1:
                self.bidDict[self.ask1p] = qty
                self.orderDict[ApplSeqNum] = (self.ask1p, qty, side, orderType, 0)
                self.aggSide = 1
            else:
                self.askDict[self.bid1p] = qty
                self.orderDict[ApplSeqNum] = (self.bid1p, qty, side, orderType, 0)
                self.aggSide = 2
                
        elif orderType == 3:
            if side == 1:
                self.orderDict[ApplSeqNum] = (self.bid1p, qty, side, orderType, 0)
                if self.bid1p in self.bidDict:
                    self.bidDict[self.bid1p] += qty
                    isImprove = 1 if self.prevMktApplSeqNum > 0 else 0
                    self.updateMktInfo(caa, exchgTm, ApplSeqNum, isImprove, record=True)
                else:
                    self.bidDict[self.bid1p] = qty
                    self.updateMktInfo(caa, exchgTm, ApplSeqNum, 0, record=False)
            else:
                self.orderDict[ApplSeqNum] = (self.ask1p, qty, side, orderType, 0)
                if self.ask1p in self.askDict:
                    self.askDict[self.ask1p] += qty
                    isImprove = 1 if self.prevMktApplSeqNum > 0 else 0
                    self.updateMktInfo(caa, exchgTm, ApplSeqNum, isImprove, record=True)
                else:
                    self.askDict[self.ask1p] = qty
                    self.updateMktInfo(caa, exchgTm, ApplSeqNum, 0, record=False)
            self.aggSide = 0
                    
        self.prevMktApplSeqNum = 0

                
    def removeOrderByTrade(self, caa, exchgTm, ApplSeqNum, price, qty, BidApplSeqNum, OfferApplSeqNum):
        
        bidOrderPrice, bidOrderQty, bidOrderSide, bidOrderType, hasTrade = self.orderDict[BidApplSeqNum]
        askOrderPrice, askOrderQty, askOrderSide, askOrderType, hasTrade = self.orderDict[OfferApplSeqNum]
        
        if self.prevMktApplSeqNum > 0 and BidApplSeqNum != self.prevMktApplSeqNum and OfferApplSeqNum != self.prevMktApplSeqNum:
            hasUnfrozen = self.guessNBBO(caa, exchgTm, ApplSeqNum, self.aggSide, record=True)
            
        hasUnfrozen = 0
        if BidApplSeqNum in self.frozenOrderDict:
            self.bidDict, self.frozenBidDict, self.frozenOrderDict, self.frozenApplSeqNumDict =\
            self.unfrozen(bidOrderPrice, 1, self.bidDict, self.frozenBidDict, self.frozenOrderDict, self.frozenApplSeqNumDict)
            hasUnfrozen = 1
        
        if OfferApplSeqNum in self.frozenOrderDict:
            self.askDict, self.frozenAskDict, self.frozenOrderDict, self.frozenApplSeqNumDict =\
            self.unfrozen(askOrderPrice, 2, self.askDict, self.frozenAskDict, self.frozenOrderDict, self.frozenApplSeqNumDict)
            hasUnfrozen = 1
        
        bidRemain = self.bidDict[bidOrderPrice] - qty
        if bidRemain == 0:
            self.bidDict.pop(bidOrderPrice)
        else:
            self.bidDict[bidOrderPrice] = bidRemain
        bidOrderQty -= qty
        if bidOrderQty == 0:
            self.orderDict.pop(BidApplSeqNum)
        else:
            self.orderDict[BidApplSeqNum] = (bidOrderPrice, bidOrderQty, bidOrderSide, bidOrderType, 1)
        
        if askOrderPrice not in self.askDict:
            print(exchgTm, ApplSeqNum, price, qty)
        
        askRemain = self.askDict[askOrderPrice] - qty
        if askRemain == 0:
            self.askDict.pop(askOrderPrice)
        elif askRemain > 0:
            self.askDict[askOrderPrice] = askRemain
        askOrderQty -= qty
        if askOrderQty == 0:
            self.orderDict.pop(OfferApplSeqNum)
        else:
            self.orderDict[OfferApplSeqNum] = (askOrderPrice, askOrderQty, askOrderSide, askOrderType, 1)
                        
        self.cum_volume += qty
        self.cum_amount += price*qty
        self.close = price
            
        if (self.aggSide == 1 and bidOrderType == 1 and bidOrderQty > 0) or (self.aggSide == 2 and askOrderType == 1 and askOrderQty > 0):
            self.prevMktApplSeqNum = BidApplSeqNum if self.aggSide == 1 else OfferApplSeqNum
        else:
            self.prevMktApplSeqNum = 0
                  
        if (self.aggSide == 1 and bidOrderType == 1 and bidOrderQty == 0) or (self.aggSide == 2 and askOrderType == 1 and askOrderQty == 0):
            hasUnfrozen = self.guessNBBO(caa, exchgTm, ApplSeqNum, self.aggSide, True)
        else:
            isImprove, hasUnfrozen = 0, 0
            self.updateMktInfo(caa, exchgTm, ApplSeqNum, isImprove, False)
        
    
    def removeOrderByCancel(self, caa, exchgTm, ApplSeqNum, cancelQty, BidApplSeqNum, OfferApplSeqNum):
        
        if self.isAuction:
            auctionCaa, auctionExchgTm, auctionApplSeqNum = self.caaLs[-1], self.exchgTmLs[-1], self.ApplSeqNumLs[-1]
            self.caaLs, self.exchgTmLs, self.ApplSeqNumLs = [], [], []
            self.updateMktInfo(auctionCaa, auctionExchgTm, auctionApplSeqNum, 1, True)
            self.isAuction = False
        
        hasUnfrozen = 0
        
        if self.prevMktApplSeqNum > 0:
            hasUnfrozen = self.guessNBBO(self, caa, exchgTm, ApplSeqNum, record=False)
        
        cancelApplSeqNum = max(BidApplSeqNum, OfferApplSeqNum)
        cancelPrice, cancelOrderQty, cancelSide, cancelOrderType, hasTrade = self.orderDict[cancelApplSeqNum]
        assert(cancelOrderQty == cancelQty)
        self.orderDict.pop(cancelApplSeqNum)
        print('2. done here')
        
        if cancelApplSeqNum in self.frozenOrderDict:
            self.frozenOrderDict.pop(cancelApplSeqNum)
            cancelIx = self.frozenApplSeqNumDict[cancelPrice].index(cancelApplSeqNum)
            self.frozenApplSeqNumDict[cancelPrice].pop(cancelIx)
            if cancelSide == 1:
                remain = self.frozenBidDict[cancelPrice] - cancelQty
                if remain == 0:
                    self.frozenBidDict.pop(cancelPrice)
                else:
                    self.frozenBidDict[cancelPrice] = remain
            else:
                remain = self.frozenAskDict[cancelPrice] - cancelQty
                if remain == 0:
                    self.frozenAskDict.pop(cancelPrice)
                else:
                    self.frozenAskDict[cancelPrice] = remain
            
            if self.prevMktApplSeqNum != 0:
                isImprove = 1
                self.updateMktInfo(caa, exchgTm, ApplSeqNum, isImprove, True)
                
        else:
            if cancelSide == 1:
                remain = self.bidDict[cancelPrice] - cancelQty
                if remain == 0:
                    self.bidDict.pop(cancelPrice) 
                else:
                    self.bidDict[cancelPrice] = remain
            elif cancelSide == 2:
                remain = self.askDict[cancelPrice] - cancelQty
                if remain == 0:
                    self.askDict.pop(cancelPrice)
                else:
                    self.askDict[cancelPrice] = remain

            if cancelOrderType == 1 and self.prevMktApplSeqNum > 0:
                isImprove = 1 if hasTrade == 1 else 0
            else:
                if (cancelSide == 1 and cancelPrice == self.bid1p and remain == 0) or \
                   (cancelSide == 2 and cancelPrice == self.ask1p and remain == 0) or \
                   (self.prevMktApplSeqNum > 0):
                    isImprove = 1
                else:
                    isImprove = 0
        
            print('3. done here')
                
            if remain == 0 and ((cancelSide == 1 and cancelPrice == self.bid1p) or (cancelSide == 2 and cancelPrice == self.ask1p)):
                hasUnfrozen = self.guessNBBO(caa, exchgTm, ApplSeqNum, cancelSide, record=True)
                
            else:
                self.updateMktInfo(caa, exchgTm, ApplSeqNum, isImprove, True)
                
        print('4. done here')
                
        self.prevMktApplSeqNum = 0
            

    def guessNBBO(self, caa, exchgTm, ApplSeqNum, aggSide, record=True):
        
        fakeBidDict = self.bidDict.copy()
        fakeAskDict = self.askDict.copy()
        
        hasTrade = 0
        fakeBidDict, fakeAskDict, fake_cum_volume, fake_cum_amount, fake_close, fakeBid1p, fakeAsk1p, fakeHasTrade = \
        self.matchTrades(fakeBidDict, fakeAskDict, self.cum_volume, self.cum_amount, self.close, aggSide)
        hasTrade = max(hasTrade, fakeHasTrade)
        
        hasUnfrozen = 0
        if aggSide == 1 and len(self.frozenBidDict) > 0:
            fakeMinBuyP = max(round(fakeAsk1p*1.02+self.EPSILON, -2), fakeAsk1p+100)
            while len(self.frozenBidDict) > 0 and min(self.frozenBidDict.keys()) <= fakeMinBuyP:
                fakeFrozenBidDict, fakeFrozenOrderDict, fakeFrozenApplSeqNumDict = self.frozenBidDict.copy(), self.frozenOrderDict.copy(), self.frozenApplSeqNumDict.copy()
                self.bidDict, self.frozenBidDict, self.frozenOrderDict, self.frozenApplSeqNumDict =\
                self.unfrozen(fakeMinBuyP, 1, self.bidDict, self.frozenBidDict, self.frozenOrderDict, self.frozenApplSeqNumDict)
                fakeBidDict, fakeFrozenBidDict, fakeFrozenOrderDict, fakeFrozenApplSeqNumDict = \
                self.unfrozen(fakeMinBuyP, 1, fakeBidDict, fakeFrozenBidDict, fakeFrozenOrderDict, fakeFrozenApplSeqNumDict)
                fakeBidDict, fakeAskDict, fake_cum_volume, fake_cum_amount, fake_close, fakeBid1p, fakeAsk1p, fakeHasTrade = \
                self.matchTrades(fakeBidDict, fakeAskDict, fake_cum_volume, fake_cum_amount, fake_close, aggSide)
                hasTrade = max(hasTrade, fakeHasTrade)
                fakeMinBuyP = max(round(fakeAsk1p*1.02+self.EPSILON, -2), fakeAsk1p+100)
                hasUnfrozen = 1
                
        if aggSide == 2 and len(self.frozenAskDict) > 0:
            fakeMaxSellP = min(round(fakeBid1p*0.98+self.EPSILON, -2), fakeBid1p-100)
            while len(self.frozenAskDict) > 0 and max(self.frozenAskDict.keys()) >= fakeMaxSellP:
                fakeFrozenAskDict, fakeFrozenOrderDict, fakeFrozenApplSeqNumDict = self.frozenAskDict.copy(), self.frozenOrderDict.copy(), self.frozenApplSeqNumDict.copy()
                self.askDict, self.frozenAskDict, self.frozenOrderDict, self.frozenApplSeqNumDict =\
                self.unfrozen(fakeMaxSellP, 2, self.askDict, self.frozenAskDict, self.frozenOrderDict, self.frozenApplSeqNumDict)
                fakeAskDict, fakeFrozenAskDict, fakeFrozenOrderDict, fakeFrozenApplSeqNumDict = \
                self.unfrozen(fakeMaxSellP, 2, fakeAskDict, fakeFrozenAskDict, fakeFrozenOrderDict, fakeFrozenApplSeqNumDict)
                fakeBidDict, fakeAskDict, fake_cum_volume, fake_cum_amount, fake_close, fakeBid1p, fakeAsk1p, fakeHasTrade = \
                self.matchTrades(fakeBidDict, fakeAskDict, fake_cum_volume, fake_cum_amount, fake_close, aggSide)
                hasTrade = max(hasTrade, fakeHasTrade)
                fakeMaxSellP = min(round(fakeBid1p*0.98+self.EPSILON, -2), fakeBid1p-100)
                hasUnfrozen = 1
                
        curBidP = sorted(fakeBidDict.keys(), reverse=True)[:self.outputLv]
        curAskP = sorted(fakeAskDict.keys())[:self.outputLv]

        if hasTrade == 0:
            if len(self.askDict) != 0:
                self.ask1p = curAskP[0]
            else:
                self.ask1p = curBidP[0] + 100

            if len(self.bidDict) != 0:
                self.bid1p = curBidP[0]
            else:
                self.bid1p = curAskP[0] - 100
            
            self.maxLmtBuyP = max(round(self.ask1p*1.02+self.EPSILON, -2), self.ask1p+100) 
            self.minLmtSellP = min(round(self.bid1p*0.98+self.EPSILON, -2), self.bid1p-100)
        
        if record:
            self.caaLs.append(caa)
            self.exchgTmLs.append(exchgTm)
            self.ApplSeqNumLs.append(ApplSeqNum)
            self.bboImproveLs.append(1)
            self.hasUnfrozenLs.append(hasUnfrozen)

            curBidQ = [fakeBidDict[i] for i in curBidP]
            self.bidp += [curBidP + [0]*(self.outputLv-len(curBidP))]
            self.bidq += [curBidQ + [0]*(self.outputLv-len(curBidQ))]

            curAskQ = [fakeAskDict[i] for i in curAskP]
            self.askp += [curAskP + [0]*(self.outputLv-len(curAskP))]
            self.askq += [curAskQ + [0]*(self.outputLv-len(curAskQ))]

            self.cum_volume_ls.append(fake_cum_volume)
            self.cum_amount_ls.append(fake_cum_amount)
            self.closeLs.append(fake_close)
            
        return hasUnfrozen

            
    def matchTrades(self, bidDict, askDict, cum_volume, cum_amount, close, aggSide):
        bidPLs = sorted(bidDict.keys(), reverse=True)
        askPLs = sorted(askDict.keys())
        bid1p = bidPLs[0] if len(bidPLs) > 0 else 0
        ask1p = askPLs[0] if len(askPLs) > 0 else 1000000
        if bid1p >= ask1p:
            while bid1p >= ask1p:
                bidQty = bidDict[bid1p]
                askQty = askDict[ask1p]
                tradeQty = min(bidQty, askQty)
                tradeP = bid1p if aggSide == 2 else ask1p
                cum_volume += tradeQty
                cum_amount += tradeQty*tradeP
                close = tradeP
                if tradeQty == bidQty:
                    bidDict.pop(bid1p)
                    bidPLs = bidPLs[1:]
                else:
                    bidDict[bid1p] -= tradeQty
                if tradeQty == askQty:
                    askDict.pop(ask1p)
                    askPLs = askPLs[1:]
                else:
                    askDict[ask1p] -= tradeQty

                if len(bidPLs) > 0 and len(askPLs) > 0:
                    bid1p = bidPLs[0]
                    ask1p = askPLs[0]
                elif len(bidPLs) == 0:
                    ask1p = askPLs[0]
                    bid1p = ask1p - 100
                elif len(askPLs) == 0:
                    bid1p = bidPLs[0]
                    ask1p = bid1p + 100
            hasTrade = 1
        else:
            hasTrade = 0

        return bidDict, askDict, cum_volume, cum_amount, close, bid1p, ask1p, hasTrade
    
    
    def unfrozen(self, price, side, orderbookDict, frozenOrderbookDict, frozenOrderDict, frozenApplSeqNumDict):
        
        if side == 1:
            frozenPLs = np.array(sorted(frozenOrderbookDict.keys(), reverse=True))
            frozenPLs = frozenPLs[frozenPLs <= price]
        else:
            frozenPLs = np.array(sorted(frozenOrderbookDict.keys()))
            frozenPLs = frozenPLs[frozenPLs >= price]
            
        for frozenP in frozenPLs:
            if frozenP in orderbookDict:
                orderbookDict[frozenP] += frozenOrderbookDict[frozenP]
            else:
                orderbookDict[frozenP] = frozenOrderbookDict[frozenP]        
            frozenOrderbookDict.pop(frozenP)
            frozenApplSeqNumLs = frozenApplSeqNumDict[frozenP]
            for frozenApplSeqNum in frozenApplSeqNumLs:
                frozenOrderDict.pop(frozenApplSeqNum)
            frozenApplSeqNumDict.pop(frozenP)
                
        return orderbookDict, frozenOrderbookDict, frozenOrderDict, frozenApplSeqNumDict
    
    
    def updateMktInfo(self, caa, exchgTm, ApplSeqNum, isImprove, record):
        curBidP = sorted(self.bidDict.keys(), reverse=True)[:self.outputLv]
        curAskP = sorted(self.askDict.keys())[:self.outputLv]
        
        if len(self.askDict) != 0:
            self.ask1p = curAskP[0]
        else:
            self.ask1p = curBidP[0] + 100
            
        if len(self.bidDict) != 0:
            self.bid1p = curBidP[0]
        else:
            self.bid1p = curAskP[0] - 100

        self.maxLmtBuyP = max(round(self.ask1p*1.02+self.EPSILON, -2), self.ask1p+100)
        self.minLmtSellP = min(round(self.bid1p*0.98+self.EPSILON, -2), self.bid1p-100)
        
        if record:
            self.caaLs.append(caa)
            self.exchgTmLs.append(exchgTm)
            self.ApplSeqNumLs.append(ApplSeqNum)
            self.bboImproveLs.append(isImprove)
            
            curBidQ = [self.bidDict[i] for i in curBidP]
            self.bidp += [curBidP + [0]*(self.outputLv-len(curBidP))]
            self.bidq += [curBidQ + [0]*(self.outputLv-len(curBidQ))]

            curAskQ = [self.askDict[i] for i in curAskP]
            self.askp += [curAskP + [0]*(self.outputLv-len(curAskP))]
            self.askq += [curAskQ + [0]*(self.outputLv-len(curAskQ))]

            self.cum_volume_ls.append(self.cum_volume)
            self.cum_amount_ls.append(self.cum_amount)
            self.closeLs.append(self.close)
            
            self.hasMktLeft = 0
        
        
    def getSimMktInfo(self):
        bidp = pd.DataFrame(self.bidp, columns=['bid%sp'%i for i in range(1, self.outputLv+1)])
        bidq = pd.DataFrame(self.bidq, columns=['bid%sq'%i for i in range(1, self.outputLv+1)])
        askp = pd.DataFrame(self.askp, columns=['ask%sp'%i for i in range(1, self.outputLv+1)])
        askq = pd.DataFrame(self.askq, columns=['ask%sq'%i for i in range(1, self.outputLv+1)])
        
        mdData = pd.DataFrame({'caa': self.caaLs, 'time': self.exchgTmLs,  'ApplSeqNum': self.ApplSeqNumLs,
                               'cum_volume': self.cum_volume_ls, 'cum_amount': self.cum_amount_ls, 'close': self.closeLs,
                               'bboImprove': self.bboImproveLs})
        for data in [bidp, bidq, askp, askq]:
            mdData = pd.concat([mdData, data], axis=1, sort=False)
        mdData['skey'] = self.skey
        mdData['date'] = self.date
        
        targetPLs = ['bid%sp'%i for i in range(self.outputLv, 0, -1)] + ['ask%sp'%i for i in range(1, self.outputLv+1)]
        targetQLs = ['bid%sq'%i for i in range(self.outputLv, 0, -1)] + ['ask%sq'%i for i in range(1, self.outputLv+1)]
        
        for col in ['cum_amount', 'close'] + targetPLs:
            mdData[col] = mdData[col]/10000
            mdData[col] = mdData[col].fillna(0)
        for col in ['cum_volume'] + targetQLs:
            mdData[col] = mdData[col].fillna(0)
            mdData[col] = mdData[col].astype('int64')
            
        closePrice = mdData['close'].values
        openPrice = closePrice[closePrice > 0][0]
        mdData['openPrice'] = openPrice
        mdData.loc[mdData['cum_volume'] == 0, 'openPrice'] = 0
        targetCols = ['skey', 'date', 'time', 'caa', 'ApplSeqNum', 'cum_volume', 'cum_amount', 'close'] + \
                     targetPLs + targetQLs + ['openPrice', 'bboImprove']
        mdData = mdData[targetCols].reset_index(drop=True)
        
        return mdData

