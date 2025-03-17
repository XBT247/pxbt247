# Required dependencies and imports
import math
from copy import deepcopy

class TrendAware:
    def __init__(self, avgLimitH30, avgLimit=25, showLog=False, v=2, posClosingDelay=False, posClosingBand=False, showTable=False, tierCommission=0.075):
        # Public properties
        self.candleLength30 = 30
        self.candleLength100 = 100
        self.candleLength200 = 200
        self.queQty = []
        self.queQty100 = []
        self.queTrades = []
        self.queTrades100 = []
        self.quePrice200 = []
        self.quePrice100 = []
        self.priceAvgLimit = []
        self.priceDiffAvg = []
        self.h100s = []
        self.h30PosLast10s = []
        self.h30NegLast10s = []
        self.lastTrade = []
        self.thisTrade = []
        self.h30s = []  #qtr_history30
        self.h30Pos = []
        self.h30Neg = []

        self.pDiffs = []
        self.pDiffsLast6 = []
        self.pDiffsPos = []
        self.pDiffsNeg = []

        self.avgPosNegs = []
        self.avgPrices = []  #contains list of average prices, avgPrice at any given moment
        self.avgH100s = []   #contains list of average prices, avgH100 at any given moment
        self.avgPriceLast = 0
        self.avgH30Pos = 0
        self.avgH30PosLast = 0
        self.avgH30Neg = 0
        self.avgH30NegLast = 0
        self.avgH30Price = 0
        self.avgH30PriceLast = 0

        #holds current value of h30, h100 and average h100
        self.thisValH30 = 0
        self.thisValH100 = 0
        self.thisValAvgH100 = 0

        #this block will calculate the below at last
        self.dirAvgNegThis = 0
        self.dirAvgPosThis = 0
        self.dirAvgPriceThis = 0
        self.dirH30This = 0
        self.dirH100This = 0
        self.dirH100AvgThis = 0
        self.dirAvgNegLast = 0
        self.dirAvgPosLast = 0
        self.dirAvgPriceLast = 0
        self.dirPriceWRTAvg = 0

        #trends used in ta2
        self.trendAvgH30 = 0
        self.trendH30Pos = 0
        self.trendH30Neg = 0
        self.trendPrice = 0
        self.trendAvgPrice = 0

        self.posCountThis = 0
        self.posSumThis = 0
        self.negCountThis = 0
        self.negSumThis = 0
        self.openingPos = 0

        self.posCountLast = 0
        self.posSumLast  = 0
        self.negCountLast = 0
        self.negSumLast  = 0
        self.highlow200Band = []

        # "private" properties (no real private in Python, but preserving names)
        self.avgLimitH30 = avgLimitH30
        self.avgLimit = avgLimit
        self.color1 = ''
        self.color2 = ''
        self.h30Signal = ''
        self.avgPosNegLog = ''
        self.showLog = showLog
        self.showOnce = True

        #colors
        self.trendHighLow = []
        self.avgPriceDistance = []
        self.posOpen = []
        self.positions = []
        self.orderIncPos = []
        self.orderDecPos = []
        self.sideLOrder = []
        self.pnl = []
        self.trendColor = ''
        self.orderSidelined = False
        self.posClosingDelay = posClosingDelay
        self.posClosingBand = posClosingBand
        self.showTable = showTable
        self.counterClosingPos15 = False
        self.walletBtc = 0.1
        self.leverage = 50
        self.posPrcOfWallet = 10
        self.availablePosSize = 0
        self.posSize = 0
        self.rebateCommission = 0
        self.tierCommission = 0.075

        self.id = 0
        self.idc = 0
        self.v = v
        self.isNewCycleWait = 0
        self.isFalseColDir = False  # if Aqua going down or Red going up, then this is true
        self.cw = 'White'
        self.cy = 'Yellow'
        self.cg = 'Green'
        self.ca = 'Aqua'
        self.cp = 'Pink'
        self.cb = 'Blue'
        self.cr = 'Red'
        self.ck = 'Black'

        # Overwrite tierCommission with constructor parameter
        self.tierCommission = tierCommission

    async def AddLastTrade(self, lastTrade):
        self.lastTrade = lastTrade

    async def AddTrade(self, trade):
        self.thisTrade = trade
        await self.AddH30AvgLimit()

    async def AddH30AvgLimit(self):
        if len(self.queQty) >= self.candleLength30:
            self.queQty.pop(0)
            self.queTrades.pop(0)
        if len(self.queQty100) == self.candleLength100:
            self.queQty100.pop(0)
            self.queTrades100.pop(0)
            # array_shift for queQtySumHistory and queTradesSumHistory commented in original code
        if len(self.quePrice200) == self.candleLength200:
            self.quePrice200.pop(0)
        if len(self.h30s) > self.avgLimitH30:
            self.h30s.pop(0)
        if len(self.priceAvgLimit) > self.avgLimit:
            """
            ECHO count(self.priceAvgLimit) . '>' . self.avgLimit; 
            avgPrice = self.avgPrices[len(self.avgPrices)-1];
            avgPriceLast = self.avgPrices[len(self.avgPrices)-2];
            print("avgPrice:", avgPrice, "avgPriceLast:", avgPriceLast);
            EXIT;
            """
            self.priceAvgLimit.pop(0)
            self.h100s.pop(0)
        if len(self.quePrice100) >= self.candleLength100:
            self.quePrice100.pop(0)
        self.queQty.append(self.thisTrade['volumePoints'])
        self.queQty100.append(self.thisTrade['volumePoints'])
        self.queTrades.append(self.thisTrade['tradesPoints'])
        self.queTrades100.append(self.thisTrade['tradesPoints'])

        currentSumQueQty = sum(self.queQty)
        currentSumQueTrades = sum(self.queTrades)
        self.thisTrade['qtr_history30'] = (currentSumQueQty + currentSumQueTrades)
        self.h30s.append(self.thisTrade['qtr_history30'])

        self.thisTrade["sumVolumePoints100"] = sum(self.queQty100)
        self.thisTrade["sumTradePoints100"] = sum(self.queTrades100)
        self.thisTrade['qtr_history100'] = (self.thisTrade["sumVolumePoints100"] + self.thisTrade["sumTradePoints100"])
        self.h100s.append(self.thisTrade['qtr_history100'])

        self.priceAvgLimit.append(self.thisTrade['close'])
        self.quePrice100.append(self.thisTrade['close'])
        self.quePrice200.append(self.thisTrade['close'])
        self.thisTrade["high200"] = max(self.quePrice200)
        self.thisTrade["low200"] = min(self.quePrice200)

        # isSidelined
        self.thisTrade["isSidelined"] = False
        sidelineData = await self.IsSidelined()
        if sidelineData and "isSidelined" in sidelineData[0]:
            if sidelineData[0]["isSidelined"] == True or sidelineData[0]["isSidelined"] == 1:
                self.thisTrade["isSidelined"] = True

        # START Avg Price 
        if len(self.priceAvgLimit) > 0:
            avgPriceTemp = float(round(sum(self.priceAvgLimit) / len(self.priceAvgLimit), 1))
        if len(self.h100s) > 0:
            avgH100QtrTemp = float(round(sum(self.h100s) / len(self.h100s), 1))
        if len(self.avgPrices) > self.avgLimit:
            self.avgPrices.pop(0)
            self.avgH100s.pop(0)
        self.thisTrade["avgPrice"] = avgPriceTemp
        self.avgPrices.append(avgPriceTemp)
        self.avgH100s.append(avgH100QtrTemp)

        if 'qtr_history30' in self.thisTrade:
            self.thisValH30 = self.thisTrade['qtr_history30']
        if 'qtr_history100' in self.thisTrade:
            self.thisValH100 = self.thisTrade['qtr_history100']
        self.thisValAvgH100 = avgH100QtrTemp

        if len(self.priceDiffAvg) > 10:
            self.priceDiffAvg.pop(0)
        if len(self.avgPrices) >= 2:
            self.priceDiffAvg.append(round(avgPriceTemp - self.avgPrices[-2], 2))
        # END Avg Price 

        self.h30Pos = [v for v in self.h30s if v > 3]
        self.h30Neg = [v for v in self.h30s if v < -3]

        if 1 == 0 and self.showOnce and len(self.h30s) > self.avgLimitH30:
            self.showOnce = False
            print(self.h30s)
            print(self.h30Pos)
            print(self.h30Neg)
            print("<hr color='red'>")

    async def IsSidelined(self):
        sidelineData = []
        if len(self.quePrice100) < 100:
            return sidelineData
        # global strLog; (omitted as global not used in Python)
        arr70 = self.quePrice100[0:70]
        arrLast30 = self.quePrice100[-30:len(self.quePrice100)]
        # strLog appended commented out in original code

        max_val = max(arr70)
        min_val = min(arr70)
        if max(arrLast30) > max_val:
            max_val = max(arrLast30)
        if min(arrLast30) < min_val:
            min_val = min(arrLast30)
        diff = max_val - min_val

        # Defining start_time_str as empty string since it's not provided in original code
        start_time_str = ""
        sidelineData.append({
            "time": start_time_str,
            "max70": max(self.quePrice100),
            "min70": min(self.quePrice100),
            "max": max_val,
            "min": min_val,
            "diff": diff,
            "log": " SIDELINED (" + str(max_val) + "-" + str(min_val) + ")" + str(diff),
            "isSidelined": True
        })
        price = self.quePrice100[-1]
        bandWidth = 0.0025 * price
        if diff > bandWidth or diff < (bandWidth * -1):
            sidelineData[0]["isSidelined"] = False
            sidelineData[0]["log"] = ""
        return sidelineData

    async def AddPriceDiffs(self, closeDiffIndex):
        self.openingPos = 0
        while len(self.pDiffs) > 6:
            self.pDiffs.pop(0)
        if closeDiffIndex > 0:
            closeDiffIndex -= 1

        diffPrice = 0
        diffPrice = self.thisTrade["close"] - self.thisTrade["open"]
        # if(diffPrice > 0.5 or diffPrice < -0.5)
        self.pDiffs.append(diffPrice)

        diffPriceLast = 0
        diffPriceLast = self.thisTrade["open"] - self.lastTrade["close"]
        if diffPriceLast > 0.5 or diffPriceLast < -0.5:
            self.pDiffs.append(diffPriceLast)
            closeDiffIndex = 3

        pos = [v for v in self.pDiffs if v > 0.5]
        neg = [v for v in self.pDiffs if v < -0.5]

        self.posCountThis = len(pos)
        self.posSumThis = sum(pos)
        self.negCountThis = len(neg)
        self.negSumThis = sum(neg)

        del diffPrice
        del diffPriceLast
        return closeDiffIndex

    async def ta1(self, closeDiffIndex):
        diffPrice = self.thisTrade["close"] - self.thisTrade["open"]
        diffPriceLast = self.thisTrade["open"] - self.lastTrade["close"]

        avgPrice = self.avgPrices[-1]
        avgPriceLast = self.avgPrices[-2]

        pos = [v for v in self.pDiffs if v > 0.5]
        neg = [v for v in self.pDiffs if v < -0.5]

        self.posCountThis = len(pos)
        self.posSumThis = sum(pos)
        self.negCountThis = len(neg)
        self.negSumThis = sum(neg)

        isPos = True
        isNeg = True
        consecutiveCandlesToCheck = 2
        if (len(pos) > consecutiveCandlesToCheck and len(neg) > consecutiveCandlesToCheck) or closeDiffIndex > 0:
            if self.posSumThis < self.posSumLast:
                isPos = False
            if self.negSumThis > self.negSumLast:
                isNeg = False
            if self.posSumThis > (-1 * self.negSumThis):
                isNeg = False
            if self.negSumThis < (-1 * self.posSumThis):
                isPos = False
        """
        if(self.posCountThis > consecutiveCandlesToCheck and isPos):
            if(diffPrice > 0.5):
                self.openingPos = 1
                self.color1 = "#222"
            else:
                if(diffPrice < -0.5 and sum(self.priceDiffAvg) < 0):
                    self.color1 = "#8f1913"
                else if(diffPrice < -0.5):
                    self.color1 = "#420C09"
                else if(sum(self.priceDiffAvg) < 0):
                    self.color1 = "#554559"
            if(self.posSumThis < self.posSumLast):
                self.color2 = "#3D558A"
                self.openingPos = 0
            if(self.color2 != ""):
                cssBg = "background-image: linear-gradient(" + self.color1 + ", " + self.color2 + ");"
            else:
                cssBg = "background-color:" + self.color1 + ";"
            if(self.thisTrade['close'] < avgPrice):
                cssText = "color:#00cc00;"
                self.openingPos = 2
            if(diffPriceLast > 0.5 or diffPriceLast < -0.5):
                cssDiffLastBg = "background-color:#000;"
            # if(self.showLog) echo makeHtmlRow(cssBg, self.thisTrade, "B")
            # echo '<br>' + self.posCountThis + ' pos count'
        else if(self.negCountThis > consecutiveCandlesToCheck and isNeg):
            if(diffPrice < -0.5):
                self.color1 = "#222"
                self.openingPos = -1
            else:
                if(diffPrice > 0.5 and sum(self.priceDiffAvg) > 0):
                    self.color1 = "#8f1913"
                else if(diffPrice > 0.5):
                    self.color1 = "#420C09"
                else if(sum(self.priceDiffAvg) > 0):
                    self.color1 = "#554559"
            if(self.posSumThis > self.posSumLast):
                self.color2 = "#3D558A"
                self.openingPos = 0
            if(self.color2 != ""):
                cssBg = "background-image: linear-gradient(" + self.color1 + ", " + self.color2 + ");"
            else:
                cssBg = "background-color:" + self.color1 + ";"
            if(self.thisTrade['close'] > avgPrice):
                cssText = "color:#ff0099;"
                self.openingPos = -2
            if(diffPriceLast > 0.5 or diffPriceLast < -0.5):
                cssDiffLastBg = "background-color:#000;"
            # if(self.showLog) echo makeHtmlRow(cssBg, self.thisTrade, "S")
        """
        # End of commented block

    async def ta2(self):
        h30PosLast = sum(self.h30Pos)
        h30NegLast = sum(self.h30Neg)
        directionH30 = 0
        score = 0
        signal = ""
        if len(self.h30PosLast10s) > 10:
            self.h30PosLast10s.pop(0)
            """
            print(self.avgH30Pos, ', Neg=', self.avgH30Neg)
            print(self.h30PosLast10s)
            print(self.h30NegLast10s)
            """
        self.h30PosLast10s.append({"h30PosVal": h30PosLast, "close": self.thisTrade["close"]})
        if len(self.h30NegLast10s) > 10:
            self.h30NegLast10s.pop(0)
        self.h30NegLast10s.append({"h30NegVal": h30NegLast, "close": self.thisTrade["close"]})

        self.avgH30Pos = round(sum(item["h30PosVal"] for item in self.h30PosLast10s) / len(self.h30PosLast10s), 1)
        self.avgH30Neg = round(sum(item["h30NegVal"] for item in self.h30NegLast10s) / len(self.h30NegLast10s), 1)
        self.avgH30Price = round(sum(item["close"] for item in self.h30NegLast10s) / len(self.h30NegLast10s), 1)

        if len(self.avgPosNegs) > self.avgLimit:
            self.avgPosNegs.pop(0)
        self.avgPosNegs.append({
            "serverTime": self.thisTrade["serverTime"],
            "avgPos": self.avgH30Pos,
            "avgNeg": self.avgH30Neg,
            "avgPrice": self.avgH30Price
        })

        if self.avgH30PosLast == 0:
            self.avgH30PosLast = self.avgH30Pos
        if self.avgH30NegLast == 0:
            self.avgH30NegLast = self.avgH30Neg
        if self.avgH30PriceLast == 0:
            self.avgH30PriceLast = self.avgH30Price

        if len(self.h30PosLast10s) > 10:
            signal += str(self.avgH30Pos) + ' ' + str(self.avgH30Neg) + ' ' + str(self.avgH30Price)

        uniqueH30Pos = list({item["h30PosVal"] for item in self.h30PosLast10s})
        uniqueH30Neg = list({item["h30NegVal"] for item in self.h30NegLast10s})

        # overall trend
        # increase strength when greater than last strength
        if self.avgH30Pos >= (-1 * self.avgH30Neg):
            self.trendAvgH30 = 1
            if self.avgH30Pos > self.avgH30PosLast:
                self.trendAvgH30 = 2
            signal += " ~~Longs~~ "
        elif self.avgH30Pos < (-1 * self.avgH30Neg):
            self.trendAvgH30 = -1
            if self.avgH30Neg < self.avgH30NegLast:
                self.trendAvgH30 = -2
            signal += " ~~Shorts~~ "

        # both Pos & Neg increasing in strength
        if h30PosLast > self.avgH30Pos and h30NegLast < self.avgH30Neg:
            self.trendH30Neg = 1
            self.trendH30Pos = 1
        # both Pos & Neg decreasing in strength
        if h30PosLast < self.avgH30Pos and h30NegLast > self.avgH30Neg:
            self.trendH30Neg = -1
            self.trendH30Pos = -1

        # Price Trend
        # Increase strength when greater than last strength
        if self.thisTrade['close'] > self.avgH30Price:
            score = 1
            signal += " .priceUp. "
            self.trendPrice = 1
            if self.thisTrade['close'] > self.lastTrade['close']:
                self.trendPrice = 2
            else:
                self.trendPrice = 1.5
        else:
            score = -1
            signal += " .priceDown. "
            if self.thisTrade['close'] < self.lastTrade['close']:
                self.trendPrice = -2
            else:
                self.trendPrice = -1.5

        if h30PosLast > self.avgH30Pos:
            self.trendH30Pos = 1
            signal += " .h30Pos Up. "
            if self.h30PosLast10s[-2]["h30PosVal"] <= self.avgH30Pos:
                # trending up
                signal += " .h30Pos Trending up. "
                # self.openingPos = 1
        if h30NegLast < self.avgH30Neg:
            self.trendH30Neg = 1
            signal += " .h30Neg down. "
            if self.h30NegLast10s[-2]["h30NegVal"] >= self.avgH30Neg:
                # trending up
                signal += " .h30Neg Trending down. "
                # self.openingPos = -1

        if len(uniqueH30Neg) < 5:
            # negatives not moving
            self.trendH30Neg = 0
            signal += " .h30Neg not moving. "
        if len(uniqueH30Pos) < 5:
            # Positives not moving
            self.trendH30Pos = 0
            signal += " .h30Pos not moving. "

        del uniqueH30Pos
        del uniqueH30Neg
        return signal
    async def ta3(self, closeDiffIndex):
        self.openingPos = 0
        self.h30Signal = ''
        diffPrice = self.thisTrade["close"] - self.thisTrade["open"]
        diffPriceLast = self.thisTrade["open"] - self.lastTrade["close"]
        
        pos = []
        pos = list(filter(lambda v: v > 0.5, self.pDiffs))
        neg = []
        neg = list(filter(lambda v: v < -0.5, self.pDiffs))
        
        self.posCountThis = len(pos)
        self.posSumThis = sum(pos)
        self.negCountThis = len(neg)
        self.negSumThis = sum(neg)
        
        isPos = False
        isNeg = False
        consecutiveCandlesToCheck = 2
        #if(len(pos) >= consecutiveCandlesToCheck or len(neg) >= consecutiveCandlesToCheck or closeDiffIndex > 0)
    
        # 5/99	2/-92.5
        if self.posSumThis >= abs(self.negSumThis) and self.posCountThis >= self.negCountThis:
            isPos = True
            #self.h30Signal += 'a.1 '
        if abs(self.negSumThis) >= self.posSumThis and self.negCountThis >= self.posCountThis:
            isNeg = True
            #self.h30Signal += 'a.-1 '
        
        if isPos and isNeg:
            if self.thisTrade["close"] >= self.lastTrade["close"] - 0.5:
                isPos = True
                isNeg = False
                #self.h30Signal += 'aa.1 '
            elif self.thisTrade["close"] <= self.lastTrade["close"] + 0.5:
                isPos = False
                isNeg = True
                #self.h30Signal += 'aa.-1 '
        elif not isPos and not isNeg:
            #here posSum is greater but negCount is higher
            if self.posSumThis >= abs(self.negSumThis):
                #first check if price is stable
                if self.thisTrade["close"] >= self.lastTrade["close"] - 0.5 and self.thisTrade["close"] <= self.lastTrade["close"] + 0.5:
                    isPos = True
                    #self.h30Signal += 'ba.1 '
                elif self.posSumThis >= (abs(self.negSumThis) / 2):
                    isPos = True
                    #self.h30Signal += 'ba.1 '
            #here negSum is greater but posCount is higher
            if abs(self.negSumThis) >= self.posSumThis:
                #first check if price is stable
                if self.thisTrade["close"] >= self.lastTrade["close"] - 0.5 and self.thisTrade["close"] <= self.lastTrade["close"] + 0.5:
                    isNeg = True
                    #self.h30Signal += 'bb.-1 '
                elif abs(self.negSumThis) >= (self.posSumThis / 2):
                    isNeg = True
                    #self.h30Signal += 'bb.-1 '
    
        lastPriceDiff = self.pDiffs[-1] if self.pDiffs else 0
        if isPos:
            if lastPriceDiff > 0.5 or diffPriceLast > 0.5:
                #if(self.dirAvgPriceThis>0 and self.thisTrade["close"]> self.priceAvgLimit):
                #if(self.dirAvgPosThis>=0 and self.dirAvgNegThis<=0 and self.dirAvgPriceThis>0):
                #commenting in v2
                #self.openingPos = 1
                pass
            else:
                #self.h30Signal += ' !c.1 d=' + str(lastPriceDiff) + ',d1=' + str(diffPriceLast)
                pass
        if isNeg:
            if lastPriceDiff < -0.5 or diffPriceLast < -0.5:
                #if(self.dirAvgPriceThis<0 and self.thisTrade["close"]< self.priceAvgLimit):
                #if(self.dirAvgNegThis>=0 and self.dirAvgPosThis<=0 and self.dirAvgPriceThis<0):
                #commenting in v2
                #self.openingPos = -1
                pass
            else:
                #self.h30Signal += ' !c.-1 d=' + str(lastPriceDiff) + ',d1=' + str(diffPriceLast)
                pass

        await self.IsAvgPosMoving()
        await self.IsAvgNegMoving()
        await self.IsAvgPriceMoving()
        await self.IsH30Moving()
        await self.IsH100Moving()
        await self.IsH100AvgMoving()
        
        self.avgPosNegLog = (f"{self.avgH30Pos}/{self.dirAvgPosThis} \n"
                             f"{self.avgH30Neg}/{self.dirAvgNegThis} \n"
                             f"{self.avgH30Price}/{self.dirAvgPriceThis} \n"
                             f"h30:{self.dirH30This}/{'1' if self.thisValH30 > 0 else '-1'}\n"
                             f"h100:{self.dirH100This}/{'1' if self.thisValH100 > 0 else '-1'}/{'1' if self.thisValH100 > self.thisValAvgH100 else '-1'}\n"
                             f"h100Avg:{self.dirH100AvgThis}/{'1' if self.thisValAvgH100 > 0 else '-1'} {self.thisValAvgH100}")
        self.h30Signal += ' ' + self.avgPosNegLog

    async def SetDirAvgPosNegLast(self):
        self.dirAvgNegLast = self.dirAvgNegThis
        self.dirAvgPosLast = self.dirAvgPosThis
        self.dirAvgPriceLast = self.dirAvgPriceThis

    async def IsAvgNegMoving(self):
        if self.id > 0:
            print("<br>IsAvgNegMoving")
        avgH30Negs = [item["avgNeg"] for item in self.avgPosNegs][-19:]
        t = await self.GetTrendScore(avgH30Negs)
        if self.idc > 0:
            print("<br>dirAvgNegThis" + str(t))
        if t != 0:
            self.dirAvgNegThis = t

    async def IsAvgPosMoving(self):
        if self.id > 0:
            print("<br>IsAvgPosMoving" + str(self.avgLimitH30))
        avgH30Poses = [item["avgPos"] for item in self.avgPosNegs][-19:]
        t = await self.GetTrendScore(avgH30Poses)
        if self.idc > 0:
            print("<br>dirAvgPosThis" + str(t))
        if t != 0:
            self.dirAvgPosThis = t

    async def IsAvgPriceMoving(self):
        if self.id > 0:
            print("<br>IsAvgPriceMoving")
        avgH30Prices = [item["avgPrice"] for item in self.avgPosNegs][-11:]
        t = await self.GetTrendScore(avgH30Prices)
        if self.idc > 0:
            print("<br>dirAvgPriceThis" + str(t))
        if t != 0:
            self.dirAvgPriceThis = t

    async def IsH30Moving(self):
        if self.id > 0:
            print("<br>IsH30Moving")
        lastH30s = self.h30s[-19:]
        t = await self.GetTrendScore(lastH30s)
        if self.idc > 0:
            print("<br>dirH30This" + str(t))
        if t != 0:
            self.dirH30This = t

    async def IsH100Moving(self):
        if self.id > 0:
            print("<br>IsH100Moving")
        lastH100s = self.h100s[-19:]
        t = await self.GetTrendScore(lastH100s)
        if self.idc > 0:
            print("<br>dirH100This" + str(t))
        if t != 0:
            self.dirH100This = t

    async def IsH100AvgMoving(self):
        if self.id > 0:
            print("<br>IsH100AvgMoving")
        lastAvgH100 = self.avgH100s[-11:]
        t = await self.GetTrendScore(lastAvgH100)
        if self.idc > 0:
            print("<br>dirH100AvgThis" + str(t))
        if t != 0:
            self.dirH100AvgThis = t

    async def GetTrendScore(self, arr, isLog=False):
        if self.id > 0:
            print(arr)
        c = len(arr)
        score1 = 0
        score2 = 0
        #log = '[' + json.dumps(arr) + "]"
        for i in range(1, int(c/2) + 1):
            #log += '{' + str(arr[i-1])
            x = abs(arr[i-1])
            y = abs(arr[i])
            if (y - x) > 0:
                score1 += 1
            elif (y - x) < 0:
                score1 -= 1
            if self.id > 0:
                print("<br>score1:" + str(score1) + " ")
                print(str(y) + " - " + str(x))
        #log += "[" + str(int(c/2)) + "..."
        for i in range(int(c/2), c):
            x = abs(arr[i-1])
            y = abs(arr[i])
            if (y - x) > 0:
                score2 += 1
            elif (y - x) < 0:
                score2 -= 1
            if self.id > 0:
                print("<br>score2:" + str(score2) + " ")
                print(str(y) + " - " + str(x))
        #if(self.thisTrade['serverTime']=='2021-01-20 13:29:12'):
        #    print("<br>s1=" + str(score1) + ", s2=" + str(score2))
        #    print(arr)
        #return log + ']' + str(score1) + '|' + str(score2)
        dir = -1
        if score2 > score1:
            dir = 1
        if self.id > 0:
            print("score1=" + str(score1) + ", score2=" + str(score2))
        if abs(score1) == abs(score2):
            #print("GetTrendScore1 score1=" + str(score1) + ", score2=" + str(score2) + "<br>")
            #score1=0 score2=0  65 65 65 65 65 65 65 65 65 65
            if score1 == 0:
                return 0
            else:
                mid = round(c/2) - 1
                if abs(arr[c-1]) > abs(arr[mid]):
                    return 1  # echo " Score=1<br>"; return 1;
                if abs(arr[c-1]) < abs(arr[mid]):
                    return -1  # echo " Score=-1<br>"; return -1;
        if abs(score2) > abs(score1):
            if score2 < 0:
                return -1
            if score2 > 0:
                return 1
        elif abs(score2) < abs(score1):
            if score1 < 0:
                return -1
            if score1 > 0:
                return 1
        return 0

    async def AddLastSumPosNeg(self):
        self.posCountLast = self.posCountThis
        self.posSumLast = self.posSumThis
        self.negCountLast = self.negCountThis
        self.negSumLast = self.negSumThis
        #self.avgPriceLast = self.avgPrice
        
        self.avgH30PosLast = self.avgH30Pos
        self.avgH30NegLast = self.avgH30Neg
        self.avgH30PriceLast = self.avgH30Price
    
    async def SetTrendColor(self, dirPriceWRTAvg):
        self.dirPriceWRTAvg = dirPriceWRTAvg
        self.trendColor = await self.GetTrendColor(self.dirAvgPosThis, self.dirAvgNegThis, self.dirAvgPriceThis)
        addNewTrend = False
        countTrendArr = len(self.trendHighLow)
        if countTrendArr > 0:
            countCol = countTrendArr - 1
            self.trendHighLow[countCol]['isSidelined'] = self.thisTrade['isSidelined']
            if self.trendHighLow[countCol]['count'] > 3 and self.trendHighLow[countCol]['color'] != self.trendColor:
                addNewTrend = True
            else:
                #set high/low etc properties of this trend
                self.trendHighLow[countCol]['count'] = self.trendHighLow[countCol]['count'] + 1
                if self.thisTrade["close"] > self.thisTrade["avgPrice"]:
                    self.trendHighLow[countCol]["countAboveAvgPrice"] = self.trendHighLow[countCol]["countAboveAvgPrice"] + 1
                if self.thisTrade["close"] < self.trendHighLow[countCol]["low"]:
                    self.trendHighLow[countCol]["low"] = self.thisTrade["close"]
                if self.thisTrade["close"] > self.trendHighLow[countCol]["high"]:
                    self.trendHighLow[countCol]["high"] = self.thisTrade["close"]

                if dirPriceWRTAvg == 1:
                    if self.thisTrade['close'] > self.lastTrade['close']:
                        self.trendHighLow[countCol]["countdirPriceUp"] = self.trendHighLow[countCol].get("countdirPriceUp", 0) + 1
                    if self.thisTrade['close'] < self.lastTrade['close']:
                        self.trendHighLow[countCol]["countdirPriceDown"] = self.trendHighLow[countCol].get("countdirPriceDown", 0) + 1
                elif dirPriceWRTAvg == -1:
                    if self.thisTrade['close'] > self.lastTrade['close']:
                        self.trendHighLow[countCol]["countdirPriceUp"] = self.trendHighLow[countCol].get("countdirPriceUp", 0) + 1
                    if self.thisTrade['close'] < self.lastTrade['close']:
                        self.trendHighLow[countCol]["countdirPriceDown"] = self.trendHighLow[countCol].get("countdirPriceDown", 0) + 1
        else:
            addNewTrend = True
        if addNewTrend:
            if countTrendArr > 0:
                if self.thisTrade["close"] < self.trendHighLow[countCol]["low"]:
                    self.trendHighLow[countCol]["low"] = self.thisTrade["close"]
                if self.thisTrade["close"] > self.trendHighLow[countCol]["high"]:
                    self.trendHighLow[countCol]["high"] = self.thisTrade["close"]

                self.trendHighLow[countTrendArr - 1]['endServerTime'] = self.thisTrade['serverTime']
                self.trendHighLow[countTrendArr - 1]['close'] = self.thisTrade['close']
                self.trendHighLow[countTrendArr - 1]['endAvgPrice'] = self.thisTrade['avgPrice'] if 'avgPrice' in self.thisTrade else 0
                self.trendHighLow[countTrendArr - 1]['endBand'] = self.highlow200Band[len(self.highlow200Band) - 1]["subBand10"] if len(self.highlow200Band) > 0 else ""

            #get last 2 parent colors
            colorParent = ""
            colorParents = ""
            colorParentsLength = 0
            if countTrendArr > 2:
                colorParents = (self.trendHighLow[countTrendArr - 3]['color'][0] +
                                self.trendHighLow[countTrendArr - 2]['color'][0] +
                                self.trendHighLow[countTrendArr - 1]['color'][0])
                colorParentsLength = (self.trendHighLow[countTrendArr - 1]['count'] +
                                      self.trendHighLow[countTrendArr - 2]['count'] +
                                      self.trendHighLow[countTrendArr - 3]['count'])
            if countTrendArr > 0:
                colorParent = self.trendHighLow[countTrendArr - 1]['color'][0]
            if countTrendArr > self.avgLimit:
                self.trendHighLow.pop(0)

            self.trendHighLow.append({
                "startServerTime": self.thisTrade['serverTime'],
                "colorPNC": str(self.dirAvgPosThis) + "|" + str(self.dirAvgNegThis) + "|" + str(self.dirAvgPriceThis),
                "color": self.trendColor,
                "open": self.thisTrade['close'],
                "close": self.thisTrade['close'],
                "high": self.thisTrade['close'],
                "low": self.thisTrade['close'],
                "startAvgPrice": self.thisTrade['avgPrice'] if 'avgPrice' in self.thisTrade else 0,
                "endAvgPrice": self.thisTrade['avgPrice'] if 'avgPrice' in self.thisTrade else 0,
                "count": 1,
                "countdirPriceUp": 1 if self.thisTrade['close'] > self.lastTrade['close'] else 0,
                "countdirPriceDown": 1 if self.thisTrade['close'] < self.lastTrade['close'] else 0,
                "countAboveAvgPrice": 1 if ('avgPrice' in self.thisTrade and self.thisTrade['close'] > self.thisTrade['avgPrice']) else 0,
                "colorParent": colorParent,
                "colorParents": colorParents,
                "colorParentsLength": colorParentsLength,
                "startBand": self.highlow200Band[len(self.highlow200Band) - 1]["subBand10"] if len(self.highlow200Band) > 0 else "",
                "endBand": self.highlow200Band[len(self.highlow200Band) - 1]["subBand10"] if len(self.highlow200Band) > 0 else "",
                # "h30" and "h100" commented out as in original PHP code
                "bandPrcntByPrice": 0 if 'high200' not in self.thisTrade else round(((self.thisTrade['high200'] - self.thisTrade['low200']) / self.thisTrade['close']) * 100, 1),
                "isSidelined": 0 if 'isSidelined' not in self.thisTrade else self.thisTrade['isSidelined'],
                "high200": 0 if 'high200' not in self.thisTrade else self.thisTrade['high200'],
                "low200": 0 if 'low200' not in self.thisTrade else self.thisTrade['low200'],
                "falseColDirCount": 0
            })
        await self.IsThisFalseColorDir(dirPriceWRTAvg)
        return self.trendHighLow

    async def GetTrendColor(self, p, n, c):
        if self.idc > 0:
            print(str(self.avgLimitH30) + 'pnc=' + str(p) + str(n) + str(c))
        color = ''
        if p > 0 and n > 0 and c > 0:
            if abs(self.thisTrade['qtr_history30']) < 5 or self.thisTrade['close'] < self.thisTrade['avgPrice']:
                color = 'Yellow'
            else:
                color = 'White'
        elif p > 0 and n > 0 and c <= 0:
            color = 'Yellow'
        elif p > 0 and n <= 0 and c <= 0:
            if self.thisTrade['qtr_history30'] < -5 or self.thisTrade['close'] < self.thisTrade['avgPrice']:
                color = 'Green'
            else:
                color = 'Aqua'
        elif p > 0 and n <= 0 and c > 0:
            color = 'Aqua'
        elif p <= 0 and n > 0 and c > 0:
            if self.thisTrade['qtr_history30'] > 5 or self.thisTrade['close'] > self.thisTrade['avgPrice']:
                color = 'Pink'
            else:
                color = 'Red'
        elif p <= 0 and n <= 0 and c > 0:
            # $color = 'Blue'; //
            if self.thisTrade['qtr_history30'] < -5 or self.thisTrade['close'] < self.thisTrade['avgPrice']:
                color = 'Kblack'
            else:
                color = 'Blue'
        elif p <= 0 and n > 0 and c <= 0:
            color = 'Red'
        else:
            color = 'KblacK'
        return color

    async def PriceMaxDistanceFromAvgPrice(self, lastPriceDistance):
        isNewCycle = False
        avgPrice = self.avgPrices[len(self.avgPrices) - 1]
        # $thisPriceDistance = round($this->thisTrade['close']-$this->thisTrade['avgPrice'], 1);
        thisPriceDistance = 0
        if self.v == 1:
            thisPriceDistance = round(self.thisTrade['close'] - avgPrice, 1)
        if self.v == 2:
            thisPriceDistance = await self.GetDirCycle(round(self.thisTrade['close'] - avgPrice, 1), lastPriceDistance)
        countArr = len(self.avgPriceDistance)
        if countArr == 0:
            lastPriceDistance = thisPriceDistance
            isNewCycle = True
        elif thisPriceDistance == 0:
            self.isNewCycleWait -= 1
            self.openingPos = 0
            return 0

        if self.isNewCycleWait < 0:
            self.isNewCycleWait = 0
        # above avgPrice
        if lastPriceDistance >= 0:
            if thisPriceDistance < 0:
                countToCheck = 5
                if self.thisTrade['isSidelined'] == 1:
                    countToCheck = 9
                c25Last = self.trendHighLow[len(self.trendHighLow) - 1]
                isAqua = (c25Last['color'] != self.cr and c25Last['count'] > countToCheck and c25Last['countdirPriceUp'] > c25Last['countdirPriceDown'])
                isRed = (c25Last['color'] != self.ca and c25Last['count'] > countToCheck and c25Last['countdirPriceUp'] < c25Last['countdirPriceDown'])
                if isRed:
                    if self.showLog:
                        print(self.thisTrade['serverTime'] + " isRed=1 color:" + str(c25Last['color']) + "[" + str(c25Last['count']) + "] " + str(self.thisTrade['close'] < self.thisTrade['avgPrice']) + "<br>")
                    isNewCycle = True
                    self.openingPos = -1
        # below avgPrice
        elif lastPriceDistance <= 0:
            if thisPriceDistance > 0:
                countToCheck = 5
                if self.thisTrade['isSidelined'] == 1:
                    countToCheck = 9
                c25Last = self.trendHighLow[len(self.trendHighLow) - 1]
                isAqua = (c25Last['color'] != self.cr and c25Last['count'] > countToCheck and c25Last['countdirPriceUp'] > c25Last['countdirPriceDown'])
                if isAqua:
                    if self.showLog:
                        print(self.thisTrade['serverTime'] + " isAqua=1 color:" + str(c25Last['color']) + "[" + str(c25Last['count']) + "] " + str(self.thisTrade['close'] > self.thisTrade['avgPrice']) + "<br>")
                    isNewCycle = True
                    self.openingPos = 1
                else:
                    pass

        if countArr > 0:
            self.avgPriceDistance[countArr - 1]["count"] = self.avgPriceDistance[countArr - 1]["count"] + 1
            if abs(thisPriceDistance) > abs(self.avgPriceDistance[countArr - 1]["maxPriceDiff"]):
                self.avgPriceDistance[countArr - 1]["maxPriceDiff"] = thisPriceDistance
                self.avgPriceDistance[countArr - 1]["maxPriceDiffAt"] = self.thisTrade['close']
            maxPriceDistanceFromStart = round(self.thisTrade['close'] - self.avgPriceDistance[countArr - 1]["startPrice"], 1)
            if abs(maxPriceDistanceFromStart) > abs(self.avgPriceDistance[countArr - 1]["maxPriceDiffFromStart"]):
                self.avgPriceDistance[countArr - 1]["maxPriceDiffFromStart"] = maxPriceDistanceFromStart
                self.avgPriceDistance[countArr - 1]["maxPriceDiffFromStartAt"] = self.thisTrade['close']
            if isNewCycle:
                self.avgPriceDistance[countArr - 1]["endServerTime"] = self.thisTrade['serverTime']
                self.avgPriceDistance[countArr - 1]["endPrice"] = self.thisTrade['close']
                self.avgPriceDistance[countArr - 1]["colorEnd"] = self.trendHighLow[len(self.trendHighLow) - 1]['color']

        if isNewCycle and self.isNewCycleWait < 4:
            self.isNewCycleWait += 1
            return lastPriceDistance

        if isNewCycle:
            self.isNewCycleWait = 0
            if self.showLog:
                print(self.thisTrade['serverTime'] + " NEWW " + str(self.openingPos) + " " + str(self.thisTrade['close'] > self.thisTrade['avgPrice']) + "<br>")
                if len(self.trendHighLow) > 0:
                    pprint.pprint(self.trendHighLow[len(self.trendHighLow) - 1])
            self.avgPriceDistance.append({
                "count": 1,
                "colorStart": self.trendHighLow[len(self.trendHighLow) - 1]['color'] if len(self.trendHighLow) > 0 and 'color' in self.trendHighLow[len(self.trendHighLow) - 1] else '',
                "startServerTime": self.thisTrade['serverTime'],
                "endServerTime": self.thisTrade['serverTime'],
                "startPrice": self.thisTrade['close'],
                "endPrice": self.thisTrade['close'],
                "maxPriceDiffFromStart": thisPriceDistance,
                "maxPriceDiffFromStartAt": self.thisTrade['close'],
                "maxPriceDiff": thisPriceDistance,
                "maxPriceDiffAt": self.thisTrade['close'],
                "dir": self.openingPos
            })
        if thisPriceDistance != 0:
            lastPriceDistance = thisPriceDistance
        return lastPriceDistance

    async def SetBand(self, dirPriceWRTAvg):
        diffHighLow = (self.thisTrade["high200"] - self.thisTrade["low200"])
        diff10 = round(diffHighLow / 10)
        foundBand = 0
        high = round(self.thisTrade["high200"])
        low = (high - diff10)
        #1=Lowest, 10=Highest
        for i in range(10):
            if self.thisTrade['close'] <= high and self.thisTrade['close'] >= low:
                foundBand = 10 - i
            high = low
            low = (high - diff10)
        ct = 0
        if isinstance(self.highlow200Band, list):
            ct = len(self.highlow200Band) - 1
        if ct >= 0 and ct < len(self.highlow200Band) and self.highlow200Band[ct]["subBand10"] == foundBand:
            self.highlow200Band[ct]["count"] += 1
            if self.thisTrade["close"] > self.thisTrade["avgPrice"]:
                self.highlow200Band[ct]["countAboveAvgPrice"] = self.highlow200Band[ct].get("countAboveAvgPrice", 0) + 1
            if dirPriceWRTAvg == 1:
                if self.thisTrade['close'] > self.lastTrade['close']:
                    self.highlow200Band[ct]["countDirUp"] = self.highlow200Band[ct].get("countDirUp", 0) + 1
                if self.thisTrade['close'] < self.lastTrade['close']:
                    self.highlow200Band[ct]["countDirDown"] = self.highlow200Band[ct].get("countDirDown", 0) + 1
            elif dirPriceWRTAvg == -1:
                if self.thisTrade['close'] > self.lastTrade['close']:
                    self.highlow200Band[ct]["countDirUp"] = self.highlow200Band[ct].get("countDirUp", 0) + 1
                if self.thisTrade['close'] < self.lastTrade['close']:
                    self.highlow200Band[ct]["countDirDown"] = self.highlow200Band[ct].get("countDirDown", 0) + 1
        else:
            if ct >= 0 and ct < len(self.highlow200Band):
                self.highlow200Band[ct]["highEnd.."] = self.thisTrade["high200"]
                self.highlow200Band[ct]["lowEnd.."] = self.thisTrade["low200"]
                self.highlow200Band[ct]["endPrice.."] = self.thisTrade['close']
                self.highlow200Band[ct]["endServerTime.."] = self.thisTrade['serverTime']
            if ct > self.avgLimit:
                self.highlow200Band.pop(0)
            self.highlow200Band.append({
                "subBand10": foundBand,
                "count": 1,
                "colorStart": '',
                "countDirUp": 1 if self.thisTrade['close'] > self.lastTrade['close'] else 0,
                "countDirDown": 1 if self.thisTrade['close'] < self.lastTrade['close'] else 0,
                # "ptsDirUp" and "ptsDirDown" commented out as in original PHP code
                "countAboveAvgPrice": 1 if self.thisTrade['close'] > self.thisTrade['avgPrice'] else 0,
                "highStart": self.thisTrade["high200"],
                "highEnd..": self.thisTrade["high200"],
                "lowStart": self.thisTrade["low200"],
                "lowEnd..": self.thisTrade["low200"],
                "startServerTime": self.thisTrade['serverTime'],
                "endServerTime..": self.thisTrade['serverTime'],
                "startPrice": self.thisTrade['close'],
                "endPrice..": self.thisTrade['close']
            })

    async def GetDirCycle(self, thisPriceDistance, lastPriceDistance):
        countToCheck = 6
        if self.thisTrade['isSidelined'] == 1:
            countToCheck = 10
        c25Last = self.trendHighLow[len(self.trendHighLow) - 1]
        isAqua = (c25Last['color'] == self.ca and c25Last['count'] > countToCheck and c25Last['countdirPriceUp'] > c25Last['countdirPriceDown'])
        isRed = (c25Last['color'] == self.cr and c25Last['count'] > countToCheck and c25Last['countdirPriceUp'] < c25Last['countdirPriceDown'])
        vDown30 = (self.thisValH30 < 0 and thisPriceDistance < 0) or self.dirAvgPriceThis < 0
        vUp30 = (self.thisValH30 > 0 and thisPriceDistance > 0) or self.dirAvgPriceThis > 0

        dirUp100 = self.thisValAvgH100 > 0 and (self.thisValH100 > 0 or self.dirH100This > 0)
        dirUp101 = self.thisValAvgH100 < 0 and self.thisValH30 > 0 and self.dirAvgPriceThis > 0 and isAqua
        dirDown100 = self.thisValAvgH100 < 0 and (self.thisValH100 < 0 or self.dirH100This < 0)
        dirDown101 = self.thisValAvgH100 > 0 and self.thisValH100 < 0 and self.dirAvgPriceThis < 0 and isRed

        if (dirUp100 and dirDown100) or (not dirUp100 and not dirDown100):
            return lastPriceDistance
        else:
            if dirUp100 or dirUp101:
                if dirUp101:
                    return 3
                elif vUp30:
                    return 2
                else:
                    return 1
            if dirDown100 or dirDown101:
                if dirDown101:
                    return -3
                elif vDown30:
                    return -2
                else:
                    return -1
        return 0

    async def Wonder(self):
        isPosOpen = await self.CheckPositionAndOrders()
        if not isPosOpen:
            if self.v == 2 and self.dirPriceWRTAvg != 0:
                self.OpenPosition(self.posSize, self.dirPriceWRTAvg)
            countCol = len(self.trendHighLow) - 1

            #TO-DO ADD LIMIT ORDER NEAR AVGPRICE
            if self.dirPriceWRTAvg > 0:
                if self.trendHighLow[countCol]['countdirPriceUp'] > self.trendHighLow[countCol]['countdirPriceDown']:
                    if self.dirAvgPriceThis > 0 and self.avgH30Pos > 10:
                        self.OpenPosition(self.posSize, self.dirPriceWRTAvg)
                    else:
                        pass
            elif self.dirPriceWRTAvg < 0:
                if self.trendHighLow[countCol]['countdirPriceUp'] < self.trendHighLow[countCol]['countdirPriceDown']:
                    if self.dirAvgPriceThis < 0 and self.avgH30Neg < -10:
                        self.OpenPosition(self.posSize, self.dirPriceWRTAvg)
                    else:
                        pass

    async def IsThisFalseColorDir(self, lastPriceDistance):
        self.IsFalseColorDir = False  #NOT In USE
        c = len(self.trendHighLow)
        if c <= 0:
            return
        colorArr = self.trendHighLow[c - 1]
        countToCheck = 6
        if isinstance(self.thisTrade, dict) and self.thisTrade.get('isSidelined', 0) == 1:
            countToCheck = 10
        if ("count" not in colorArr) or (colorArr["count"] <= countToCheck):
            return
        if colorArr['color'] == 'Aqua' or colorArr['color'] == 'Pink' or colorArr['color'] == 'White':
            if (colorArr.get("countdirPriceUp", 0) < colorArr.get("countdirPriceDown", 0)
                and self.dirAvgPriceThis < 0 and lastPriceDistance < 0):
                self.IsFalseColorDir = True
                self.trendHighLow[c - 1]['falseColDirCount'] = self.trendHighLow[c - 1].get('falseColDirCount', 0) + 1
        if colorArr['color'] == 'Red' or colorArr['color'] == 'Green' or colorArr['color'] == 'Kblack':
            if (colorArr.get("countdirPriceDown", 0) < colorArr.get("countdirPriceUp", 0)
                and self.dirAvgPriceThis > 0 and lastPriceDistance < 0):
                self.IsFalseColorDir = True
                self.trendHighLow[c - 1]['falseColDirCount'] = self.trendHighLow[c - 1].get('falseColDirCount', 0) + 1

    async def setLastBandValues(self, bandIntVal, countLB, lastBand, band):
        if countLB[0] > 0 and lastBand[countLB[0]]["band"] == bandIntVal:
            lastBand[countLB[0]]["count"] += band["count"]
            lastBand[countLB[0]]["endServerTime"] = band["endServerTime.."]
            lastBand[countLB[0]]["endPrice"] = band["endPrice.."]
        else:
            lastBand.append({
                "band": bandIntVal,
                "count": band["count"],
                "startServerTime": band["startServerTime"],
                "endServerTime": band["endServerTime.."],
                "startPrice": band["startPrice"],
                "endPrice": band["endPrice.."]
            })
            countLB[0] = len(lastBand) - 1

    async def CalculateBandDirection(self):
        """
        1=Lowest, 10=Highest

        Long 10 9 8 7
        6 5
        SHORT 4 3 2 1
        """
        lastBand = []
        countLB = [0]
        #print(self.highlow200Band)
        for band in self.highlow200Band:
            if countLB[0] > 0 and countLB[0] >= len(lastBand):
                continue

            if band["subBand10"] > 6:
                await self.setLastBandValues(1, countLB, lastBand, band)
            if band["subBand10"] == 6 or band["subBand10"] == 5:
                await self.setLastBandValues(0, countLB, lastBand, band)
            if band["subBand10"] < 5:
                await self.setLastBandValues(-1, countLB, lastBand, band)
        print("<pre>")
        print(lastBand)
        
    async def CheckPositionAndOrders(self):
        isPosOpen = False
        if len(self.trendHighLow) > 0:
            trendHighLowColor = self.trendHighLow[-1]['color']
        else:
            trendHighLowColor = ''
        self.availablePosSize = self.walletBtc * self.leverage * self.thisTrade["close"]  # leveraged in USD now
        self.posSize = round(self.availablePosSize * (self.posPrcOfWallet / 100))
        c25Last = self.trendHighLow[-1] if len(self.trendHighLow) > 0 else {}

        if self.posOpen and len(self.posOpen) > 0:
            pos = self.posOpen[-1]
            # look for any IncreasePosition limit order fall in price
            for iOrder in self.orderIncPos:
                isFound = False

                if pos["sideDir"] == 1:
                    if iOrder['price'] <= self.lastTrade["close"] and iOrder['price'] >= self.thisTrade["low"]:
                        isFound = True
                elif pos["sideDir"] == -1:
                    if iOrder['price'] >= self.lastTrade["close"] and iOrder['price'] <= self.thisTrade["high"]:
                        isFound = True
                if isFound:
                    isFound = False
                    # print(self.posOpen)
                    exitValue = iOrder['size'] * (1 / self.thisTrade["close"])
                    comm = 0
                    if 'commission' in self.posOpen[-1]:
                        comm = self.posOpen[-1]['commission']
                    
                    self.posOpen[-1]['price'] = getAvgSharePrice(pos, iOrder)
                    self.posOpen[-1]['size'] = pos['size'] + iOrder['size']
                                        
                    self.posOpen[-1]["closeTime"] = self.thisTrade["serverTime"]
                    self.posOpen[-1]["closePrice"] = self.thisTrade["close"]
                    
                    self.posOpen[-1]["pnl"]["commission"] = self.posOpen[-1]["pnl"]["commission"] - exitValue * (self.rebateCommission / 100)
                    
                    self.posOpen[-1]["pnl"]["partialTimeInc"] = self.thisTrade["serverTime"]
                    self.posOpen[-1]["pnl"]["partialCommInc"] = self.posOpen[-1]["pnl"]["commission"]
                    self.orderIncPos.pop(0)
                    otext = " Adding Position " + str(pos['price'])
                    if pos["sideDir"] == 1:
                        otext += " Long"
                    else:
                        otext += " Short"
                    otext += " " + str(pos['size']) + " @ " + str(self.thisTrade["close"])
            
            # look for any DecreasePosition limit order fall in price
            for iOrder in self.orderDecPos:
                isFound = False
                
                if pos["sideDir"] == 1:
                    if iOrder['price'] >= self.lastTrade["close"] and iOrder['price'] <= self.thisTrade["close"]:
                        isFound = True
                elif pos["sideDir"] == -1:
                    if iOrder['price'] <= self.lastTrade["close"] and iOrder['price'] >= self.thisTrade["close"]:
                        isFound = True
                otext = ''
                if isFound:
                    isFound = False
                    profit = 0
                    if pos["sideDir"] == 1:
                        profit = iOrder['size'] * ((1 / pos['price']) - (1 / self.thisTrade["close"]))
                    elif pos["sideDir"] == -1:
                        profit = iOrder['size'] * ((1 / self.thisTrade["close"]) - (1 / pos['price']))
                    
                    self.posOpen[-1]['size'] = pos['size'] - iOrder['size']
                    self.orderDecPos.pop(0)
                    exitValue = iOrder['size'] * (1 / self.thisTrade["close"])
                    self.posOpen[-1]["closeTime"] = self.thisTrade["serverTime"]
                    self.posOpen[-1]["closePrice"] = self.thisTrade["close"]
                    self.posOpen[-1]["pnl"]["avgs"] = self.avgPosNegLog
                    self.posOpen[-1]["pnl"]["exitTime"] = self.thisTrade["serverTime"]
                    self.posOpen[-1]["pnl"]["sideDir"] = pos["sideDir"]
                    self.posOpen[-1]["pnl"]["exitPrice"] = self.thisTrade["close"]
                    if "exitSize" not in self.posOpen[-1]["pnl"]:
                        self.posOpen[-1]["pnl"]["exitSize"] = 0
                    self.posOpen[-1]["pnl"]["exitSize"] += iOrder['size']
                    if "pnl" not in self.posOpen[-1]["pnl"]:
                        self.posOpen[-1]["pnl"]["pnl"] = 0
                    self.posOpen[-1]["pnl"]["pnl"] += profit                    
                    self.posOpen[-1]["pnl"]["commission"] = self.posOpen[-1]["pnl"]["commission"] - profit - exitValue * (self.rebateCommission / 100)
                    
                    self.posOpen[-1]["pnl"]["partialTimeDec"] = self.thisTrade["serverTime"]
                    self.posOpen[-1]["pnl"]["partialCommDec"] = self.posOpen[-1]["pnl"]["commission"]
                    # UpdateOrderText(self.thisTrade, 0, 'Dec ')
                    if self.posOpen[-1]['size'] == 0:
                        self.orderIncPos = []
                        self.orderDecPos = []
                        self.positions.append(self.posOpen)
                        pos = {}
                        self.posOpen = []
                        
                        # show Log
                        otext = " Closing " + str(pos.get('price', ''))
                        if pos.get("sideDir", 0) == 1:
                            otext += " Long"
                        else:
                            otext += " Short"
                        otext += " " + str(pos.get('size', '')) + " @ " + str(self.thisTrade["close"])
                        # if showPosLog or self.showTable: print(makeOrderHtmlRow(self.thisTrade, otext, pos.get("sideDir", 0), 0))
                        
                        break
                    else:
                        # show Log
                        otext = " Decreasing Position " + str(pos['price'])
                        if pos["sideDir"] == 1:
                            otext += " Long"
                        else:
                            otext += " Short"
                        otext += " " + str(pos['size']) + " @ " + str(self.thisTrade["close"])
            
            posColors = self.posOpen[-1]['trendHighLowForPos']
            totalPosColorTicks = sum(item['count'] for item in posColors)
            lastColor = self.posOpen[-1]['trendHighLowForPos'][-1]['color']
            lastColorCount = self.posOpen[-1]['trendHighLowForPos'][-1]['count']
            # maintain high low of this color in Position
            posColors = self.posOpen[-1]['trendHighLowForPos']
            if lastColor == self.trendColor or self.trendColor == "":
                self.posOpen[-1]['trendHighLowForPos'][-1]['count'] += 1
                self.posOpen[-1]['trendHighLowForPos'][-1]["close"] = self.thisTrade["close"]
                if self.thisTrade["close"] < self.posOpen[-1]['trendHighLowForPos'][-1]["low"]:
                    self.posOpen[-1]['trendHighLowForPos'][-1]["low"] = self.thisTrade["close"]
                if self.thisTrade["close"] > self.posOpen[-1]['trendHighLowForPos'][-1]["high"]:
                    self.posOpen[-1]['trendHighLowForPos'][-1]["high"] = self.thisTrade["close"]
    
                if self.dirPriceWRTAvg == 1:
                    if self.thisTrade['close'] > self.lastTrade['close']:
                        self.posOpen[-1]['trendHighLowForPos'][-1]["countdirPriceUp"] += 1
                    if self.thisTrade['close'] < self.lastTrade['close']:
                        self.posOpen[-1]['trendHighLowForPos'][-1]["countdirPriceDown"] += 1
                elif self.dirPriceWRTAvg == -1:
                    if self.thisTrade['close'] > self.lastTrade['close']:
                        self.posOpen[-1]['trendHighLowForPos'][-1]["countdirPriceUp"] += 1
                    if self.thisTrade['close'] < self.lastTrade['close']:
                        self.posOpen[-1]['trendHighLowForPos'][-1]["countdirPriceDown"] += 1
            else:
                self.posOpen[-1]['trendHighLowForPos'].append({
                    "startServerTime": self.thisTrade['serverTime'],
                    "color": self.trendColor,
                    "open": self.thisTrade['close'],
                    "close": self.thisTrade['close'],
                    "high": self.thisTrade['close'],
                    "low": self.thisTrade['close'],
                    "startAvgPrice": self.thisTrade['avgPrice'],
                    "endAvgPrice": self.thisTrade['avgPrice'],
                    "count": 1,
                    "countdirPriceUp": 1 if self.thisTrade['close'] > self.lastTrade['close'] else 0,
                    "countdirPriceDown": 1 if self.thisTrade['close'] < self.lastTrade['close'] else 0,
                    "countAboveAvgPrice": 1 if self.thisTrade['close'] > self.thisTrade['avgPrice'] else 0,
                })
                            
            isPosOpen = True
            
            # detect any trend reversal
            if c25Last.get('count', 0) > 4:
                if pos["sideDir"] == 1:
                    if c25Last.get('countdirPriceUp', 0) < c25Last.get('countdirPriceDown', 0):
                        if self.trendColor != self.ca and self.trendColor != self.cw:
                            if len(posColors) > 1 and totalPosColorTicks > 10:
                                if self.avgH30Neg == 0 and pos["price"] < self.thisTrade['close'] and trendHighLowColor != 'black':
                                    pass
                                else:
                                    self.ClosePosition()
                elif pos["sideDir"] == -1:
                    if c25Last.get('countdirPriceUp', 0) > c25Last.get('countdirPriceDown', 0):
                        if self.trendColor != self.cr and self.trendColor != self.ck:
                            if len(posColors) > 1 and totalPosColorTicks > 10:
                                if self.avgH30Pos == 0 and pos["price"] > self.thisTrade['close'] and trendHighLowColor != 'White':
                                    pass
                                else:
                                    self.ClosePosition()
            
            if pos["sideDir"] == 1 and self.dirPriceWRTAvg == -1:
                # Keep into profit trade until positive Vol appear
                if self.dirAvgPriceThis > 0 or (self.avgH30Neg == 0 and pos["price"] < self.thisTrade['close']):  # and trendHighLowColor!='black'
                    pass
                else:
                    # echo '<BR>CLOSING long dirPriceWRTAvg==-1';
                    self.ClosePosition()
                # ClosePosition();
                # continue;
                """
                if c25Last['countdirPriceUp'] < c25Last['countdirPriceDown']:
                    if self.trendColor != self.ca and self.trendColor != self.cw and self.trendColor != self.cg:
                        ClosePosition();
                        continue;
                """
            elif pos["sideDir"] == -1 and self.dirPriceWRTAvg == 1:
                # if(self.trendColor != self.cr and self.trendColor != self.ck )
                # Keep into profit trade until positive Vol appear
                if self.dirAvgPriceThis > 0 or (self.avgH30Pos == 0 and pos["price"] > self.thisTrade['close']):  # and trendHighLowColor!='White'
                    pass
                else:
                    # echo '<BR>CLOSING short dirPriceWRTAvg==1';
                    self.ClosePosition()
                # ClosePosition();
                # continue;
                """
                if c25Last['countdirPriceUp'] > c25Last['countdirPriceDown']:
                    if self.trendColor != self.cr and self.trendColor != self.ck and self.trendColor != self.cp:
                        ClosePosition();
                        continue;
                """
        return isPosOpen