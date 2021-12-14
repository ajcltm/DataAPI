import os
from pathlib import Path
import re
from tqdm import tqdm
import time

import pandas as pd
from datetime import datetime, timedelta

import sqlite3

from pykrx import stock

def createTickerDf(day) :
    tickers = stock.get_market_ticker_list(day, market="KOSPI")   # day에 해당하는 일자에 KOSPI에 존재하는 모든 ticker를 호출함 
    df = pd.DataFrame(tickers, columns=['ticker'])
    df = df.assign(name = df['ticker'].apply(lambda x: stock.get_market_ticker_name(x)))    # 존재하는 ticker에 대해 loop를 돌며 해당 ticker의 상호명을 새로운 열에 추가
    df = df.assign(key=df.apply(lambda x: x.loc['ticker']+x.loc['name'], axis=1))   # ticker와 name을 합쳐서 key값을 만듬(해당 ticker가 거리가 먼 서로 다른 일자에 다른 상호명으로 사용되었을 경우를 대비)
    return df

def filterDfs(filteringDf, filteredDf) :      # filetingDf = 필터하는 df / filteredDf = 필터 당하는 df
    filteringkeys = filteringDf['key'].tolist()     # 필터하는 df에 존재하는 key(tikcer+name값)을 리스트로 변환
    con = filteredDf['key'].isin(filteringkeys)     
    uniqueDf = filteredDf.loc[~con]     # filetingDf에는 존재하지만 filteredDf에는 존재하지 않는 ticker df 반환
    return uniqueDf

def creatingTickerInfoDf(day) :     # 가장 첫날에만 사용하는 함수(day = db의 가장 첫날)
    tickers = stock.get_market_ticker_list(day, market="KOSPI")  # day에 해당하는 일자에 KOSPI에 존재하는 모든 ticker를 호출함 
    df = pd.DataFrame(tickers, columns=['ticker'])
    df = df.assign(name = df['ticker'].apply(lambda x: stock.get_market_ticker_name(x)))     # 존재하는 ticker에 대해 loop를 돌며 해당 ticker의 상호명을 새로운 열에 추가
    df = df.assign(key=df.apply(lambda x: x.loc['ticker']+x.loc['name'], axis=1))    # ticker와 name을 합쳐서 key값을 만듬(해당 ticker가 거리가 먼 서로 다른 일자에 다른 상호명으로 사용되었을 경우를 대비)
    df = df.assign(firstDay=day)     # 첫날을 firstDay로 입력함
    df = df.assign(endDay=day)      # 첫날을 endDay로 입력함
    return df

def updateTickerInfoDf(stockListDf, delistedDf, listedDf, oldDay, newDay) :   #   stockListDf는 oldDay에 완성된 ticker df
    delistedKey = delistedDf['key'].tolist()
    con1 = stockListDf['key'].isin(delistedKey)
    con2 = stockListDf['endDay'].isin([oldDay])
    index = stockListDf.loc[(~con1)&(con2)].index
    stockListDf.loc[index, 'endDay'] = newDay   # 전날을 마지막으로 삭제되지 않았던(오늘 살아남은 tikcer들) 중 endDay가 전날로 되어 있는 ticker들의 endDay을 새로운 날로 연장시킴

    listedDf = listedDf.assign(firstDay=newDay)     # 오늘 새로 생긴 ticker의 firstDay는 오늘임
    listedDf = listedDf.assign(endDay=newDay)

    stockListDf = pd.concat([stockListDf, listedDf])
    stockListDf = stockListDf.reset_index(drop=True)

    return stockListDf

def getDayList(startDay, endDay):   # 해당 기간동안 존재하는 모든 일자를 리스트로 반환함
    # Day List
    start = datetime.strptime(startDay, "%Y%m%d")
    end = datetime.strptime(endDay, "%Y%m%d")
    date_generated = [start + timedelta(days=x) for x in range(0, (end-start).days+1)]

    dayList = []
    for date in date_generated:
        dayList.append(date.strftime("%Y%m%d"))

    return dayList

def updateDfByAllStock(oldDf, date) :       # stockPriceDb에 해당하는 함수
    date = date
    df = oldDf
    newDf = stock.get_market_ohlcv_by_ticker(date, market="KOSPI")
    cols = ['open', 'high', 'low', 'close', 'volume', 'volume($)', 'change']
    newDf.columns = cols
    newDf = newDf.assign(date = date)
    newDf.index.name = 'ticker'
    newDf = newDf.assign(name=newDf.apply(lambda x : stock.get_market_ticker_name(x.name), axis=1))
    newDf = newDf.reset_index()
    if not newDf.close.sum() == 0 :
        df = pd.concat([df, newDf])
    return df

def getPeriodStockListDf(folderPath, start, end) :
        stockListDf = pd.read_parquet(folderPath/'stockListDB.parquet')
        con1 = stockListDf.loc[:, 'firstDay'] < datetime.strptime(end, '%Y%m%d')
        con2 = stockListDf.loc[:, 'endDay'] > datetime.strptime(start, '%Y%m%d')
        stockListDf = stockListDf.loc[con1&con2]
        return stockListDf

def getStartEndDay(folderPath, start, end, ticker) :
    stockListDf = pd.read_parquet(folderPath/'stockListDB.parquet')
    tickerDf = stockListDf.loc[stockListDf.loc[:, 'ticker']==ticker]
    firstDay = tickerDf.loc[:, 'firstDay'].iat[-1]
    endDay = tickerDf.loc[:, 'endDay'].iat[-1]

    if firstDay > datetime.strptime(start, '%Y%m%d'):
        start = firstDay.strftime(format='%Y%m%d')
    else:
        start = start

    if endDay < datetime.strptime(end, '%Y%m%d'):
        end = endDay.strftime(format='%Y%m%d')
    else:
        end = end

    dic = {'start' : start, 'end' : end}
    return dic

def differenceDf(oldDay, newDay) :
    oldDf = createTickerDf(oldDay)     # 어제의 ticker df
    newDf = createTickerDf(newDay)     # 오늘의 ticker df
    delistedDf = filterDfs(newDf, oldDf)   # 어제를 마지막으로 오늘은 존재하지 않는 ticker df
    listedDf = filterDfs(oldDf, newDf) # 어제는 없었지만 오늘은 존재하는 ticker df
    return delistedDf, listedDf  

class StockData:
    def createDb(self, start, end) :
        pass
    
    def updateDb(self, today=datetime.today().strftime('%Y%m%d')) : 
        pass

class StockListDB(StockData) :
    def __init__(self, folderPath) :      # folderPath : DB파일을 보관하고 있는 경로 입력
        self.folderPath = folderPath

    def createDb(self, start, end) :
        dayList = getDayList(start, end)

        for k in tqdm(range(0, len(dayList))) :
            if k == 0 :
                stockListDf = creatingTickerInfoDf(dayList[k])
            else :
                oldDay = dayList[k-1]
                newDay = dayList[k]
                delistedDf, listedDf = differenceDf(oldDay, newDay)
                stockListDf = updateTickerInfoDf(stockListDf, delistedDf, listedDf, oldDay, newDay)
        stockListDf['firstDay'] = pd.to_datetime(stockListDf['firstDay'], format="%Y%m%d") # str(format 20211127) -> datetime
        stockListDf['endDay'] = pd.to_datetime(stockListDf['endDay'], format="%Y%m%d") # str(format 20211127) -> datetime
        stockListDf_ = stockListDf[['ticker', 'name', 'firstDay', 'endDay']]
        stockListDf_.to_parquet(self.folderPath/'stockListDB.parquet')

        conn = sqlite3.connect(self.folderPath/"stockListDB.db") 
        cur = conn.cursor()
        conn.execute(
            'CREATE TABLE stockListDB (id INTEGER PRIMARY KEY AUTOINCREMENT, ticker TEXT, name TEXT, firstDay TIMESTAMP, endDay TIMESTAMP)'
        )
        conn.commit()
        conn.close()

        stockListDf['firstDay']=stockListDf['firstDay'].apply(lambda x: x.strftime(format='%Y-%m-%d')) # datetime -> str (format 2021-11-27)
        stockListDf['endDay']=stockListDf['endDay'].apply(lambda x: x.strftime(format='%Y-%m-%d'))# datetime -> str (format 2021-11-27)

        connect = sqlite3.connect(self.folderPath/'stockListDB.db')
        cursor = connect.cursor()
        for row in stockListDf.itertuples():
            sql = "insert into stockListDB (ticker, name, firstDay, endDay) values (?, ?, ?, ?)"
            cursor.execute(sql, (row[1], row[2], row[4], row[5]))
        connect.commit()
        connect.close()

    def updateDb(self, today=datetime.today().strftime('%Y%m%d')) : 
        # query the exited DB and get the python dataframe
        conn = sqlite3.connect(self.folderPath/'stockListDB.db', isolation_level=None)
        stockListDf = pd.read_sql_query('SELECT * FROM stockListDB', conn)
        stockListDf = stockListDf.assign(key=stockListDf.apply(lambda x: x.loc['ticker']+x.loc['name'], axis=1))

        stockListDf['firstDay'] = pd.to_datetime(stockListDf['firstDay'], format='%Y-%m-%d') # str(format 2021-11-27) -> datetime
        stockListDf['endDay'] = pd.to_datetime(stockListDf['endDay'], format='%Y-%m-%d') # str(format 2021-11-27) -> datetime
        stockListDf['firstDay'] = stockListDf['firstDay'].apply(lambda x: x.strftime('%Y%m%d')) # datetime -> str (format 20211127)
        stockListDf['endDay'] = stockListDf['endDay'].apply(lambda x: x.strftime('%Y%m%d')) # datetime -> str (format 20211127)

        stockListDf = stockListDf.drop('id', axis=1)

        conn.close()
        stockListDf_ = stockListDf.copy()
        stockListDf_['endDay'] = pd.to_datetime(stockListDf_['endDay'], format='%Y%m%d')

        start = stockListDf_.sort_values(by='endDay')['endDay'].iat[-1].strftime('%Y%m%d')
        end = today
        dayList = getDayList(start, end)  # ex) if the last day of the exited df  : 20211008 -> inset(20211008, 20211011) 

        for k in tqdm(range(1, len(dayList))) :
            oldDay = dayList[k-1]
            newDay = dayList[k]
            delistedDf, listedDf = differenceDf(oldDay, newDay)
            stockListDf = updateTickerInfoDf(stockListDf, delistedDf, listedDf, oldDay, newDay)
        stockListDf['firstDay'] = pd.to_datetime(stockListDf['firstDay'], format="%Y%m%d") # str(format 20211127) -> datetime
        stockListDf['endDay'] = pd.to_datetime(stockListDf['endDay'], format="%Y%m%d") # str(format 20211127) -> datetime
        stockListDf['firstDay']=stockListDf['firstDay'].apply(lambda x: x.strftime(format='%Y-%m-%d')) # datetime -> str (format 2021-11-27)
        stockListDf['endDay']=stockListDf['endDay'].apply(lambda x: x.strftime(format='%Y-%m-%d'))# datetime -> str (format 2021-11-27)
        
        # 1. drop the existed DB -> 2. create all new DB -> 3. insert data into DB
        conn = sqlite3.connect(self.folderPath/"stockListDB.db") 
        cur = conn.cursor()
        conn.execute('DROP TABLE stockListDB')
        conn.execute(
            'CREATE TABLE stockListDB (id INTEGER PRIMARY KEY AUTOINCREMENT, ticker TEXT, name TEXT, firstDay TIMESTAMP, endDay TIMESTAMP)'
        )
        for row in stockListDf.itertuples():
            sql = "insert into stockListDB (ticker, name, firstDay, endDay) values (?, ?, ?, ?)"
            conn.execute(sql, (row[1], row[2], row[3], row[4]))
        conn.commit()
        conn.close()

        stockListDf['firstDay'] = pd.to_datetime(stockListDf['firstDay'], format='%Y-%m-%d') # str(format 2021-11-27) -> datetime
        stockListDf['endDay'] = pd.to_datetime(stockListDf['endDay'], format='%Y-%m-%d') # str(format 2021-11-27) -> datetime
        stockListDf.iloc[:, :-1].to_parquet(self.folderPath/'stockListDB.parquet')

class StockPriceDB(StockData):
    def __init__(self, folderPath) :      # folderPath : DB파일을 보관하고 있는 경로 입력
        self.folderPath = folderPath

    def createDb(self, start, end):
        conn = sqlite3.connect(self.folderPath/'stockPriceDB.db')
        cur = conn.cursor()
        conn.execute(
            'CREATE TABLE stockPriceDB (id INTEGER PRIMARY KEY AUTOINCREMENT, date TIMESTAMP, ticker TEXT,  name TEXT, open REAL, high REAL, low REAL, close REAL, volume REAL)')
        conn.commit()
        conn.close()

        cols = ['date','ticker', 'open', 'high', 'low', 'close', 'volume', 'name']
        stockPriceDB = pd.DataFrame(columns=cols)
        stockPriceDB.index.name='ticker'


        dayList = getDayList(start, end)

        for day in tqdm(dayList) :
            stockPriceDB = updateDfByAllStock(stockPriceDB, day)
            stockPriceDB = stockPriceDB[['date','ticker','name', 'open', 'high', 'low', 'close', 'volume']]
        stockPriceDB['date'] = pd.to_datetime(stockPriceDB['date'], format = '%Y-%m-%d')
        
        stockPriceDB = stockPriceDB.reset_index(drop=True)
        stockPriceDB['date'] = stockPriceDB['date'].apply(lambda x: x.strftime('%Y-%m-%d'))

        connect = sqlite3.connect(self.folderPath/'stockPriceDB.db')
        cursor = connect.cursor()
        for row in stockPriceDB.itertuples():
            sql = "insert into stockPriceDB (date, ticker, name, open, high, low, close, volume) values (?, ?, ?, ?, ?, ?, ?, ?)"
            cursor.execute(sql, (row[1], row[2], row[3], row[4], row[5], row[6], row[7], row[8]))
        connect.commit()
        connect.close()

        stockPriceDB['date'] = pd.to_datetime(stockPriceDB['date'], format = '%Y-%m-%d')
        stockPriceDB.to_parquet(self.folderPath/'stockPriceDB.parquet')
    
    def updateDb(self, today=datetime.today().strftime('%Y%m%d')) :  
        conn = sqlite3.connect(self.folderPath/'stockPriceDB.db', isolation_level=None)
        stockPriceDf = pd.read_sql_query('SELECT * FROM stockPriceDB', conn)
        conn.close()
        stockPriceDf['date'] = pd.to_datetime(stockPriceDf['date'], format='%Y-%m-%d')

        start = stockPriceDf.sort_values(by='date')['date'].iat[-1]  
        start = start + timedelta(days=1)  
        start = start.strftime('%Y%m%d')
        end = today
        dayList = getDayList(start, end)  # ex) if the last day of the exited df  : 20211008 -> inset(20211008, 20211011) 

        cols = ['date','ticker', 'open', 'high', 'low', 'close', 'volume', 'name']
        stockPriceDB = pd.DataFrame(columns=cols)
        stockPriceDB.index.name='ticker'

        for day in tqdm(dayList) :
            stockPriceDB = updateDfByAllStock(stockPriceDB, day)
            stockPriceDB = stockPriceDB[['date','ticker','name', 'open', 'high', 'low', 'close', 'volume']]
        stockPriceDB['date'] = pd.to_datetime(stockPriceDB['date'], format = '%Y-%m-%d')
        stockPriceDB
        
        stockPriceDB = stockPriceDB.reset_index(drop=True)
        stockPriceDB['date'] = stockPriceDB['date'].apply(lambda x: x.strftime('%Y-%m-%d'))

        connect = sqlite3.connect(self.folderPath/'stockPriceDB.db')
        cursor = connect.cursor()
        for row in stockPriceDB.itertuples():
            sql = "insert into stockPriceDB (date, ticker, name, open, high, low, close, volume) values (?, ?, ?, ?, ?, ?, ?, ?)"
            cursor.execute(sql, (row[1], row[2], row[3], row[4], row[5], row[6], row[7], row[8]))
        connect.commit()
        connect.close()

        stockPriceDB['date'] = pd.to_datetime(stockPriceDB['date'], format = '%Y-%m-%d')
        stockPriceDB = pd.concat([stockPriceDf, stockPriceDB], axis=0)
        stockPriceDB.reset_index(drop=True)
        stockPriceDB.iloc[:, 1:].to_parquet(self.folderPath/'stockPriceDB.parquet')

class StockShareDB(StockData):
    def __init__(self, folderPath) :      # folderPath : DB파일을 보관하고 있는 경로 입력
        self.folderPath = folderPath

    def getTickerNNofShares(self, day, stockName, df) :

        con3=df.loc[:, 'date'] == day
        con4=df.loc[:, 'name'] == stockName

        tempDf = df.loc[con3&con4]
        # ticker = tempDf.ticker.iat[0]
        ticker = tempDf.ticker.iat[0]
        nofShares = tempDf.nOfShare.iat[0]

        return [ticker, nofShares]

    def additionalInfo(self, day, originStock, df):
        originStockInfo = self.getOriginStockInfo(day, originStock)      # {'kind': 'preferredStock', 'filteredText': 'CJ'}
        filteredText = originStockInfo['filteredText']

        familyStock = self.getFamilyStock(day, filteredText)     # ['CJ', 'CJ우', 'CJ4우(전환)']


        dic = {'kindOfStock':''}
        for k in range(1,5) :
            dic[f'familyStock{k}'] = None     # {'kindOfStock': 'preferredStock', 'familyStock1': '', 'nOfShare_familyStock1': '', 'familyStock2': '', 'nOfShare_familyStock2': ''}
            dic[f'nOfShare_familyStock{k}'] = None
        num=0
        for stock in familyStock :
            index = familyStock.index(stock)
            if index==familyStock.index(originStock):
                dic['kindOfStock'] = originStockInfo['kind']
            else :
                num+=1
                dic[f'familyStock{num}'] = stock
                tickerNNofShares = self.getTickerNNofShares(day, stock, df)
                ticker = tickerNNofShares[0]
                nofShares = tickerNNofShares[1]
                dic[f'nOfShare_familyStock{num}'] = nofShares  # {'kindOfStock': 'preferredStock', 'familyStock1': 'CJ', 'nOfShare_familyStock1': '', 'familyStock2': 'CJ우', 'nOfShare_familyStock2': ''}
        nOfShare = list(dic.values())
        return nOfShare
    
    def uniqueTickerDf(self) :  # get tickersDF which have been existed since the year of 2010

        stockListDf = pd.read_parquet(self.folderPath/'stockListDB.parquet')

        groupbyDf = stockListDf.groupby('ticker').last()
        groupbyDf.firstDay = groupbyDf.firstDay.apply(lambda x: x.strftime('%Y%m%d'))
        groupbyDf.endDay = groupbyDf.endDay.apply(lambda x: x.strftime('%Y%m%d'))
        return groupbyDf
        #	        name	   key             firstDay    endDay
        # ticker				
        # 000020	동화약품	000020동화약품	20100101	20211008
        # 000030	우리은행	000030우리은행	20141119	20190212

    def getFirstEndDay(self, ticker) :
        groupbyDf = self.uniqueTickerDf()
        firstDay = groupbyDf.loc[ticker, 'firstDay']
        endDay = groupbyDf.loc[ticker, 'endDay']

        dic = {'firstDay' : firstDay, 'endDay' : endDay}

        return dic
        # {'firstDay': '20100101', 'endDay': '20211008'}

    def getNOfSharesDf(self, ticker):     # This is the first step which get the number of shares and we will get family stock info in the next step later.
        dic = self.getFirstEndDay(ticker)
        nOfShareDf = stock.get_market_cap_by_date(dic['firstDay'], dic['endDay'], ticker)
        nOfShareDf = nOfShareDf.reset_index()[['날짜', '상장주식수']]
        nOfShareDf.columns = ['date', 'nOfShare']
        nOfShareDf = nOfShareDf.assign(ticker = ticker)
        nOfShareDf = nOfShareDf.assign(name = stock.get_market_ticker_name(ticker))
        colsOrder = ['date', 'ticker', 'name', 'nOfShare']
        nOfShareDf = nOfShareDf[colsOrder]
        return nOfShareDf

    def getDayStockListDf(self, day) :
        stockListDf = pd.read_parquet(self.folderPath/'stockListDB.parquet')
        con1 = stockListDf.loc[:, 'firstDay'] <= day
        con2 = stockListDf.loc[:, 'endDay'] >= day
        stockListDf = stockListDf.loc[con1&con2]
        return stockListDf


    def getOriginStockInfo(self, day, originText) :  

        targetText = originText.replace(' ','') # delete blank texts

        pasedWoo = re.search(r'.+우', targetText) # check the target text whether including '우'
        if pasedWoo :
            spanWoo = pasedWoo.span()
            filteredText = targetText[spanWoo[0] : spanWoo[1]-1]
            # print(f'우:{filteredText}')
            pasedNumber = re.search(r'.+[0-9]$', filteredText) # check the 'woo' filtered text whether including number
            if pasedNumber :
                spanNumber = pasedNumber.span()
                filteredText = targetText[spanNumber[0] : spanNumber[1]-1]
            StockListDf = self.getDayStockListDf(day)
            con = StockListDf.name==filteredText
                
            if len(StockListDf.name.loc[con]) ==0 :
                kind = 'commonStock'
                filteredText = originText
            else : 
                kind = 'preferredStock'

        else :
            kind = 'commonStock'
            filteredText = originText
            # print(f'해당없음:{filteredText}')

        return {'kind' : kind, 'filteredText':filteredText}

    def checkIfRelative(self, targetText, x):
        p1 = re.findall(f'{targetText}\s?[0-9]?\s?[우]', x)
        p2 = re.findall(f'{targetText}$', x)
        if p1 or p2 :
            return True
        else :
            return False

    def getFamilyStock(self, day, targetText) : 
        stockListDf = self.getDayStockListDf(day)
        con = stockListDf.name.apply(lambda x : self.checkIfRelative(targetText,x))
        relativeStock = stockListDf.name.loc[con].tolist()
        return relativeStock

    def createDb(self, start, end) :
        lst = self.uniqueTickerDf().index.tolist()  # This is the first step which get DF for the number of shares and we will get family stock info in the next step later.
        lst = ['095570', '000540', '000545', '000547', '001460', '001465']
        dfs = []
        for ticker in tqdm(lst) :
            df = self.getNOfSharesDf(ticker)
            dfs.append(df)
        historicalDf = pd.concat(dfs)

        bool1 = historicalDf.loc[:, 'date'] >= datetime.strptime(start, '%Y%m%d')
        bool2 = historicalDf.loc[:, 'date'] <= datetime.strptime(end, '%Y%m%d')
        historicalDf = historicalDf.loc[bool1&bool2]
        
        cols = ['date', 'ticker', 'name', 'nOfShare', 'kindOfStock', 'familyStock1', 'nOfShare_familyStock1', 'familyStock2', 'nOfShare_familyStock2', 'familyStock3', 'nOfShare_familyStock3', 'familyStock4', 'nOfShare_familyStock4']
        historicalDf = historicalDf.sort_values(by='date').reset_index(drop=True)
        tqdm.pandas()
        expandDf = historicalDf.progress_apply(lambda row : self.additionalInfo(row['date'], row['name'], historicalDf[historicalDf.date==row['date']]), axis=1, result_type='expand')
        historicalDfFinal = pd.concat([historicalDf.reset_index(drop=True), expandDf.reset_index(drop=True)], axis=1, join='inner') # merge wiht family stock df with the step1 df 
        historicalDfFinal.columns = cols

        historicalDfFinal.to_parquet(self.folderPath/'stockNumberOfSharesDB.parquet')

        # creating DB file
        conn = sqlite3.connect(self.folderPath/'stockNumberOfSharesDB.db')
        cur = conn.cursor()
        conn.execute(
            'CREATE TABLE stockNumberOfSharesDB (id INTEGER PRIMARY KEY AUTOINCREMENT, date TIMESTAMP, ticker TEXT,  name TEXT, nOfShares REAL, kindOfStock TEXT, familyStock1 TEXT, nOfShare_familyStock1 REAL, familyStock2 TEXT, nOfShare_familyStock2 REAL, familyStock3 TEXT, nOfShare_familyStock3 REAL, familyStock4 TEXT, nOfShare_familyStock4 REAL)')
        conn.commit()
        conn.close()

        historicalDfFinal['date'] = historicalDfFinal['date'].apply(lambda x: x.strftime('%Y-%m-%d'))
        # inset data into DB
        connect = sqlite3.connect(self.folderPath/'stockNumberOfSharesDB.db')
        cursor = connect.cursor()
        for row in historicalDfFinal.itertuples():
            sql = "insert into stockNumberOfSharesDB (date, ticker, name, nOfShares, kindOfStock, familyStock1, nOfShare_familyStock1, familyStock2, nOfShare_familyStock2, familyStock3, nOfShare_familyStock3, familyStock4, nOfShare_familyStock4) values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
            cursor.execute(sql, (row[1], row[2], row[3], row[4], row[5], row[6], row[7], row[8], row[9], row[10], row[11], row[12], row[13]))
        connect.commit()
        connect.close()
    
    def updateDb(self, today=datetime.today().strftime('%Y%m%d')) :  
        df = pd.read_parquet(self.folderPath/'stockNumberOfSharesDB.parquet')
        dateS = df.sort_values(by='date').loc[:,'date'].iat[-1]
        start = dateS+timedelta(days=1)
        start = start.strftime('%Y%m%d')
        end = today

        tickers = getPeriodStockListDf(self.folderPath, start, end)['ticker'].unique().tolist()
        dfs=[]
        for ticker in tqdm(tickers) :
            time.sleep(.5)
            dic = getStartEndDay(self.folderPath, start, end, ticker)
            # print(dic['start'], dic['end'], ticker)
            nOfShareDf = stock.get_market_cap_by_date(dic['start'], dic['end'], ticker)
            nOfShareDf = nOfShareDf.reset_index()[['날짜', '상장주식수']]
            nOfShareDf.columns = ['date', 'nOfShare']
            nOfShareDf = nOfShareDf.assign(ticker = ticker)
            nOfShareDf = nOfShareDf.assign(name = stock.get_market_ticker_name(ticker))
            colsOrder = ['date', 'ticker', 'name', 'nOfShare']
            nOfShareDf = nOfShareDf[colsOrder]
            dfs.append(nOfShareDf)
        dfs = pd.concat(dfs)
        cols = ['date', 'ticker', 'name', 'nOfShare', 'kindOfStock', 'familyStock1', 'nOfShare_familyStock1', 'familyStock2', 'nOfShare_familyStock2', 'familyStock3', 'nOfShare_familyStock3', 'familyStock4', 'nOfShare_familyStock4']
        dfs = dfs.sort_values(by='date').reset_index(drop=True)
        tqdm.pandas()
        expandDf = dfs.progress_apply(lambda row : self.additionalInfo(row['date'], row['name'], dfs[dfs.date==row['date']]), axis=1, result_type='expand')
        dfFinal = pd.concat([dfs.reset_index(drop=True), expandDf.reset_index(drop=True)], axis=1, join='inner') # merge wiht family stock df with the step1 df 
        dfFinal.columns = cols

        historicalDf = pd.read_parquet(self.folderPath/'stockNumberOfSharesDB.parquet')
        newDf = pd.concat([historicalDf, dfFinal], ignore_index=True)  
        newDf.reset_index(drop=True).to_parquet(self.folderPath/'stockNumberOfSharesDB.parquet') 

        dfFinal['date'] = dfFinal['date'].apply(lambda x: x.strftime('%Y-%m-%d'))
        # inset data into DB
        connect = sqlite3.connect(self.folderPath/'stockNumberOfSharesDB.db')
        cursor = connect.cursor()
        for row in dfFinal.itertuples():
            sql = "insert into stockNumberOfSharesDB (date, ticker, name, nOfShares, kindOfStock, familyStock1, nOfShare_familyStock1, familyStock2, nOfShare_familyStock2, familyStock3, nOfShare_familyStock3, familyStock4, nOfShare_familyStock4) values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
            cursor.execute(sql, (row[1], row[2], row[3], row[4], row[5], row[6], row[7], row[8], row[9], row[10], row[11], row[12], row[13]))
        connect.commit()
        connect.close()

def testCreateDbStockList(folderPath, today):
    db = StockListDB(folderPath)
    db.updateDb(today)

def testCreateDbStockPrice(folderPath, today):
    db = StockPriceDB(folderPath)
    db.updateDb(today)

def testCreateDbShares(folderPath, today):
    db = StockShareDB(folderPath)
    db.updateDb(today)