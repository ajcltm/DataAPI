from abc import ABC, abstractmethod

import pandas as pd
from tqdm import tqdm
import OpenDartReader


class StockProvider(ABC):
    """An interface for Stock Provider which give us some kind of stocks (i.e. common stock, prefered stock etc..)"""

    @abstractmethod
    def operation(self):
        pass

class commonStockProvider(StockProvider):
    """This is the concrete function which has resposibility for only 'common stock'"""
    def operation(self, path):
        stockN = pd.read_parquet(path/'stockNumberOfSharesDB.parquet')
        con = stockN.loc[:,'kindOfStock'] == 'commonStock'
        tickersLst = stockN.loc[con, 'ticker'].unique().tolist()
        return tickersLst

class NameProvider:
    def __init__(self, path, ticker):
        self.path = path
        self.ticker = ticker
    
    def get_name(self) :
        stockList = pd.read_parquet(self.path/'stockListDB.parquet')
        con = stockList.loc[:, 'ticker'] == self.ticker
        name = stockList.loc[con, 'name'].iat[-1]
        return name

class CorpCodeProvider:
    def __init__(self, ticker):
        api_key = '92c176817e681dcc4ad263eb3fa5182792b0b7a3'
        self.dart = OpenDartReader(api_key)
        self.ticker = ticker
    
    def get_corp_code(self):
        try:
            corp_code = self.dart.find_corp_code(self.ticker)
        except :
            corp_code = None
        return corp_code

class StockInfo:
    stockInfoDic = {}

    def __init__(self, path, StockProvider):
        commonStockTickers = StockProvider.operation(path)
        for ticker in tqdm(commonStockTickers) :
            self.stockInfoDic[ticker] = {'ticker' : ticker,
                                            'name' : NameProvider(path, ticker).get_name(),
                                            'corp_code' : CorpCodeProvider(ticker).get_corp_code()
                                            }


