from dataclasses import dataclass

import numpy as np
import pandas as pd
import report


class ValueIdentifier:
    columns_parser = r'^.*[0-9]*.?[기]'
    numeric_parser = r'^[0-9]*\.?[0-9]$'
    def identifyColumns(self, df):
        for col in df.columns:
            con = df.loc[:, col].str.contains(self.columns_parser, regex=True)
            print(f'identified columns : {df.loc[con]}')
            result = df.loc[con]
            return result
    def identifyNumeric(self, df):
        # con_ = sr.str.contains(self.numeric_parser, regex=True)
        # print(f'identifyNumeric : {sr.loc[con_]}')
        # result = sr.loc[con_].iat[0]
        df = df.apply(lambda x: pd.to_numeric(x, errors='ignore'))
        for col in df.columns:
            test_df = df.loc[:, col]
            print(f'value : {test_df.values}')
            dtype = test_df.values.dtype
            print(f'dtype : {dtype}')
            bool1 = dtype == 'float'
            bool2 = dtype == 'int64'
            print(f'float_bool : {bool1}')
            print(f'int_bool : {bool2}')
            
            if dtype == 'float':
                con = test_df == 0
                return test_df[~con].iat[0]
            if dtype =='int64':
                con = test_df == 0
                return test_df[~con].iat[0]

        return None
        

class ReportPreprocessor:

    def __init__(self, report):
        self.report = report

    def operation(self):
        self.report = self.report.apply(lambda col: col.astype(str), axis=1)
        # self.report = self.report.apply(lambda col: col.str.replace(' ', ''), axis=1)
        # self.report = self.report.apply(lambda col: pd.to_numeric(col, errors='ignore'), axis=1)

        return self.report

class EquityProvider:

    parserLst = ['^자본총계$']

    def __init__(self):
        self.__report = None

    def set(self, report):
        self.__report = report

    def get_values(self):
        for col in self.__report.columns:
            for parser in self.parserLst:
                con = self.__report.loc[:, col].str.contains(parser, regex=True)
                sr = self.__report.loc[con]
                if not sr.empty :
                    df = sr.squeeze().reset_index()
                    print(f'series to df including value : {df}')
                    df_ = ValueIdentifier().identifyColumns(df)
                    if isinstance(df_, pd.DataFrame):
                        print(df_)
                        value = ValueIdentifier().identifyNumeric(df_)
                        print(value)
                        value = int(float(value))
                        print(f'equity : {value}', type(value), sep=', ')
                        return value
                else:
                    return None

class LiabilityProvider:

    parserLst = ['^부채총계$']

    def __init__(self):
        self.__report = None

    def set(self, report):
        self.__report = report

    def get_values(self):
        for col in self.__report.columns:
            for parser in self.parserLst:
                con = self.__report.loc[:, col].str.contains(parser, regex=True)
                sr = self.__report.loc[con]
                if not sr.empty:
                    df = sr.squeeze().reset_index()
                    print(f'series to df including value : {df}')
                    df_ = ValueIdentifier().identifyColumns(df)
                    if isinstance(df_, pd.DataFrame):
                        value = ValueIdentifier().identifyNumeric(df_)
                        value = int(float(value))
                        print(f'liability : {value}', type(value), sep=', ')
                        return value
                else:
                    return None

class ValueSearcher:

    def __init__(self, report, provider):
        self.report = report
        self.provider = provider

    def search(self):
        if not isinstance(self.report, pd.DataFrame):
            return None
        print('-'*50)
        report = ReportPreprocessor(self.report).operation()
        self.provider.set(report)
        value = self.provider.get_values()
        print(f'valueSearcher result : {value}')
        if value :
            return value
        return None

@dataclass
class FundamentalValues:
    consolidatedEquity:int
    consolidatedLiability:int
    equity:int
    liability:int

class FundamentalValuesProvider:

    def __init__(self, rcept_no):
        self.rcept_no = rcept_no

    def get_fundamental_value(self):
        set_of_reports = report.ReportSetter(self.rcept_no).get_set_of_report()

        ep = EquityProvider()
        lp = LiabilityProvider()

        consolidated_balance_sheet = set_of_reports['consolidated_balance_sheet']
        consolidatedEquity = ValueSearcher(consolidated_balance_sheet, ep).search()
        consolidatedliability = ValueSearcher(consolidated_balance_sheet, lp).search()    

        balance_sheet = set_of_reports['balance_sheet']
        equity = ValueSearcher(balance_sheet, ep).search()    
        liability = ValueSearcher(balance_sheet, lp).search()  

        data = FundamentalValues(consolidatedEquity, consolidatedliability, equity, liability)

        return data



if __name__ == '__main__' :

    from pathlib import Path
    import random
    import stockInfo
    import rceptnoInfo

    path = Path.home().joinpath('Desktop', 'dataBackUp(211021)')

    stockList = pd.read_parquet(path/'stockListDB.parquet')
    tickers = stockList.ticker.unique().tolist()
    ticker = random.choice(tickers)

    commonStockProvider = stockInfo.commonStockProvider()
    stockinfo = stockInfo.StockInfo(path, commonStockProvider)
    stockInfoDic = stockinfo.get_stockInfo(ticker)
    corp_code = stockInfoDic[ticker]['corp_code']
    print('='*150)
    print(f'target : {stockInfoDic[ticker]}')

    preprocessor = rceptnoInfo.PreprocessorRceptnoInfo()
    rc = rceptnoInfo.RceptnoInfo(preprocessor)
    rceptnoInfoDic = rc.get_rceptnoInfo(corp_code, '20100101', '20211130')

    rceptnoInfoDf = pd.DataFrame(rceptnoInfoDic[corp_code])
    con = rceptnoInfoDf.add_info == ''
    rcept_noLst = rceptnoInfoDf.loc[con].rcept_no.to_list()
    print(f'length of rcept_noLst : {len(rcept_noLst)}')

    rcept_no = random.choice(rcept_noLst)
    # rcept_no = '20141114000936'
    print(f'rcept_no : {rcept_no}')

    data = FundamentalValuesProvider(rcept_no).get_fundamental_value()
    print(data)
