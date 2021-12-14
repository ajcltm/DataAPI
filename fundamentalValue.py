import numpy as np
import pandas as pd


class ValueIdentifier:
    columns_parser = r'^.*[0-9]*.?[기]'
    numeric_parser = r'^[0-9]*\.?[0-9]$'
    def identifyColumns(self, df):
        for col in df.columns:
            con = df.loc[:, col].str.contains(self.columns_parser, regex=True)
            result = df.loc[con].iloc[0]
            return result
    def identifyNumeric(self, sr):
        con_ = sr.str.contains(self.numeric_parser, regex=True)
        print(sr.loc[con_])
        result = sr.loc[con_].iat[0]
        return result
        

class ReportPreprocessor:

    def __init__(self, report):
        self.report = report

    def operation(self):
        self.report = self.report.apply(lambda col: col.astype(str), axis=1)
        self.report = self.report.apply(lambda col: col.str.replace(' ', ''), axis=1)
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
                    sr = ValueIdentifier().identifyColumns(df)
                    if isinstance(sr, pd.Series):
                        print(sr)
                        value = ValueIdentifier().identifyNumeric(sr)
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
                    sr = ValueIdentifier().identifyColumns(df)
                    if isinstance(sr, pd.Series):
                        value = ValueIdentifier().identifyNumeric(sr)
                        value = int(float(value))
                        print(f'liability : {value}', type(value), sep=', ')
                        return value
                else:
                    return None

class ValueSearcher:

    def __init__(self, reports, provider):
        self.reports = reports
        self.provider = provider

    def search(self):
        for report in self.reports:
            print(report)
            print('-'*50)
            report = ReportPreprocessor(report).operation()
            self.provider.set(report)
            value = self.provider.get_values()
            if value :
                return value
        return None


if __name__ == '__main__' :

    from pathlib import Path
    import pandas as pd
    import random
    import stockInfo
    import rceptnoInfo
    import report

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

    # rcept_no = '20211115001521'
    rcept_no = '20210309000744'
    # rcept_no = '20121129001089'

    print(f'rcept_no : {rcept_no}')
    print('-'*150)
    reports = report.Report().get_report(rcept_no)

    ep = EquityProvider()
    equity = ValueSearcher(reports, ep).search()
    lp = LiabilityProvider()
    liability = ValueSearcher(reports, lp).search()
    if equity and liability:
        print(f'sum : {equity+liability}', type(equity+liability), sep=', ')
