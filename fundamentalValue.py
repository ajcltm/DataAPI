from dataclasses import dataclass, field
from typing import List, Dict

import numpy as np
import pandas as pd
from datetime import datetime
import report


class ValueIdentifier:
    columns_parser = r'^.*[0-9]*.?[기]'
    numeric_parser = r'^[0-9]*\.?[0-9]$'
    def identifyColumns(self, df):
        df = df.apply(lambda col: col.astype(str), axis=1)
        for col in df.columns:
            con = df.loc[:, col].str.contains(self.columns_parser, regex=True)
            print(f'identified columns : {df.loc[con]}')
            result_df = df.loc[con]
            result_df = result_df.drop(result_df.columns[0], axis=1)
            if not result_df.empty :
                return result_df
        return None

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
                if not test_df[~con].empty:
                    return test_df[~con].iat[0]
            if dtype =='int64':
                con = test_df == 0
                if not test_df[~con].empty:
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

class ValueProvider:

    def __init__(self, parserLst):
        self.__report = None
        self.parserLst = parserLst

    def set(self, report):
        self.__report = report

    def get_values(self):
        for col in self.__report.columns:
            for parser in self.parserLst:
                print(f'get_Values : {parser}')
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
        return None

# class LiabilityProvider:

#     parserLst = ['^부채총계$']

#     def __init__(self):
#         self.__report = None

#     def set(self, report):
#         self.__report = report

#     def get_values(self):
#         for col in self.__report.columns:
#             for parser in self.parserLst:
#                 con = self.__report.loc[:, col].str.contains(parser, regex=True)
#                 sr = self.__report.loc[con]
#                 if not sr.empty:
#                     df = sr.squeeze().reset_index()
#                     print(f'series to df including value : {df}')
#                     df_ = ValueIdentifier().identifyColumns(df)
#                     if isinstance(df_, pd.DataFrame):
#                         value = ValueIdentifier().identifyNumeric(df_)
#                         value = int(float(value))
#                         print(f'liability : {value}', type(value), sep=', ')
#                         return value
#                 else:
#                     return None

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
    consolidatedNetIncome:int
    consolidatedGrossProfit:int
    consolidatedOperatingProfit:int
    consolidatedComprehensiveNetIncome:int
    consolidatedComprehensiveGrossProfit:int
    consolidatedComprehensiveOperatingProfit:int
    consolidatedOperatingActivities:int

    equity:int
    liability:int
    netIncome:int
    grossProfit:int
    operatingProfit:int
    comprehensiveNetIncome:int
    comprehensiveGrossProfit:int
    comprehensiveOperatingProfit:int
    operatingActivities:int


class FundamentalValuesProvider:

    def __init__(self, rcept_no):
        self.rcept_no = rcept_no

    def multiply_unit(self, value, unit):
        if not value == None :
            return value*unit
        return None

    def get_fundamental_value(self):
        set_of_reports = report.ReportSetter(self.rcept_no).get_set_of_report()

        parserLst = [r'^자본총계$',r'^[^가-힣]*자\s*본\s*총\s*계\s*', r'^[가나다라마바사아]*[^가-힣]*자\s*본\s*총\s*계\s*']
        ep = ValueProvider(parserLst)
        parserLst = [r'^부채총계$',r'^[^가-힣]*부\s*채\s*총\s*계\s*', r'^[가나다라마바사아]*[^가-힣]*부\s*채\s*총\s*계\s*']
        lp = ValueProvider(parserLst)
        parserLst = [r'^당\s*기\s*순\s*이\s*익\s*', r'^[^가-힣]*당\s*기\s*순\s*이\s*익\s*', r'^[가나다라마바사아]*[^가-힣]*당\s*기\s*순\s*이\s*익\s*',
                    r'^당\s*기\s*연\s*결\s*순\s*이\s*익\s*', r'^[^가-힣]*당\s*기\s*연\s*결\s*순\s*이\s*익\s*', r'^[가나다라마바사아]*[^가-힣]*당\s*기\s*연\s*결\s*순\s*이\s*익\s*',
                    r'^반\s*기\s*순\s*이\s*익\s*', r'^[^가-힣]*반\s*기\s*순\s*이\s*익\s*', r'^[가나다라마바사아]*[^가-힣]*반\s*기\s*순\s*이\s*익\s*',
                    r'^반\s*기\s*연\s*결\s*순\s*이\s*익\s*', r'^[^가-힣]*반\s*기\s*연\s*결\s*순\s*이\s*익\s*', r'^[가나다라마바사아]*[^가-힣]*반\s*기\s*연\s*결\s*순\s*이\s*익\s*',
                    r'^분\s*기\s*순\s*이\s*익\s*', r'^[^가-힣]*분\s*기\s*순\s*이\s*익\s*', r'^[가나다라마바사아]*[^가-힣]*분\s*기\s*순\s*이\s*익\s*'
                    r'^분\s*기\s*연\s*결\s*순\s*이\s*익\s*', r'^[^가-힣]*분\s*기\s*연\s*결\s*순\s*이\s*익\s*', r'^[가나다라마바사아]*[^가-힣]*분\s*기\s*연\s*결\s*순\s*이\s*익\s*',
                    r'^당\s분\s*기\s*[연\s*결\s]*순\s*이\s*익\s*', r'^[^가-힣]*당\s*분\s*기\s*[연\s*결\s]*순\s*이\s*익\s*', r'^[가나다라마바사아]*[^가-힣]*당\s*분\s*기\s*[연\s*결\s]**순\s*이\s*익\s*']
        ni = ValueProvider(parserLst)
        parserLst = [r'매\s*출\s*총\s*이\s*익\s*',r'[^가-힣]*매\s*출\s*총\s*이\s*익\s*', r'^[가나다라마바사아]*[^가-힣]*매\s*출\s*총\s*이\s*익\s*']
        gp = ValueProvider(parserLst)
        parserLst = [r'영\s*업\s*이\s*익\s*',r'^[^가-힣]*영\s*업\s*이\s*익\s*', r'^[가나다라마바사아]*[^가-힣]*영\s*업\s*이\s*익\s*']
        op = ValueProvider(parserLst)
        parserLst = [r'영\s*업\s*활\s*동\s*현\s*금\s*흐\s*름', r'^[^가-힣]*영\s*업\s*활\s*동\s*현\s*금\s*흐\s*름\s*', r'^[가나다라마바사아]*[^가-힣]*영\s*업\s*활\s*동\s*현\s*금\s*흐\s*름\s*',
                    r'영.*업.*활.*동.*으.*로.*부.*터.*의.*현.*금.*흐.*름.*']
        oa = ValueProvider(parserLst)
        

        consolidated_balance_sheet = set_of_reports['consolidated_balance_sheet'][0]
        unit = set_of_reports['consolidated_balance_sheet'][1]
        consolidatedEquity = ValueSearcher(consolidated_balance_sheet, ep).search()
        consolidatedEquity = self.multiply_unit(consolidatedEquity, unit)
        consolidatedliability = ValueSearcher(consolidated_balance_sheet, lp).search()
        consolidatedliability = self.multiply_unit(consolidatedliability, unit)

        consolidated_income_statement = set_of_reports['consolidated_income_statement'][0]
        unit = set_of_reports['consolidated_income_statement'][1]
        consolidatedNetIncome = ValueSearcher(consolidated_income_statement, ni).search()
        consolidatedNetIncome = self.multiply_unit(consolidatedNetIncome, unit)
        consolidatedGrossProfit = ValueSearcher(consolidated_income_statement, gp).search()
        consolidatedGrossProfit = self.multiply_unit(consolidatedGrossProfit, unit)
        consolidatedOperatingProfit = ValueSearcher(consolidated_income_statement, op).search()
        consolidatedOperatingProfit = self.multiply_unit(consolidatedOperatingProfit, unit)

        Consolidated_comprehensive_income_statement = set_of_reports['Consolidated_comprehensive_income_statement'][0]
        unit = set_of_reports['Consolidated_comprehensive_income_statement'][1]
        consolidatedComprehensiveNetIncome = ValueSearcher(Consolidated_comprehensive_income_statement, ni).search()
        consolidatedComprehensiveNetIncome = self.multiply_unit(consolidatedComprehensiveNetIncome, unit)
        consolidatedComprehensiveGrossProfit = ValueSearcher(Consolidated_comprehensive_income_statement, gp).search()
        consolidatedComprehensiveGrossProfit = self.multiply_unit(consolidatedComprehensiveGrossProfit, unit)
        consolidatedComprehensiveOperatingProfit = ValueSearcher(Consolidated_comprehensive_income_statement, op).search()
        consolidatedComprehensiveOperatingProfit = self.multiply_unit(consolidatedComprehensiveOperatingProfit, unit)

        Consolidated_cash_flow_statement = set_of_reports['Consolidated_cash_flow_statement'][0]
        unit = set_of_reports['Consolidated_cash_flow_statement'][1]
        consolidatedOperatingActivities = ValueSearcher(Consolidated_cash_flow_statement, oa).search()
        consolidatedOperatingActivities = self.multiply_unit(consolidatedOperatingActivities, unit)

        balance_sheet = set_of_reports['balance_sheet'][0]
        unit = set_of_reports['balance_sheet'][1]
        equity = ValueSearcher(balance_sheet, ep).search()
        equity = self.multiply_unit(equity, unit)
        liability = ValueSearcher(balance_sheet, lp).search()
        liability = self.multiply_unit(liability, unit)

        income_statement = set_of_reports['income_statement'][0]
        unit = set_of_reports['income_statement'][1]
        NetIncome = ValueSearcher(income_statement, ni).search()
        NetIncome = self.multiply_unit(NetIncome, unit)
        GrossProfit = ValueSearcher(income_statement, gp).search()
        GrossProfit = self.multiply_unit(GrossProfit, unit)
        OperatingProfit = ValueSearcher(income_statement, op).search()
        OperatingProfit = self.multiply_unit(OperatingProfit, unit)

        comprehensive_income_statement = set_of_reports['comprehensive_income_statement'][0]
        unit = set_of_reports['comprehensive_income_statement'][1]
        ComprehensiveNetIncome = ValueSearcher(comprehensive_income_statement, ni).search()
        ComprehensiveNetIncome = self.multiply_unit(ComprehensiveNetIncome, unit)
        ComprehensiveGrossProfit = ValueSearcher(comprehensive_income_statement, gp).search()
        ComprehensiveGrossProfit = self.multiply_unit(ComprehensiveGrossProfit, unit)
        ComprehensiveOperatingProfit = ValueSearcher(comprehensive_income_statement, op).search()
        ComprehensiveOperatingProfit = self.multiply_unit(ComprehensiveOperatingProfit, unit)

        cash_flow_statement = set_of_reports['cash_flow_statement'][0]
        unit = set_of_reports['cash_flow_statement'][1]
        operatingActivities = ValueSearcher(cash_flow_statement, oa).search()
        operatingActivities = self.multiply_unit(operatingActivities, unit)

        data = FundamentalValues(
            consolidatedEquity, consolidatedliability, 
            consolidatedNetIncome, consolidatedGrossProfit, consolidatedOperatingProfit,
            consolidatedComprehensiveNetIncome, consolidatedComprehensiveGrossProfit, consolidatedComprehensiveOperatingProfit,
            consolidatedOperatingActivities,
            equity, liability,
            NetIncome, GrossProfit, OperatingProfit,
            ComprehensiveNetIncome, ComprehensiveGrossProfit, ComprehensiveOperatingProfit,
            operatingActivities
            )

        return data
    
@dataclass
class FundamentalValuesInfo:
    corp_code: str
    rcept_no: str
    date: str
    data: FundamentalValues


@dataclass
class FundamentalValuesInfoProvider:
    corp_code : str
    s_date : str
    e_date : str
    values: dict = field(default_factory=dict)

    def __post_init__(self):

        preprocessor = rceptnoInfo.PreprocessorRceptnoInfo()
        rc = rceptnoInfo.RceptnoInfo(preprocessor)
        rceptnoInfoDic = rc.get_rceptnoInfo(self.corp_code, self.s_date, self.e_date)

        rceptnoInfoDf = pd.DataFrame(rceptnoInfoDic[self.corp_code])
        con = rceptnoInfoDf.add_info == ''
        rceptnoInfoDfcon = rceptnoInfoDf.loc[con]

        # for k in range(0, len(rceptnoInfoDfcon)):
        for k in range(0, 5):
            rcept_no = rceptnoInfoDfcon.iloc[k].rcept_no
            print(f'rcept_no : {rcept_no}')

            date = rceptnoInfoDfcon.iloc[k].date

            data = FundamentalValuesProvider(rcept_no).get_fundamental_value()
            print(f'rcept_no : {rcept_no}')
            print(data)

            self.values[date] = FundamentalValuesInfo(corp_code, rcept_no, date, data)



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

    result = FundamentalValuesInfoProvider(corp_code, '20100101', '20211130')

    print(f'result : {result}')
