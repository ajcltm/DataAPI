import abc
import requests
from bs4 import BeautifulSoup
from tqdm import tqdm
import pandas as pd
import re

import preprocess



class Nord2Parser:
    parameterKeyLst = ['text', 'id', 'rcpNo', 'dcmNo', 
                'eleId', 'offset', 'length', 'dtd', 'tocNo']


    def get_parameterValue(self, key):
        return re.findall(r'node2\[\'' + key + '\'\]\s*=\s*(".*?")', self.html)

    def get_parameterDic(self):
        parameterDic = {}
        for key in self.parameterKeyLst :
            parameterDic[key] = self.get_parameterValue(key)
        return parameterDic
    
    def parse_nord(self, parser):
        stripedDf = pd.DataFrame(self.parameterDic).applymap(lambda x:x.strip('"'))
        con = stripedDf.text.str.contains(parser)
        dic = stripedDf.loc[con].to_dict('records')
        return dic

    def __init__(self, reportHtml):
        self.html = reportHtml
        self.parameterDic = self.get_parameterDic()
        

        
class Nord1Parser:
    parameterKeyLst = ['text', 'id', 'rcpNo', 'dcmNo', 
                'eleId', 'offset', 'length', 'dtd', 'tocNo']


    def get_parameterValue(self, key):
        return re.findall(r'node1\[\'' + key + '\'\]\s*=\s*(".*?")', self.html)

    def get_parameterDic(self):
        parameterDic = {}
        for key in self.parameterKeyLst :
            parameterDic[key] = self.get_parameterValue(key)
        return parameterDic
    
    def parse_nord(self, parser):
        stripedDf = pd.DataFrame(self.parameterDic).applymap(lambda x:x.strip('"'))
        con = stripedDf.text.str.contains(parser)
        dic = stripedDf.loc[con].to_dict('records')
        return dic

    def __init__(self, reportHtml):
        self.html = reportHtml
        self.parameterDic = self.get_parameterDic()

class ParseNordABC(metaclass=abc.ABCMeta):

    def __init__(self, nord, parser_format) :
        self.nord = nord
        self.parser_format = parser_format
    @abc.abstractclassmethod
    def operation(self):
        pass


class Parser(ParseNordABC):
    def operation(self):
        return self.nord.parse_nord(self.parser_format)

class Handler:

    def __init__(self, parser, successor=None):
        self.parser = parser
        self.successor = successor

    def inner_handler(self, detailReportParameter):
        print('inner_handler')
        url = f'http://dart.fss.or.kr/report/viewer.do?'
        r = requests.get(url, params=detailReportParameter[0])
        html = r.text
        soup = BeautifulSoup(html, 'html.parser')
        table = soup.find_all('table')
        if table :
            print('got table')
            return soup
        elif self.successor is not None:
            print('successor')
            return self.successor.handle_request()
        else:
            print('None')
            return None
        
    def handle_request(self):
        detailReportParameter = self.parser.operation()
        print('handler')
        if detailReportParameter :
            print('got params')
            print(detailReportParameter)
            return self.inner_handler(detailReportParameter)
        elif self.successor is not None:
            print('successor')
            return self.successor.handle_request()
        else:
            print('None')
            return None

class HtmlReport:

    def get_report(self, rcept_no):
        url = f'http://dart.fss.or.kr/dsaf001/main.do?rcpNo={rcept_no}'
        r = requests.get(url)
        reportHtml = r.text
        parser1 = Parser(nord = Nord2Parser(reportHtml), parser_format = r'.*연결재무제표$')
        parser2 = Parser(nord = Nord1Parser(reportHtml), parser_format = r'.*재무제표 등$')
        # parser3 = Parser(nord = Nord2Parser(reportHtml), parser_format = r'^[^가-힣]*재무제표$')
        # successor2 = Handler(parser3)
        # successor1 = Handler(parser2, successor2)
        successor = Handler(parser2)
        # html = Handler(parser1, successor1).handle_request()
        html = Handler(parser1, successor).handle_request()
        return html

class BalanceSheet:
    # consolidatedParsers = [r'연\s*결\s*재\s*무\s*상\s*태\s*표\s*']
    # consolidatedParsers = [r'연\s*결\s*손\s*익\s*계\s*산\s*서\s*']
    # consolidatedParsers = [r'연\s*결\s*포\s*괄\s*손\s*익\s*계\s*산\s*서\s*']
    consolidatedParsers = [r'연\s*결\s*현\s*금\s*흐\s*름\s*표']
    notConsolidated = []

    def __init__(self, html):
        self.soup = html
    
    def get_preprocessed_reportLst(self):
        reportLst = []
        for report in self.reports:
            report = preprocess.ReportPreprocessor(report).operation()
            reportLst.append(report)
        return reportLst
        
    def export_first_numeric_report(self):
        for report in self.reports:
            for col in report.columns:
                test_report = report.loc[:, col]
                print(f'value : {test_report.values}')
                dtype = test_report.values.dtype
                print(f'dtype : {dtype}')
                bool1 = dtype == 'float'
                bool2 = dtype == 'int64'
                print(f'float_bool : {bool1}')
                print(f'int_bool : {bool2}')
                
                if dtype == 'float':
                    return report
                if dtype =='int64':
                    return report

    def get_consolidated(self):
        for parser in self.consolidatedParsers:
            string = self.soup.find_all(string=re.compile(parser))
            print(bool(string))
            if string:
                print(string)
                table_soup = self.soup.find_all(string=re.compile(parser))[0].find_all_next('table')
                self.reports = pd.read_html(str(table_soup))
                self.reports = self.get_preprocessed_reportLst()
                report = self.export_first_numeric_report()
            else:
                return None
            return report
    

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
    # rcept_no = '20160330001826'
    print(f'rcept_no : {rcept_no}')
    print('-'*150)
    html = HtmlReport().get_report(rcept_no)
    print('-'*150)
    if not html == None: 
        report = BalanceSheet(html).get_consolidated()
        print(report)