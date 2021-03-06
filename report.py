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

class HtmlProvider:

    def __init__(self, parser_format_1, parser_format_2):
        self.parser_format_1 = parser_format_1
        self.parser_format_2 = parser_format_2

    def get_html(self, rcept_no):
        url = f'http://dart.fss.or.kr/dsaf001/main.do?rcpNo={rcept_no}'
        r = requests.get(url)
        reportHtml = r.text
        parser1 = Parser(nord = Nord2Parser(reportHtml), parser_format = self.parser_format_1) 
        parser2 = Parser(nord = Nord1Parser(reportHtml), parser_format = self.parser_format_2) 
        # parser3 = Parser(nord = Nord2Parser(reportHtml), parser_format = r'^[^???-???]*????????????$')
        # successor2 = Handler(parser3)
        # successor1 = Handler(parser2, successor2)
        successor = Handler(parser2)
        # html = Handler(parser1, successor1).handle_request()
        html = Handler(parser1, successor).handle_request()
        return html

class HtmlSetter:

    def __init__(self, rcept_no):
        self.rcept_no = rcept_no
    
    def get_set_of_htmls(self):
        htmlForConsolidated = HtmlProvider(r'.*??????????????????$', r'.*???????????? ???$').get_html(self.rcept_no)
        htmelForNonConsolidated = HtmlProvider(r'^[^???-???]*????????????$', r'.*???????????? ???$').get_html(self.rcept_no)
        return {'htmlForConsolidated':htmlForConsolidated, 'htmlForNonConsolidated':htmelForNonConsolidated}

class ReportSearcher:
    # consolidatedParsers = [r'???\s*???\s*???\s*???\s*???\s*???\s*???\s*']
    # consolidatedParsers = [r'???\s*???\s*???\s*???\s*???\s*???\s*???\s*']
    # consolidatedParsers = [r'???\s*???\s*???\s*???\s*???\s*???\s*???\s*???\s*???\s*']
    # consolidatedParsers = [r'???\s*???\s*???\s*???\s*???\s*???\s*???']

    def __init__(self, parser_format_lst):
        self.parser_format_lst = parser_format_lst
    
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
                print(f'value : {test_report.values[:5]}')
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

    def get_unit(self, soup, parser):
        unit_dic = {'???': 1, '??????':1000, '?????????': 1000000}

        unit_string = soup.find_all(string=re.compile(parser))[0].find_all_next(string=re.compile('??????'))[0]
        unit_string = unit_string.replace(' ', '').replace(')', '').split(':')[-1]

        unit = unit_dic.get(unit_string)
        return unit

    def get_table(self, html):
        soup = html
        for parser in self.parser_format_lst:
            print(f'parser : {parser}')
            string = soup.find_all(string=re.compile(parser))
            print(bool(string))
            if string:
                print(string)
                table_soup = soup.find_all(string=re.compile(parser))[0].find_all_next('table')
                unit = self.get_unit(soup, parser)
                print(f'unit : {unit}')
                self.reports = pd.read_html(str(table_soup))
                self.reports = self.get_preprocessed_reportLst()
                report = self.export_first_numeric_report()
                return (report, unit)
        return (None, None)
    
class ReportProvider:

    def __init__(self, html, ReportSearcher):
        self.html = html
        self.ReportSearcher = ReportSearcher
    
    def get_report(self):
        if not self.html == None:
            report = self.ReportSearcher.get_table(self.html)
            print('-'*150)
            print(report)
            return report
        else :
            return (None, None)

class ReportSetter:

    def __init__(self, rcept_no):
        self.rcept_no = rcept_no

    def get_set_of_report(self):
        set_of_report = {}

        set_of_htmls = HtmlSetter(self.rcept_no).get_set_of_htmls()
        
        htmlForConsolidated = set_of_htmls['htmlForConsolidated']
        
        parser_format_lst = [r'???\s*???\s*???\s*???\s*???\s*???\s*???\s*', r'???\s*???\s*???\s*???\s*???\s*???\s*???\s*']
        rs = ReportSearcher(parser_format_lst)
        consolidated_balance_sheet = ReportProvider(htmlForConsolidated, rs).get_report()
        set_of_report['consolidated_balance_sheet'] = consolidated_balance_sheet

        parser_format_lst = [r'???\s*???\s*???\s*???\s*???\s*???\s*???\s*']
        rs = ReportSearcher(parser_format_lst)
        consolidated_income_statement = ReportProvider(htmlForConsolidated, rs).get_report()
        set_of_report['consolidated_income_statement'] = consolidated_income_statement

        parser_format_lst = [r'???\s*???\s???\s*???\s*???\s*???\s*???\s*???\s*???\s*', r'.*???\s*???\s*???\s*???\s*???\s*']
        rs = ReportSearcher(parser_format_lst)
        Consolidated_comprehensive_income_statement = ReportProvider(htmlForConsolidated, rs).get_report()
        set_of_report['Consolidated_comprehensive_income_statement'] = Consolidated_comprehensive_income_statement

        parser_format_lst = [r'???\s*???\s*???\s*???\s*???\s*???\s*???\s*']
        rs = ReportSearcher(parser_format_lst)
        Consolidated_cash_flow_statement = ReportProvider(htmlForConsolidated, rs).get_report()
        set_of_report['Consolidated_cash_flow_statement'] = Consolidated_cash_flow_statement


        htmlForNonConsolidated = set_of_htmls['htmlForNonConsolidated']
        parser_format_lst = [r'^[^???-???]*???\s*???\s*???\s*???\s*???\s*', r'^[????????????????????????]*[^???-???]*???\s*???\s*???\s*???\s*???\s*', r'^[^???-???]*???\s*???\s*???\s*???\s*???\s*']
        rs = ReportSearcher(parser_format_lst)
        balance_sheet = ReportProvider(htmlForNonConsolidated, rs).get_report()
        set_of_report['balance_sheet'] = balance_sheet

        parser_format_lst = [r'^[^???-???]*???\s*???\s*???\s*???\s*???\s*', r'^[????????????????????????]*[^???-???]*???\s*???\s*???\s*???\s*???\s*']
        rs = ReportSearcher(parser_format_lst)
        income_statement = ReportProvider(htmlForNonConsolidated, rs).get_report()
        set_of_report['income_statement'] = income_statement

        parser_format_lst = [r'^[^???-???]*???\s*???\s*???\s*???\s*???\s*???\s*???\s*', r'^[????????????????????????]*[^???-???]*???\s*???\s*???\s*???\s*???\s*???\s*???\s*']
        rs = ReportSearcher(parser_format_lst)
        comprehensive_income_statement = ReportProvider(htmlForNonConsolidated, rs).get_report()
        set_of_report['comprehensive_income_statement'] = comprehensive_income_statement

        parser_format_lst = [r'^[^???-???]*???\s*???\s*???\s*???\s*???\s*', r'^[????????????????????????]*[^???-???]*???\s*???\s*???\s*???\s*???\s*']
        rs = ReportSearcher(parser_format_lst)
        cash_flow_statement = ReportProvider(htmlForNonConsolidated, rs).get_report()
        set_of_report['cash_flow_statement'] = cash_flow_statement

        return set_of_report



if __name__ == '__main__' :

    from pathlib import Path
    import random
    import stockInfo
    import rceptnoInfo

    # path = Path.home().joinpath('Desktop', 'dataBackUp(211021)')

    # stockList = pd.read_parquet(path/'stockListDB.parquet')
    # tickers = stockList.ticker.unique().tolist()
    # ticker = random.choice(tickers)

    # commonStockProvider = stockInfo.commonStockProvider()
    # stockinfo = stockInfo.StockInfo(path, commonStockProvider)
    # stockInfoDic = stockinfo.get_stockInfo(ticker)
    # corp_code = stockInfoDic[ticker]['corp_code']
    # print('='*150)
    # print(f'target : {stockInfoDic[ticker]}')

    # preprocessor = rceptnoInfo.PreprocessorRceptnoInfo()
    # rc = rceptnoInfo.RceptnoInfo(preprocessor)
    # rceptnoInfoDic = rc.get_rceptnoInfo(corp_code, '20100101', '20211130')

    # rceptnoInfoDf = pd.DataFrame(rceptnoInfoDic[corp_code])
    # con = rceptnoInfoDf.add_info == ''
    # rcept_noLst = rceptnoInfoDf.loc[con].rcept_no.to_list()
    # print(f'length of rcept_noLst : {len(rcept_noLst)}')

    # rcept_no = random.choice(rcept_noLst)
    rcept_no = '20180402004606'
    print(f'rcept_no : {rcept_no}')

    # print('-'*150)
    # html = HtmlProvider(r'.*??????????????????$', r'.*???????????? ???$').get_html(rcept_no)
    # parser_format_lst = [r'???\s*???\s*???\s*???\s*???\s*???\s*???\s*', r'???\s*???\s*???\s*???\s*???\s*???\s*???\s*']
    # rs = ReportSearcher(parser_format_lst)
    # report = ReportProvider(html, rs).get_report()

    set_of_reports = ReportSetter(rcept_no).get_set_of_report()
    print(set_of_reports)