import abc
import requests
from bs4 import BeautifulSoup
from tqdm import tqdm

import pandas as pd

import re



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
        print(f'parse_nord : {dic}')
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
        print(f'parse_nord : {dic}')
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
    
    def handle_request(self):
        detailReportParameter = self.parser.operation()
        print(f'handle_request : {detailReportParameter}')

        if detailReportParameter :
            print('now done!')
            print(f'i am not None : {detailReportParameter}')
            return detailReportParameter

        elif self.successor is not None:
            print('now successor')
            self.successor.handle_request()

        else:
            print('now fail')
            return None

if __name__ == '__main__' :

    rcept_no = '20100330000570'
    print(f'rcept_no : {rcept_no}')
    url = f'http://dart.fss.or.kr/dsaf001/main.do?rcpNo={rcept_no}'
    r = requests.get(url)
    reportHtml = r.text
    parser1 = Parser(nord = Nord2Parser(reportHtml), parser_format = r'.*연결재무제표$')
    parser2 = Parser(nord = Nord1Parser(reportHtml), parser_format = r'.*재무제표 등$')
    successor = Handler(parser2)
    detailReportParameter = Handler(parser1, successor).handle_request()
    print(detailReportParameter)