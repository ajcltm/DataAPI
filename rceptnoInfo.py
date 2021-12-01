import abc
from pathlib import Path
from tqdm import tqdm

import pandas as pd
import requests


class Handler(metaclass=abc.ABCMeta):

    def __init__(self,  requester, successor=None):
        self._requester = requester
        self._successor = successor
        
    @abc.abstractmethod
    def handle_request(self):
        pass


class concreteHandler(Handler):

    def handle_request(self):
        r = self._requester.operation()

        if r.json()['status'] == '000':
            return r.json()['list']
        elif self._successor is not None:
            self._successor.handle_request()
        else :
            return None



class RequesterAF(abc.ABC):
    """This is the interface which requests rceptNo information (concrete functions would be a document version of 'A'or 'F')"""
    
    @abc.abstractmethod
    def operation(self, documentVersion):
        pass


class RequesterA(RequesterAF):
    """A concrete function that requests rceptNo. pblntf_ty is 'A'"""
    documentVersion = 'A'
    url = 'https://opendart.fss.or.kr/api/list.json'
    paramsDict = None

    def __init__(self, corp_code, start, end):
        self.paramsDict = {
            'crtfc_key' : '92c176817e681dcc4ad263eb3fa5182792b0b7a3',
            'corp_code' : corp_code,
            'bgn_de' : start,
            'end_de' : end,
            'pblntf_ty': self.documentVersion,
            'last_reprt_at' : 'N',
            'page_count' : 100
        }
    
    def operation(self):
        return requests.get(self.url, self.paramsDict)


class RequesterF(RequesterAF):
    """A concrete function that requests rceptNo. pblntf_ty is 'A'"""
    documentVersion = 'F'
    url = 'https://opendart.fss.or.kr/api/list.json'
    paramsDict = None

    def __init__(self, corp_code, start, end):
        self.paramsDict = {
            'crtfc_key' : '92c176817e681dcc4ad263eb3fa5182792b0b7a3',
            'corp_code' : corp_code,
            'bgn_de' : start,
            'end_de' : end,
            'pblntf_ty': self.documentVersion,
            'last_reprt_at' : 'N',
            'page_count' : 100
        }
    
    def operation(self):
        return requests.get(self.url, self.paramsDict)


class Preprocessor(metaclass=abc.ABCMeta):
        
    @abc.abstractmethod
    def operation(self):
        pass

    
class PreprocessorRceptnoInfo(Preprocessor):

    def split_report_nm(self, df):
        df_ = df.report_nm.str.extract(r'\[?(\w*)\]?(사업보고서|반기보고서|분기보고서|감사보고서|연결감사보고서).*\((\d{4}\.\d{2})\)', expand=True).rename(columns={0:'add_info', 1:'kind', 2:'date'})
        addedDf = pd.concat([df, df_], axis=1)
        return addedDf

    def operation(self, df):
        addedDf = self.split_report_nm(df)
        dropedAddedDf = addedDf.dropna()
        return dropedAddedDf


class RceptnoInfo:

    rceptnoInfoDic = {}

    def get_rceptnoInfo(self):
        successor = concreteHandler(self.requesterF)
        result = concreteHandler(self.requesterA, successor).handle_request()
        return result

    def transfer_df(self, dic):
        keys = list(dic.keys())
        dfs=[]
        for key in keys:
            df = pd.DataFrame(dic[key])
            dfs.append(df)
        transferDf = pd.concat(dfs)
        return transferDf
    
    def transfer_dic(self, df):
        tickers = df.stock_code.unique().tolist()
        dic={}
        for ticker in tickers:
            con = df.stock_code == ticker
            df_ = df.loc[con]
            dic[ticker] = df_.to_dict('records')
        return dic
    
    def __init__(self, start, end, StockInfo, Preprocessor):
        tickerLst = StockInfo.stockInfoDic.keys()
        for ticker in tqdm(tickerLst) :
            corp_code = StockInfo.stockInfoDic[ticker]['corp_code']
            self.requesterA = RequesterA(corp_code, start, end)
            self.requesterF = RequesterF(corp_code, start, end)
            self.rceptnoInfoDic[ticker] = self.get_rceptnoInfo()
        rceptnoInfoDf = self.transfer_df(self.rceptnoInfoDic)
        preprocessedDf = Preprocessor.operation(rceptnoInfoDf)
        self.rceptnoInfoDic = self.transfer_dic(preprocessedDf)