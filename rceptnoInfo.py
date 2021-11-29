import abc
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


class RceptnoInfo(concreteHandler, RequesterA, RequesterF):

    def __init__(self, corp_code, start, end) :
        self.requesterA = RequesterA(corp_code, start, end)
        self.requesterF = RequesterF(corp_code, start, end)
        
        successor = concreteHandler(self.requesterF)
        result = concreteHandler(self.requesterA, successor)

        return result

    
