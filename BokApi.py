import pandas as pd
import requests
from pathlib import Path


class Bokapi:

    def __init__(self, key):
        self.key = key
        self.start = '1'
        self.end = '10000'
        self.dataPath = Path.cwd() / 'DataBokAPI'
        self.fileName = 'bokApiInfo.parquet'
        self.queryInfoDf = pd.read_parquet(self.dataPath / self.fileName)

    def getUrl(self):
        edge_url = '/'.join(self.codeList[1:]) + '/'
        url = 'http://ecos.bok.or.kr/api/StatisticSearch/' + self.key + '/json/kr/' + self.start + '/' + self.end + '/' + \
              self.codeList[0] + '/' \
              + self.periodUnit + '/' + self.startDate + '/' + self.endDate + '/' + edge_url
        print(url)
        return url

    def setIndex(self, resultDf):
        dateSeries = resultDf.reset_index().loc[:, 'index'].apply(lambda x: x[:-1] + "Q" + x[-1:])
        dateSeries = dateSeries.apply(lambda x: pd.Period(x, freq='M'))
        dateSeries = dateSeries.apply(lambda x: str(x)).tolist()
        dateSeries = pd.Series(dateSeries)
        dateSeries = pd.to_datetime(dateSeries, format='%Y-%m')
        resultDf = resultDf.set_index(dateSeries)

        return resultDf

    def getData(self, indicatorIndex, startDate, endDate, periodUnit='default'):

        # set self.codeList and resultColName
        if isinstance(indicatorIndex, int):
            series = self.queryInfoDf.iloc[indicatorIndex]
            self.codeList = series[['code1', 'code2', 'code3', 'code4']].dropna().tolist()
            resultColName = [series['통계지표'] + ' ' + series['단위'].join('()')]

        elif isinstance(indicatorIndex, list):
            self.codeList = indicatorIndex
            resultColName = [self.codeList[0]]

        # set date period for query
        self.startDate = startDate
        self.endDate = endDate

        # set period_unit
        self.periodUnit = periodUnit
        if periodUnit == 'default':
            self.periodUnit = self.queryInfoDf.iloc[indicatorIndex]['frequency1']

        # get url for query
        url = self.getUrl()

        # request data
        re = requests.get(url)
        j = re.json()
        data = j['StatisticSearch']['row']

        valueList = []
        timeList = []

        for item in data:
            value = item['DATA_VALUE']
            time = item['TIME']
            valueList.append(value)
            timeList.append(time)

        resultDf = pd.DataFrame(valueList, index=timeList, columns=resultColName)

        resultDf = self.setIndex(resultDf)

        return resultDf