
class ReportPreprocessor:

    def __init__(self, report):
        self.report = report

    def operation(self):
        self.report = self.report.apply(lambda col: col.str.replace(' ', ''), axis=1)

        return self.report

class EquityProvider:

    parserLst = ['^자본총계$']

    def __init__(self, report):
        self.report = report

    def get_values(self):
        for col in self.report.columns:
            for parser in self.parserLst:
                con = self.report.loc[:, col].str.contains(parser, regex=True)
                sr = self.report.loc[con]
                if not sr.empty:
                    return int(sr.iat[0,1])

class LiabilityProvider:

    parserLst = ['^부채총계$']

    def __init__(self, report):
        self.report = report

    def get_values(self):
        for col in self.report.columns:
            for parser in self.parserLst:
                con = self.report.loc[:, col].str.contains(parser, regex=True)
                sr = self.report.loc[con]
                if not sr.empty:
                    return int(sr.iat[0,1])
                    


if __name__ == '__main__' :

    from pathlib import Path
    import pandas as pd
    import random
    import stockInfo
    import rceptnoInfo
    import report

    path = Path.home().joinpath('Desktop', 'dataBackUp(211021)')
    commonStockProvider = stockInfo.commonStockProvider()
    stockinfo = stockInfo.StockInfo(path, commonStockProvider)

    ticker = '078930'
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
    print(f'rcept_no : {rcept_no}')
    print('-'*150)
    report = report.Report().get_report(rcept_no)
    print('-'*150)
    print(report[0].tail(), report[1].tail(), sep = '\n')

    report_ = ReportPreprocessor(report[0]).operation()
    report__ = ReportPreprocessor(report[1]).operation()
    print('-'*150)
    print(report_.tail(), report__.tail(), sep = '\n')

    equity = EquityProvider(report__).get_values()
    LiabilityProvider
    liability = LiabilityProvider(report__).get_values()
    print(f'equity : {equity}', type(equity), sep=', ')
    print(f'liability : {liability}', type(liability), sep=', ')
    print(f'sum : {equity+liability}', type(equity+liability), sep=', ')
