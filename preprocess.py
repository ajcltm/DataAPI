import pandas as pd

class ReportPreprocessor:

    def __init__(self, report):
        self.report = report

    def operation(self):
        report = self.report.fillna(0)  # replace 'None' with the number of zero.
        report = report.apply(lambda col: col.astype(str), axis=1)
        report = report.apply(lambda col: col.str.replace('-', ''), axis=1)
        report = report.apply(lambda col: col.str.replace(' ', ''), axis=1)
        report = report.apply(lambda col: col.str.replace(',', ''), axis=1)
        report = report.apply(lambda col: col.str.replace('(', '-'), axis=1)
        report = report.apply(lambda col: col.str.replace(')', ''), axis=1)
        report = report.apply(lambda x: pd.to_numeric(x, errors='ignore'))
        return report
