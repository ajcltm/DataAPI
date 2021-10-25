import sqlite3
import pandas as pd


class QueryStockData() :

    def __init__(self, path) :
        self.mainPath = path

    def queryStockPriceDB(self, by) :

        if by == 'parquet' :
            df = pd.read_parquet(f'{self.mainPath}/stockPriceDB.parquet')
        
        elif by == 'sql' :
            conn = sqlite3.connect(f'{self.mainPath}/stockPriceDB.db', isolation_level=None)
            df = pd.read_sql_query('SELECT * FROM stockPriceDB', conn)
            df['date'] = pd.to_datetime(df['date'], format='%Y-%m-%d') # str(format 2021-11-27) -> datetime
            conn.close()

        return df

    def queryStockListDB(self, by) :

        if by == 'parquet' :
            df = pd.read_parquet(f'{self.mainPath}/stockListDB.parquet')
        
        elif by == 'sql' :
            conn = sqlite3.connect(f'{self.mainPath}/stockListDB.db', isolation_level=None)
            df = pd.read_sql_query('SELECT * FROM StockListDB', conn)
            df['firstDay'] = pd.to_datetime(df['firstDay'], format='%Y-%m-%d') # str(format 2021-11-27) -> datetime
            df['endDay'] = pd.to_datetime(df['endDay'], format='%Y-%m-%d') # str(format 2021-11-27) -> datetime
            conn.close()

        return df


    def queryStockNumberOfSharesDB(self, by) :

        if by == 'parquet' :
            df = pd.read_parquet(f'{self.mainPath}/stockNumberOfSharesDB.parquet')
        
        elif by == 'sql' :
            conn = sqlite3.connect(f'{self.mainPath}/stockNumberOfSharesDB.db', isolation_level=None)
            df = pd.read_sql_query('SELECT * FROM stockNumberOfSharesDB', conn)
            df['date'] = pd.to_datetime(df['date'], format='%Y-%m-%d') # str(format 2021-11-27) -> datetime
            conn.close()

        return df

