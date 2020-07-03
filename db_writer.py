from stocks_data_reader import StockDataDownloader, get_table, modify_table
import sqlite3
import pandas_datareader
import pandas as pd
import numpy as np
from datetime import datetime
from datetime import timedelta


class DBWriter:
    def __init__(self, db, stock_symbols, start_date):
        self.connection = sqlite3.connect(db)
        self.cursor = self.connection.cursor()
        self.tables = [i[0] for i in self.cursor.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()]
        if isinstance(stock_symbols, str) and '/' in stock_symbols:
            self.symbols = [symbol.rstrip('\n') for symbol in open(stock_symbols)]
        elif isinstance(stock_symbols, list):
            self.symbols = stock_symbols
        else:
            self.symbols = [*stock_symbols]
        if isinstance(start_date, str):
            self.start_date = datetime.strptime(start_date, '%Y-%m-%d')
        elif isinstance(start_date, list):
            self.start_date = start_date
        self.tables = list(set(self.symbols).difference(self.tables))

    def _create_and_populate_table(self, table_name, column_names, df):
        try:
            self.cursor.execute(
                f'CREATE TABLE {table_name} ({", ".join([" ".join((name, "varchar")) for name in column_names])})')
            df.to_sql(table_name, self.connection, if_exists='append', index=False)
            print(f'{table_name} CREATED')
        except sqlite3.OperationalError:
            df.to_sql(table_name, self.connection, if_exists='append', index=False)
            print(f'DATA INSERTED IN {table_name}')

    def write_to_table(self):
        tables_created = 0
        for table in self.tables:
            df = get_table(table, self.start_date)
            if df is not None:
                self._create_and_populate_table(table, df.columns, df)
            else:
                print('NO DATA')

#obj = DBWriter('stocks.db', '^GSPC', '2015-01-01')
#obj.write_to_table()

class DBUpdater:
    def __init__(self, db, tables=None):
        self.connection = sqlite3.connect(db)
        self.cursor = self.connection.cursor()
        if tables is not None:
            if isinstance(tables, list):
                self.tables = tables
            elif isinstance(tables, str):
                self.tables = [tables]
        else:
            self.tables = [i[0] for i in self.cursor.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall() if i[0] != 'stock_info']
            self._remaining_tables()

    def _remaining_tables(self):
        remaining_tables = []
        now = str(datetime.now().date())
        for table in self.tables:
            max_date = self._get_max_date(table)
            if max_date is not None:
                if max_date != now:
                    remaining_tables.append(table)
            else:
                continue
        self.tables = remaining_tables

    def _get_max_date(self, table):
        try:
            return self.cursor.execute(f'SELECT MAX(Date) FROM {table}').fetchone()[0][:10]
        except (sqlite3.OperationalError, TypeError):
            return None

    def _get_new_records(self, table):
        last_date = self._get_max_date(table)
        df = get_table(table, last_date)
        if df is not None and last_date is not None:
            df.dropna(inplace=True)
            return df
        return None

    def _delete_old_records(self, table, limit):
        self.cursor.execute(f'DELETE FROM {table} WHERE Date IN (SELECT Date FROM {table} LIMIT {limit})')
        self.connection.commit()

    def update_table(self):
        n_update = 0
        for i in self.tables:
            df = self._get_new_records(i)
            if df is not None:
                df.to_sql(i, self.connection, if_exists='append', index=None)
                limit = df.shape[0]
                self._delete_old_records(i, limit)
                n_update += 1
                print(f'{n_update}/{len(self.tables)} - {i}')
            else:
                print(f'NO DATA FOR {i}')

#obj = DBUpdater('stocks.db')
#obj.update_sp500()
#obj.update_db()

class AggregatedReturns:
    def __init__(self):
        self.connection = sqlite3.connect('stock_info.db')
        self.cursor = self.connection.cursor()
        self.tables = [i[0] for i in self.cursor.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()]

    def aggregate_returns(self):
        iterator = iter(self.tables)
        try:
            while True:
                for _ in range(100):
                    symbol = next(iterator)
                    try:
                        df = pd.read_sql(f'SELECT Date, Daily_Returns FROM {symbol}', connection, index_col='Date',
                                         parse_dates=['Date']).astype(float)

                        daily_max = df['Daily_Returns'].max()
                        if daily_max < 1:
                            std = df['Daily_Returns'].std()
                            mean_returns = df['Daily_Returns'].mean()
                            cursor.execute(
                                f'UPDATE stock_info SET return = {mean_returns}, std = {std} WHERE symbol = "{symbol}"')
                            print(f'VALUE {symbol} UPDATED')
                        else:
                            cursor.execute(f'UPDATE stock_info SET return = NULL, std = NULL WHERE symbol = "{symbol}"')
                            print('VALUE REMOVED')
                    except pd.io.sql.DatabaseError:
                        continue
                connection.commit()
        except StopIteration:
            connection.commit()

#obj = AggregatedReturns()
#obj.aggregate_returns()
