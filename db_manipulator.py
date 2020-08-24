from stock_database.data_fetcher import COLUMNS
from stock_database.miscellaneous import SQLiteKeyWords
import sqlite3
import pandas as pd
from abc import ABC


class DBConnector(ABC):
    def __init__(self):
        self.connection = sqlite3.connect('stock_info.db')
        self.cursor = self.connection.cursor()


class DBMetaData(DBConnector):
    @property
    def schema_tables(self):
        return [table[0] for table in self.cursor.execute('SELECT name FROM sqlite_master WHERE type = "table"').fetchall() if table[0] not in SQLiteKeyWords]


class DBWriter(DBConnector):
    def __init__(self, data: pd.DataFrame, symbol):
        DBConnector.__init__(self)
        self.data = data
        self.table_name = symbol

    def write_to_table(self):
        self.data.to_sql(self.table_name, self.connection, if_exists='replace', index=True)


class TableCreator(DBConnector):
    def __init__(self, symbol):
        DBConnector.__init__(self)
        self.table_name = symbol

    def create_table(self):
        self.cursor.execute(f'CREATE TABLE {self.table_name} ({", ".join([" ".join((name, "varchar")) for name in COLUMNS])})')


class DataAggregator(DBConnector):
    def __init__(self):
        DBConnector.__init__(self)
        self.tables = [i[0] for i in self.cursor.execute("SELECT symbol FROM stock_info").fetchall()]
        self.mapping_table = pd.read_sql('SELECT * FROM stock_info', self.connection, index_col='symbol')

    def aggregate_data(self):
        for table in self.tables:
            try:
                df = pd.read_sql(f'SELECT Date, Daily_Returns FROM {table}', self.connection).astype({'Daily_Returns': float})
                daily_max_returns = df['Daily_Returns'].max()
                if daily_max_returns < 1:
                    std = df['Daily_Returns'].std()
                    daily_mean_returns = df['Daily_Returns'].mean()

                    self.mapping_table['mean_return'][table] = daily_mean_returns
                    self.mapping_table['standard_deviation'][table] = std
                    print('UPDATED')
            except pd.io.sql.DatabaseError:
                print('TABLE NOT FOUND')
        self.mapping_table.reset_index(inplace=True)
        self.mapping_table.to_sql('stock_info', self.connection, if_exists='replace', index=False)
        return self.mapping_table
