import sqlite3
from stock_database.data_fetcher import DataModifier, DataFetcher
from stock_database.db_manipulator import TableCreator, DBWriter
from typing import List
import pandas as pd


connection = sqlite3.connect('stock_info.db')
cursor = connection.cursor()

#cursor.execute('CREATE TABLE update_from (symbol varchar, date_from varchar)')


symbols = ['AAPL', 'MSFT', 'AMZN', 'FB', 'GOOG']


def write_end_dates(dates: dict) -> None:
    df = pd.read_sql('SELECT * FROM update_from', connection, index_col='symbol')
    for key in dates:
        df.loc[key] = dates[key]
    df.reset_index(inplace=True)
    df.to_sql('update_from', connection, if_exists='replace', index=False)


def load_database(symbols: (List[str], str)):
    for item in symbols:
        data = DataFetcher(item, '2015-07-01').fetch()
        data = DataModifier(data).add_daily_returns()
        try:
            TableCreator(item).create_table()
        except sqlite3.OperationalError:
            pass
        DBWriter(data, item).write_to_table()
    write_end_dates(DataFetcher.end_dates_storage)


def update_database(symbols: (List[str], str)):
    pass
