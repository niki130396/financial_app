import sqlite3
from stock_database.data_fetcher import DataModifier, DataFetcher
from stock_database.db_manipulator import TableCreator, DBWriter
from typing import List
import pandas as pd


connection = sqlite3.connect('stock_info.db')
cursor = connection.cursor()


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


def update_database(symbols: List[str]):
    update_dates = pd.read_sql('SELECT * FROM update_from', connection).set_index('symbol')

    for symbol in symbols:
        start_date = update_dates.loc[symbol, 'date_from']
        current_df = pd.read_sql(f'SELECT * FROM {symbol}', connection)
        obj = DataFetcher(symbol, start_date)
        new_data = obj.fetch()
        new_data = DataModifier(new_data).add_daily_returns()
        current_df.drop(current_df.index[:len(new_data.index)], inplace=True)

        current_df = current_df.append(new_data, ignore_index=True)
        DBWriter(current_df, symbol).write_to_table()
    write_end_dates(DataFetcher.end_dates_storage)
