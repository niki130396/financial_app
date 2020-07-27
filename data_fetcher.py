from datetime import datetime
from pandas_datareader import DataReader
from pandas_datareader._utils import RemoteDataError
import pandas as pd

COLUMNS = ['Date', 'High', 'Low', 'Open', 'Close', 'Volume', 'Adj_Close', 'Daily_Returns']


class DataFetcher:
    end_dates_storage = {}

    def __init__(self, symbol: str, start_date: (str, datetime)):
        self.symbol = symbol
        self.start_date = start_date

    def fetch(self) -> (pd.DataFrame, None):
        try:
            df = DataReader(self.symbol, 'yahoo', self.start_date)
            DataFetcher.end_dates_storage[self.symbol] = str(df.index[-1]).split()[0]
            return df
        except (RemoteDataError, KeyError):
            return None


class DataModifier:
    def __init__(self, data: pd.DataFrame):
        self.data = data

    def add_daily_returns(self) -> pd.DataFrame:
        self.data.reset_index(inplace=True)
        self.data = self.data.astype({'Adj Close': float})
        self.data['Daily_Returns'] = self.data['Adj Close'].pct_change()
        self.data.columns = COLUMNS
        return self.data
