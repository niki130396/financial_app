from pandas_datareader import DataReader
from pandas_datareader._utils import RemoteDataError


def modify_table(df):
    columns = ['Date', 'High', 'Low', 'Open', 'Close', 'Volume', 'Adj_Close', 'Daily_Returns']
    df.reset_index(level=0, inplace=True)
    df = df.astype({'Adj Close': float})
    df['Daily_Returns'] = df['Adj Close'].pct_change()
    df.columns = columns
    return df


def get_table(symbol, date):
    try:
        df = DataReader(symbol, 'yahoo', date)
        df = modify_table(df)
        return df
    except (RemoteDataError, KeyError):
        return None
