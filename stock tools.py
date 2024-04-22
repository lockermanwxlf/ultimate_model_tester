import pandas as pd
import requests
import os
import datetime as dt
import time

def get_stock_info():
    nasdaq_df = pd.read_json('https://raw.githubusercontent.com/rreichel3/US-Stock-Symbols/main/nasdaq/nasdaq_full_tickers.json')
    amex_df = pd.read_json('https://raw.githubusercontent.com/rreichel3/US-Stock-Symbols/main/amex/amex_full_tickers.json')
    nyse_df = pd.read_json('https://raw.githubusercontent.com/rreichel3/US-Stock-Symbols/main/nyse/nyse_full_tickers.json')
    
    
    nasdaq_df = nasdaq_df[~nasdaq_df['symbol'].str.contains(r'[\^/]')]
    amex_df = amex_df[~amex_df['symbol'].str.contains(r'[\^/]')]
    nyse_df = nyse_df[~nyse_df['symbol'].str.contains(r'[\^/]')]
    
    nasdaq_df.to_csv('Stock Info by Provider/NASDAQ.csv', index=False)
    amex_df.to_csv('Stock Info by Provider/AMEX.csv', index=False)
    nyse_df.to_csv('Stock Info by Provider/NYSE.csv', index=False)
    
    total_df = pd.concat([nasdaq_df, amex_df, nyse_df], axis=0)
    total_df.sort_values(by='symbol', ascending=True, inplace=True)
    total_df['symbol'] = total_df['symbol'].str.strip()
    total_df.to_csv('Stock Info.csv', index=False)
    
    
def get_stock_history_path(symbol: str):
    return f'Stock History/{symbol}.csv'
    
def __get_stock_history_alpha_vantage(symbol: str):
    # not using alpha vantage. 5 calls/minute for 5k stocks per day
    url = f'https://www.alphavantage.co/query?function=TIME_SERIES_DAILY&symbol={symbol}&apikey={os.environ['ALPHAVANTAGEAPIKEY']}&datatype=csv&outputsize=full'
    df = pd.read_csv(url)
    df.to_excel(f'Stock History/{symbol}.xlsx', index=False)
    
def get_last_close_timestamp():
    today = dt.datetime.now(dt.UTC)
    last_close_datetime = dt.datetime(today.year, today.month, today.day if today.hour >= 21 else today.day-1, 21, 0, 0, 0, dt.UTC)
    last_close_timestamp = round(last_close_datetime.timestamp())
    return last_close_timestamp

def get_stock_history_yahoo_finance(symbol: str):
    last_close_timestamp = get_last_close_timestamp()
    url = f'https://query1.finance.yahoo.com/v7/finance/download/{symbol}?period1=1451606400&period2={last_close_timestamp}&interval=1d&events=history&includeAdjustedClose=true'
    df = pd.read_csv(url, index_col='Date', parse_dates=True)
    df.to_csv(get_stock_history_path(symbol))

def populate_stock_histories():
    df = pd.read_csv('Stock Info.csv')
    requests = 0
    for symbol in df['symbol']:
        if not os.path.exists(get_stock_history_path(symbol)):
            print(symbol)
            get_stock_history_yahoo_finance(symbol)
            time.sleep(0.3)
            requests = (requests + 1) % 10
            if requests == 0:
                time.sleep(4)
                
def update_stock_history(symbol: str):
    stock_history_path = get_stock_history_path(symbol)
    if not os.path.exists(stock_history_path):
        get_stock_history_yahoo_finance(symbol)
        return False
    df = pd.read_csv(stock_history_path, index_col='Date', parse_dates=True)
    latest_date = df.index.max()
    now = pd.Timestamp.utcnow()
    # Return if last entry was on Friday and today is before the following Monday's close.
    if latest_date.dayofweek == 4:
        time_delta = now.timestamp() - latest_date.timestamp()
        if time_delta < (3 * 24 + 21) * 3600:
            return False
    url = f'https://query1.finance.yahoo.com/v7/finance/download/{symbol}?period1={round(latest_date.timestamp())}&period2={round(now.timestamp())}&interval=1d&events=history&includeAdjustedClose=true'
    print(url)
    new_df = pd.read_csv(url, index_col='Date', parse_dates=True)
    pd.concat([df, new_df], axis=0).drop_duplicates().to_csv(stock_history_path)
    return True

def update_all_stock_histories():
    df = pd.read_csv('Stock Info.csv')
    requests = 0
    for symbol in df['symbol']:
        if update_stock_history(symbol):
            time.sleep(0.3)
            requests = (requests + 1) % 10
            if requests == 0:
                time.sleep(4)
            
if __name__ == '__main__':
    get_stock_info()
    populate_stock_histories()