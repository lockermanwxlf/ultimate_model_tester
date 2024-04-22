import pandas as pd

def evaluate_against_symbol(
    df: pd.DataFrame, 
    stochastic_period=14, 
    stochastic_smoothing=3,
    zscore_period=150,
    stochastic_buy_in = 0.3,
    stochastic_sell_out = 0.7,
    zscore_buy_in = -2
    ):
    '''df should contain Date, Open, High, Low, Close.\nReturns avg percent gain per entry.'''
    max_high = df['High'].rolling(stochastic_period).max()
    min_low = df['Low'].rolling(14).min()
    df['Stochastic'] = (df['Close']-min_low)/(max_high-min_low)
    
    meanr_window = df['Close'].rolling(zscore_period)
    mean = meanr_window.mean()
    stddev = meanr_window.std()
    df['Z-Score'] = (df['Close'] - mean)/stddev
    
    entry = None
    stoch_stop = False
    close_stop = 0
    percent_total = 1
    percent_gains = []
    
    for date, close, stoch, zscore in zip(df.index, df['Close'], df['Stochastic'], df['Z-Score']):
        if entry is None:
            if zscore < zscore_buy_in and stoch < stochastic_buy_in:
                entry = close
                stoch_stop = False
                close_stop = close * 0.9
        else:
            if (stoch_stop and stoch < stochastic_sell_out) or (close < close_stop):
                percent_gains.append(close/entry - 1)
                percent_total *= close / entry
                entry = None
                close_stop = 0
                stoch_stop = False
                continue
            if stoch > stochastic_sell_out:
                stoch_stop = True
            close_stop = max(close_stop, close * 0.9)
    
    return percent_total - 1
    return 0 if len(percent_gains) == 0 else pd.Series(percent_gains).mean()