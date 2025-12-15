import pandas as pd
from datetime import timedelta

def prepare_data_for_strategy(ticker_df: pd.DataFrame, trades_df: pd.DataFrame, product: str, freq: str = '1min') -> pd.DataFrame:
    """
    Prepares the ticker data for the Dual Thrust strategy by resampling it to a fixed frequency.
    """
    # Filter for the specific product
    p_data = ticker_df[ticker_df['Product'] == product].copy() if not ticker_df.empty else pd.DataFrame()
    p_trades = trades_df[trades_df['Product'] == product].copy() if not trades_df.empty else pd.DataFrame()
    
    if p_data.empty:
        return pd.DataFrame()

    # Set index to Time
    p_data = p_data.set_index('Time').sort_index()
    if not p_trades.empty:
        p_trades = p_trades.set_index('Time').sort_index()
    
    # Resample Ticker (State) -> Take Last and ffill
    # We include BestBidQty and BestAskQty
    resampled = p_data[['BestBid', 'BestAsk', 'BestBidQty', 'BestAskQty']].resample(freq).last().ffill()
    
    # Resample Trades (Events) -> Sum Quantity
    if not p_trades.empty:
        traded_vol = p_trades['Quantity'].resample(freq).sum()
        buy_vol = p_trades[p_trades['Side'] == 'BUY']['Quantity'].resample(freq).sum()
        sell_vol = p_trades[p_trades['Side'] == 'SELL']['Quantity'].resample(freq).sum()
    else:
        traded_vol = pd.Series(0, index=resampled.index)
        buy_vol = pd.Series(0, index=resampled.index)
        sell_vol = pd.Series(0, index=resampled.index)
    
    # Align indices
    # Ensure traded_vol has same index as resampled (fill missing with 0)
    traded_vol = traded_vol.reindex(resampled.index, fill_value=0)
    buy_vol = buy_vol.reindex(resampled.index, fill_value=0)
    sell_vol = sell_vol.reindex(resampled.index, fill_value=0)
    
    resampled['traded_qty'] = traded_vol
    resampled['buy_vol'] = buy_vol
    resampled['sell_vol'] = sell_vol
    
    # Calculate Mid
    resampled['mid'] = (resampled['BestBid'] + resampled['BestAsk']) / 2
    
    # Rename columns to match dual_thrust expectation if needed, or adjust dual_thrust
    resampled = resampled.rename(columns={
        'BestBid': 'best_bid', 
        'BestAsk': 'best_ask',
        'BestBidQty': 'bid_qty',
        'BestAskQty': 'ask_qty'
    })
    
    # Calculate VWAP (Volume Weighted Average Price) based on the order book
    resampled['vwap'] = (
        (resampled['best_bid'] * resampled['bid_qty'] + resampled['best_ask'] * resampled['ask_qty']) /
        (resampled['bid_qty'] + resampled['ask_qty']).replace(0, float('nan'))  # Avoid division by zero
    )

    # Calculate total buy/sell depth
    resampled['total_bid_depth'] = p_data['BestBidQty'].resample(freq).sum()
    resampled['total_ask_depth'] = p_data['BestAskQty'].resample(freq).sum()

    return resampled

def dual_thrust(data: pd.DataFrame, n: int, k1: float, k2: float, delivery_hour: pd.Timestamp, trading_window_open: timedelta, trading_window_close: timedelta):
    """
    Calculates dual thrust trading signals.
    """
    if data.empty:
        return None, None, None

    # Calculate rolling high, low, close, Shift to use previous n period's data for current signal
    window = f'{n}min'
    
    rolling_high = data['best_ask'].rolling(window=window).max().shift(1)
    rolling_low = data['best_bid'].rolling(window=window).min().shift(1)
    close = data['mid'].shift(1)

    # Calculate range
    highest_high = rolling_high
    lowest_low = rolling_low
    
    # Range calculation
    range_val = pd.concat([(highest_high - close).abs(), (close - lowest_low).abs()], axis=1).max(axis=1)
    
    # Calculate bands
    open_price = data['mid'].shift(1) # Using previous close as open approximation for continuous trading
    upper_band = open_price + k1 * range_val
    lower_band = open_price - k2 * range_val
    
    # Generate signals
    signals = pd.Series(0, index=data.index)
    signals[data['best_bid'] > upper_band] = 1  # Buy signal
    signals[data['best_ask'] < lower_band] = -1 # Sell signal
    
    # Filter signals based on trading window
    trading_start = delivery_hour - trading_window_open
    trading_end = delivery_hour - trading_window_close
    
    # Ensure we don't filter out everything if the data doesn't align perfectly, 
    # but strictly following the strategy:
    mask = (signals.index >= trading_start) & (signals.index <= trading_end)
    
    signals = signals[mask]
    upper_band = upper_band[mask]
    lower_band = lower_band[mask]
    
    return signals, upper_band, lower_band
