import pandas as pd
import plotly.graph_objects as go

df = pd.read_csv('20210625-130000_to_20210626-225000_full_transaction_book.csv')
data_intervals = '15min'

K = 0.3
Ks = Kx = K
lookback = 2
sell_offset = 2 # only use data until 1 hour before delivery start

min_time = pd.to_datetime(df['DeliveryStart'].min())
max_time = pd.to_datetime(df['DeliveryEnd'].max())
time_range = pd.date_range(start=min_time, end=max_time, freq=data_intervals)

print(f"reading {min_time} to {max_time}")

fig = go.Figure()

ohlc_times = []
ohlc_open = []
ohlc_high = []
ohlc_low = []
ohlc_close = []

dt_times = []
dt_high_threshold = []
dt_low_threshold = []

for time in time_range:
    # DEPENDENT ON PAST DATA:

    # Close Price (prev day):
    #  the last consensus value of the market before it shut down.
    #  helps gauge market sentiment going into the next day.

    # Open Price (Current Day):
    #  captures overnight sentiment changes (e.g., from news, macro factors, etc.)
    #  often shows a gap from the previous close, which signals potential breakout direction.

    # High/Low (Previous Day):
    #  determines the recent volatility or range.
    #  used to compute the breakout thresholds.

    if len(ohlc_times) >= lookback+sell_offset:
        # RANGE FORMULA 1
        # HH = max(ohlc_high[-lookback-sell_offset:-sell_offset])
        # LL = min(ohlc_low[-lookback-sell_offset:-sell_offset])
        # HC = max(ohlc_close[-lookback-sell_offset:-sell_offset])
        # LC = min(ohlc_close[-lookback-sell_offset:-sell_offset])
        # range = max(max(ohlc_high[start_index:])-min(ohlc_close[start_index:]), max(ohlc_close[start_index:])-min(ohlc_low[start_index:]))

        # RANGE FORMULA 2
        recent_high = max(ohlc_high[-lookback-sell_offset:-sell_offset])
        recent_low = min(ohlc_low[-lookback-sell_offset:-sell_offset])
        recent_close = ohlc_close[-lookback-sell_offset]
        recent_open = ohlc_open[-sell_offset]
        range = max(recent_high-recent_low, abs(recent_close-recent_open))

        uptrack = recent_open + Ks * range
        downtrack = recent_open - Kx * range

        dt_times.append(time)
        dt_high_threshold.append(uptrack)
        dt_low_threshold.append(downtrack)

    mask = (pd.to_datetime(df['DeliveryStart']) <= time) & (pd.to_datetime(df['DeliveryEnd']) > time)
    prices = df.loc[mask, 'Price']


    if not prices.empty:
        high = prices.max()
        low = prices.min()
        open = prices.iloc[0]
        close = prices.iloc[-1]

        # print(f"Time: {time}, High: {high}, Low: {low}, Open: {open}, Close: {close}")

        ohlc_times.append(time)
        ohlc_open.append(open)
        ohlc_high.append(high)
        ohlc_low.append(low)
        ohlc_close.append(close)

        if len(dt_high_threshold) > 0 and len(dt_low_threshold) > 0:
            breakout_orders_mask = mask & ((df['Price'] > uptrack) | (df['Price'] < downtrack))
            breakout_orders = df.loc[breakout_orders_mask, ['Price', 'ActionCode', 'Side', 'Volume']]

fig.add_trace(go.Candlestick(x=ohlc_times, open=ohlc_open, high=ohlc_high, low=ohlc_low, close=ohlc_close, name='OHLC'))
fig.add_trace(go.Scatter(x=dt_times, y=dt_high_threshold, mode='lines', name='High Breakout Threshold', line=dict(color='red', dash='dash')))
fig.add_trace(go.Scatter(x=dt_times, y=dt_low_threshold, mode='lines', name='Low Breakout Threshold', line=dict(color='green', dash='dash')))

fig.update_layout(
    title=f'OHLC with K={K}, lookback={lookback}x{data_intervals}, sell offset={sell_offset}x{data_intervals}',
    xaxis_title='Time',
    yaxis_title='Price',
    xaxis_rangeslider_visible=False,
    template='plotly_dark'
)
fig.update_xaxes(minor=dict(showgrid=True))
fig.show()