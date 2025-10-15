import pickle
from datetime import datetime
import pandas as pd

# config
ignore_quarterly_trades = True
quantiles = [0.025, 0.25]
resolution = '60min'

# setup
with open('pickle/all_open_interest.pkl', 'rb') as f:
    file = pickle.load(f)

# all series:
# OrderId InitialId ParentId Side Product: DeliveryStart DeliveryEnd CreationTime DeliveryArea ExecutionRestriction UserDefinedBlock LinkedBasketId RevisionNo ActionCode TransactionTime (when server processed trade initiation not when it's filled) ValidityTime Price Currency Quantity (- MAW?) QuantityUnit Volume VolumeUnit BlockVolume Trading_time Unique_Data_ID

hourly_data = {}

for _, dataframe in file.items():
    # series
    extracted_df = pd.DataFrame({
        'CreationTime': pd.to_datetime(dataframe['CreationTime'], errors='coerce'),
        'DeliveryStart': pd.to_datetime(dataframe['DeliveryStart'], errors='coerce'),
        'DeliveryEnd': pd.to_datetime(dataframe['DeliveryEnd'], errors='coerce'),
        'Side': dataframe['Side'],
        'Product': dataframe['Product'],
        'Price': dataframe['Price'],
        'Volume': dataframe['Volume']
    })

    # iterate over extracted_df rows
    for _, row in extracted_df.iterrows():
        for hour in pd.date_range(start=row['DeliveryStart'].floor('h'), end=row['DeliveryEnd'].floor('h'), freq='h'):
            if hour not in hourly_data:
                hourly_data[hour] = []
            hourly_data[hour].append(row)

for hour in hourly_data:
    hourly_data[hour] = pd.DataFrame(hourly_data[hour])

hourly_evolution = {}

print("done loading, processing each hourly financial product")

for hour, df in hourly_data.items():
    if ignore_quarterly_trades:
        # all: ['XBID_Hour_Power', 'Intraday_Hour_Power', 'XBID_Quarter_Hour_Power', 'Intraday_Quarter_Hour_Power']
        df = df[df['Product'].isin(['XBID_Hour_Power', 'Intraday_Hour_Power'])]

    if df.empty:
        continue

    # separate buy and sell
    buys = df[df['Side'] == 'BUY']
    sells = df[df['Side'] == 'SELL']

    # aggregate by CreationTime
    buys_agg = buys.groupby('CreationTime').agg({'Price': 'mean', 'Volume': 'sum'}).reset_index()
    sells_agg = sells.groupby('CreationTime').agg({'Price': 'mean', 'Volume': 'sum'}).reset_index()

    # Quantile aggregations
    buys_quantiles = buys.groupby('CreationTime').agg({
        'Price': [lambda x: x.quantile(q) for q in quantiles],
        'Volume': 'sum'
    }).reset_index()
    buys_quantiles.columns = ['CreationTime'] + [f'Price_q{q}' for q in quantiles] + ['Volume']

    sells_quantiles = sells.groupby('CreationTime').agg({
        'Price': [lambda x: x.quantile(1-q) for q in quantiles],
        'Volume': 'sum'
    }).reset_index()
    sells_quantiles.columns = ['CreationTime'] + [f'Price_q{1-q}' for q in quantiles] + ['Volume']

    # resample to desired resolution
    buys_resampled = buys_agg.set_index('CreationTime').resample(resolution).agg({'Price': 'mean', 'Volume': 'sum'}).fillna(0).reset_index()
    sells_resampled = sells_agg.set_index('CreationTime').resample(resolution).agg({'Price': 'mean', 'Volume': 'sum'}).fillna(0).reset_index()

    # store in hourly_evolution
    hourly_evolution[hour] = {
        'buys': buys_resampled,
        'sells': sells_resampled
    }

print("done processing, plotting")

# plot example for a specific hour
import matplotlib.pyplot as plt

example_hour = list(hourly_evolution.keys())[0]
example_data = hourly_evolution[example_hour]

# print the quantiles dataframes for debugging
for q in quantiles:
    print(f"Buy Price Q{q}:\n", example_data['buys'].head())
    print(f"Sell Price Q{1-q}:\n", example_data['sells'].head())