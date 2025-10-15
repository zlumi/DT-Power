import pandas as pd

# Parse dates directly when reading, handle errors
df = pd.read_csv(
    '20210625-130000_to_20210626-225000_full_transaction_book.csv',
    parse_dates=['TransactionTime', 'DeliveryStart'],
    infer_datetime_format=True,
    dayfirst=False,
    keep_default_na=False
)

# For each transaction, print latest buy/sell and time difference to deliveryStart
for idx, row in df.iterrows():
    side = row['Side']
    buy_or_sell = 'buy' if str(side).strip().upper() == 'BUY' else 'sell'
    # Ensure both columns are Timestamps
    try:
        delivery_start = pd.to_datetime(row['DeliveryStart'], errors='coerce')
        transaction_time = pd.to_datetime(row['TransactionTime'], errors='coerce')
        if pd.isna(delivery_start) or pd.isna(transaction_time):
            print(f"{buy_or_sell}, invalid datetime")
        else:
            time_diff = (delivery_start - transaction_time).total_seconds()
            print(f"{buy_or_sell}, {time_diff} seconds")
    except Exception as e:
        print(f"{buy_or_sell}, error: {e}")