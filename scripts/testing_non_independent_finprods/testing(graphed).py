import pandas as pd
import matplotlib.pyplot as plt

# Parse dates directly when reading, handle errors
df = pd.read_csv(
    '20210625-130000_to_20210626-225000_full_transaction_book.csv',
    parse_dates=['TransactionTime', 'DeliveryStart'],
    infer_datetime_format=True,
    dayfirst=False,
    keep_default_na=False
)

# Collect data for plotting
buy_diffs = []
sell_diffs = []

for idx, row in df.iterrows():
    side = row['Side']
    buy_or_sell = 'buy' if str(side).strip().upper() == 'BUY' else 'sell'
    try:
        delivery_start = pd.to_datetime(row['DeliveryStart'], errors='coerce')
        transaction_time = pd.to_datetime(row['TransactionTime'], errors='coerce')
        if pd.isna(delivery_start) or pd.isna(transaction_time):
            continue
        time_diff = (delivery_start - transaction_time).total_seconds()
        if buy_or_sell == 'buy':
            buy_diffs.append(time_diff)
        else:
            sell_diffs.append(time_diff)
    except Exception:
        continue

# Plotting
plt.figure(figsize=(10, 6))
plt.hist(buy_diffs, bins=50, alpha=0.6, label='Buy', color='blue')
plt.hist(sell_diffs, bins=50, alpha=0.6, label='Sell', color='red')
plt.xlabel('Seconds to Delivery Start')
plt.ylabel('Number of Transactions')
plt.title('Time Difference to Delivery Start by Transaction Type')
plt.legend()
plt.tight_layout()
plt.show()
