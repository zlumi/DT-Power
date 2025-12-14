import pandas as pd
import numpy as np

class ReplayEngine:
    def __init__(self, filepath):
        self.filepath = filepath
        self.df = None
        self.min_time = None
        self.max_time = None
        self.products = None

    def load_data(self):
        # Skip the first line which is a comment
        self.df = pd.read_csv(self.filepath, skiprows=1, low_memory=False)
        
        # Parse dates
        time_cols = ['DeliveryStart', 'DeliveryEnd', 'CreationTime', 'TransactionTime', 'ValidityTime']
        for col in time_cols:
            if col in self.df.columns:
                self.df[col] = pd.to_datetime(self.df[col])
                
        self.min_time = self.df['TransactionTime'].min()
        self.max_time = self.df['TransactionTime'].max()
        self.products = sorted(self.df['DeliveryStart'].unique())
        
        # Sort by TransactionTime and RevisionNo to ensure correct order
        self.df = self.df.sort_values(['TransactionTime', 'RevisionNo'])

    def get_snapshot(self, query_time):
        """
        Returns the active orders at a specific time.
        """
        # Filter events up to query_time
        # Optimization: If we assume append-only, we could use searchsorted, 
        # but we need to filter by InitialId.
        
        mask = self.df['TransactionTime'] <= query_time
        df_past = self.df.loc[mask]
        
        if df_past.empty:
            return pd.DataFrame()
            
        # Get latest state for each order
        latest_states = df_past.groupby('InitialId').last()
        
        # Filter for active orders
        active_orders = latest_states[
            (latest_states['ActionCode'].isin(['A', 'M'])) & 
            (latest_states['Quantity'] > 0)
        ]
        
        return active_orders

    def precompute_ticker(self):
        """
        Iterates through all events to generate a history of Best Bid/Ask changes.
        Returns a DataFrame with columns: [Time, Product, BestBid, BestAsk]
        """
        ticker_data = []
        
        # Dictionary to hold current active orders: {Product: {OrderId: {Side, Price, Quantity}}}
        # Actually, we need to track by InitialId.
        # {InitialId: {Product, Side, Price, Quantity}}
        # And also an index to quickly get orders for a product: {Product: {InitialId}}
        
        orders = {}
        product_map = {} # Product -> set(InitialId)
        
        # Current Best Prices cache: {Product: (BestBid, BestAsk)}
        current_best = {}
        
        # We iterate through the dataframe
        # It is already sorted by TransactionTime
        
        total_rows = len(self.df)
        print(f"Precomputing ticker for {total_rows} events...")
        
        for idx, row in self.df.iterrows():
            initial_id = row['InitialId']
            action = row['ActionCode']
            quantity = row['Quantity']
            price = row['Price']
            side = row['Side']
            product = row['DeliveryStart']
            time = row['TransactionTime']
            
            # Update Order State
            if action in ['A', 'M'] and quantity > 0:
                # Add or Modify
                orders[initial_id] = {
                    'Product': product,
                    'Side': side,
                    'Price': price,
                    'Quantity': quantity
                }
                if product not in product_map:
                    product_map[product] = set()
                product_map[product].add(initial_id)
            else:
                # Delete or Inactive
                if initial_id in orders:
                    # We need to know the product to remove from product_map
                    # But the row might not have the product info if it's a pure delete?
                    # The CSV seems to have Product info on all rows.
                    p = orders[initial_id]['Product']
                    del orders[initial_id]
                    if p in product_map:
                        product_map[p].discard(initial_id)
            
            # Re-calculate Best Bid/Ask for this product
            # Optimization: Only re-calc if the changed order was the best, or if it becomes the best.
            # For simplicity, just re-calc the product.
            
            best_bid = None
            best_ask = None
            
            if product in product_map and product_map[product]:
                # Get all orders for this product
                p_orders = [orders[oid] for oid in product_map[product]]
                
                bids = [o['Price'] for o in p_orders if o['Side'] == 'BUY']
                asks = [o['Price'] for o in p_orders if o['Side'] == 'SELL']
                
                if bids:
                    best_bid = max(bids)
                if asks:
                    best_ask = min(asks)
            
            # Check if changed
            prev_bid, prev_ask = current_best.get(product, (None, None))
            
            if best_bid != prev_bid or best_ask != prev_ask:
                current_best[product] = (best_bid, best_ask)
                ticker_data.append({
                    'Time': time,
                    'Product': product,
                    'BestBid': best_bid,
                    'BestAsk': best_ask
                })
                
        self.ticker_df = pd.DataFrame(ticker_data)
        print(f"Precomputation complete. Generated {len(self.ticker_df)} ticker events.")
        return self.ticker_df
