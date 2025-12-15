import pandas as pd
import numpy as np
from typing import List, Optional
from matching_engine import MatchingEngine

class ReplayEngine:
    def __init__(self, filepath: str):
        self.filepath = filepath
        self.df: Optional[pd.DataFrame] = None
        self.min_time: Optional[pd.Timestamp] = None
        self.max_time: Optional[pd.Timestamp] = None
        self.products: Optional[List[pd.Timestamp]] = None
        self.products_with_duration: Optional[pd.DataFrame] = None
        self.ticker_df: Optional[pd.DataFrame] = None
        self.trades_df: Optional[pd.DataFrame] = None

    def load_data(self):
        """Loads and preprocesses the data from the CSV file."""
        # Skip the first line which is a comment
        self.df = pd.read_csv(self.filepath, skiprows=1, low_memory=False)
        
        # Parse dates
        time_cols = ['DeliveryStart', 'DeliveryEnd', 'CreationTime', 'TransactionTime', 'ValidityTime']
        for col in time_cols:
            if col in self.df.columns:
                self.df[col] = pd.to_datetime(self.df[col])
                
        self.min_time = self.df['TransactionTime'].min()
        self.max_time = self.df['DeliveryEnd'].max()
        self.products = sorted(self.df['DeliveryStart'].unique())
        self.products_with_duration = self.df[['DeliveryStart', 'DeliveryEnd']].drop_duplicates().sort_values('DeliveryStart').reset_index(drop=True)
        
        # Sort by TransactionTime and RevisionNo to ensure correct order
        self.df = self.df.sort_values(['TransactionTime', 'RevisionNo'])

    def get_snapshot(self, query_time: pd.Timestamp) -> pd.DataFrame:
        """
        Returns the active orders at a specific time.
        """
        # Filter events up to query_time
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

    def precompute_ticker(self) -> pd.DataFrame:
        """
        Iterates through all events to generate a history of Best Bid/Ask changes.
        Delegates matching logic to MatchingEngine.
        Returns a DataFrame with columns: [Time, Product, BestBid, BestAsk, BestBidQty, BestAskQty]
        """
        matching_engine = MatchingEngine()
        
        total_rows = len(self.df)
        print(f"Precomputing ticker with matching for {total_rows} events...")
        
        for idx, row in self.df.iterrows():
            matching_engine.process_event(row)
                
        self.ticker_df, self.trades_df = matching_engine.get_results()
        print(f"Precomputation complete. Generated {len(self.ticker_df)} ticker events and {len(self.trades_df)} trades.")
        return self.ticker_df

