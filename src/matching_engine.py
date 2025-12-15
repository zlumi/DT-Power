from typing import List, Dict, Tuple, Optional
import pandas as pd

class MatchingEngine:
    def __init__(self):
        # Order Books per product: {Product: {'bids': [], 'asks': []}}
        # List items: [Price, Time, InitialId, Quantity]
        # Bids sorted by Price DESC, Time ASC
        # Asks sorted by Price ASC, Time ASC
        self.books: Dict[str, Dict[str, List]] = {}
        
        # Map InitialId to Order details for quick deletion/lookup
        # InitialId -> {'Product': p, 'Side': s, 'Price': p, 'Quantity': q, 'Time': t}
        self.order_lookup: Dict[int, Dict] = {}
        
        self.trades: List[Dict] = []
        self.ticker_data: List[Dict] = []
        
        # Current Best Prices cache: {Product: (BestBid, BestAsk, BestBidQty, BestAskQty)}
        self.current_best: Dict[str, Tuple] = {}

    def process_event(self, row: pd.Series):
        initial_id = row['InitialId']
        action = row['ActionCode']
        quantity = row['Quantity']
        price = row['Price']
        side = row['Side']
        product = row['DeliveryStart']
        time = row['TransactionTime']
        
        if product not in self.books:
            self.books[product] = {'bids': [], 'asks': []}
        
        # 1. Handle Deletion / Modification (Remove old version first)
        if initial_id in self.order_lookup:
            self._remove_order(initial_id)

        # 2. Handle Add / Modify (Insert new version and Match)
        if action in ['A', 'M'] and quantity > 0:
            self._match_and_add_order(product, side, price, quantity, time, initial_id)
        
        # 3. Record Ticker State
        self._update_ticker(product, time)

    def _remove_order(self, initial_id: int):
        old_order = self.order_lookup[initial_id]
        old_product = old_order['Product']
        old_side = old_order['Side']
        
        # Remove from book
        if old_product in self.books:
            if old_side == 'BUY':
                self.books[old_product]['bids'] = [o for o in self.books[old_product]['bids'] if o[2] != initial_id]
            else:
                self.books[old_product]['asks'] = [o for o in self.books[old_product]['asks'] if o[2] != initial_id]
        
        del self.order_lookup[initial_id]

    def _match_and_add_order(self, product: str, side: str, price: float, quantity: int, time: pd.Timestamp, initial_id: int):
        remaining_qty = quantity
        
        if side == 'BUY':
            # Match against Asks (Price ASC)
            asks = self.books[product]['asks']
            while remaining_qty > 0 and asks:
                best_ask = asks[0] # [Price, Time, Id, Qty]
                ask_price = best_ask[0]
                ask_id = best_ask[2]
                ask_qty = best_ask[3]
                
                if price >= ask_price:
                    trade_qty = min(remaining_qty, ask_qty)
                    self._record_trade(time, product, ask_price, trade_qty, 'BUY')
                    
                    remaining_qty -= trade_qty
                    
                    # Update Ask
                    new_ask_qty = ask_qty - trade_qty
                    if new_ask_qty > 0:
                        asks[0][3] = new_ask_qty
                        if ask_id in self.order_lookup:
                            self.order_lookup[ask_id]['Quantity'] = new_ask_qty
                    else:
                        asks.pop(0)
                        if ask_id in self.order_lookup:
                            del self.order_lookup[ask_id]
                else:
                    break # No more overlap
            
            # Add remaining to Bids
            if remaining_qty > 0:
                new_order = [price, time, initial_id, remaining_qty]
                self.books[product]['bids'].append(new_order)
                # Sort Bids: Price DESC, Time ASC
                self.books[product]['bids'].sort(key=lambda x: (-x[0], x[1]))
                
                self.order_lookup[initial_id] = {
                    'Product': product, 'Side': 'BUY', 'Price': price, 'Quantity': remaining_qty, 'Time': time
                }

        else: # SELL
            # Match against Bids (Price DESC)
            bids = self.books[product]['bids']
            while remaining_qty > 0 and bids:
                best_bid = bids[0]
                bid_price = best_bid[0]
                bid_id = best_bid[2]
                bid_qty = best_bid[3]
                
                if price <= bid_price:
                    trade_qty = min(remaining_qty, bid_qty)
                    self._record_trade(time, product, bid_price, trade_qty, 'SELL')
                    
                    remaining_qty -= trade_qty
                    
                    # Update Bid
                    new_bid_qty = bid_qty - trade_qty
                    if new_bid_qty > 0:
                        bids[0][3] = new_bid_qty
                        if bid_id in self.order_lookup:
                            self.order_lookup[bid_id]['Quantity'] = new_bid_qty
                    else:
                        bids.pop(0)
                        if bid_id in self.order_lookup:
                            del self.order_lookup[bid_id]
                else:
                    break
            
            # Add remaining to Asks
            if remaining_qty > 0:
                new_order = [price, time, initial_id, remaining_qty]
                self.books[product]['asks'].append(new_order)
                # Sort Asks: Price ASC, Time ASC
                self.books[product]['asks'].sort(key=lambda x: (x[0], x[1]))
                
                self.order_lookup[initial_id] = {
                    'Product': product, 'Side': 'SELL', 'Price': price, 'Quantity': remaining_qty, 'Time': time
                }

    def _record_trade(self, time, product, price, quantity, side):
        self.trades.append({
            'Time': time,
            'Product': product,
            'Price': price,
            'Quantity': quantity,
            'Side': side
        })

    def _update_ticker(self, product: str, time: pd.Timestamp):
        best_bid = self.books[product]['bids'][0][0] if self.books[product]['bids'] else None
        best_ask = self.books[product]['asks'][0][0] if self.books[product]['asks'] else None
        best_bid_qty = self.books[product]['bids'][0][3] if self.books[product]['bids'] else 0
        best_ask_qty = self.books[product]['asks'][0][3] if self.books[product]['asks'] else 0
        
        # Check if changed
        prev_state = self.current_best.get(product, (None, None, None, None))
        current_state = (best_bid, best_ask, best_bid_qty, best_ask_qty)
        
        if current_state != prev_state:
            self.current_best[product] = current_state
            self.ticker_data.append({
                'Time': time,
                'Product': product,
                'BestBid': best_bid,
                'BestAsk': best_ask,
                'BestBidQty': best_bid_qty,
                'BestAskQty': best_ask_qty
            })

    def get_results(self) -> Tuple[pd.DataFrame, pd.DataFrame]:
        return pd.DataFrame(self.ticker_data), pd.DataFrame(self.trades)
