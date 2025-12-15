import streamlit as st
import pandas as pd
from plotly.subplots import make_subplots
import plotly.graph_objects as go
from datetime import timedelta
from replay_engine import ReplayEngine
from config import FILEPATH
from strategy import prepare_data_for_strategy, dual_thrust
from utils import load_engine

# --- Constants & Config ---
st.set_page_config(layout="wide", page_title="Dual Thrust Strategy Visualization")

# --- UI Components ---
def render_sidebar(engine):
    st.sidebar.header("Configuration")

    # Add checkbox for quarter-hour products
    show_quarter_hour = st.sidebar.checkbox("Show Quarter-Hour Products", value=False)
    
    # Product Selector
    # Get available products from engine
    products_with_duration = engine.products_with_duration
    
    if show_quarter_hour:
        # Break down long products into 15-min intervals
        all_products = []
        for _, row in products_with_duration.iterrows():
            start, end = row['DeliveryStart'], row['DeliveryEnd']
            # Generate 15-min products
            current = start
            while current < end:
                all_products.append(current)
                current += timedelta(minutes=15)
        products = sorted(list(set(all_products)))
    else:
        # Break down long products into 1-hour intervals for products longer than 1 hour
        # and only show hour-start products
        all_products = []
        for _, row in products_with_duration.iterrows():
            start, end = row['DeliveryStart'], row['DeliveryEnd']
            duration_hours = (end - start).total_seconds() / 3600
            if duration_hours > 1:
                 # Generate 1-hour products
                current = start
                while current < end:
                    all_products.append(current)
                    current += timedelta(hours=1)
            elif start.minute == 0: # Only include full hour products
                all_products.append(start)

        products = sorted(list(set(all_products)))


    product_labels = [p.strftime("%Y-%m-%d %H:%M") for p in products]
    product_map = dict(zip(product_labels, products))
    
    selected_label = st.sidebar.selectbox("Delivery Product", product_labels)
    selected_product = product_map[selected_label]
    
    st.sidebar.divider()
    st.sidebar.subheader("Dual Thrust Parameters")
    
    n = st.sidebar.number_input("Lookback Period (N minutes)", min_value=1, value=15, step=1)
    k1 = st.sidebar.number_input("K1 (Upper Band Coeff)", min_value=0.1, value=0.5, step=0.1)
    k2 = st.sidebar.number_input("K2 (Lower Band Coeff)", min_value=0.1, value=0.5, step=0.1)
    
    st.sidebar.subheader("Trading Window")
    window_open_m = st.sidebar.number_input("Trading Window Open (Minutes before delivery)", min_value=0, value=60, step=30)
    window_close_m = st.sidebar.number_input("Trading Window Close (Minutes before delivery)", min_value=0, value=15, step=5)
    
    return selected_product, n, k1, k2, window_open_m, window_close_m, show_quarter_hour

def render_chart(data, signals, upper_band, lower_band, product):
    # Create Subplots: 4 Rows
    fig = make_subplots(rows=4, cols=1, shared_xaxes=True, 
                        vertical_spacing=0.05, row_heights=[0.7, 0.1, 0.1, 0.1])

    # --- Row 1: Price ---
    # Plot Prices
    fig.add_trace(go.Scatter(x=data.index, y=data['best_bid'], mode='lines', name='Best Bid', line=dict(color='green', width=1)), row=1, col=1)
    fig.add_trace(go.Scatter(x=data.index, y=data['best_ask'], mode='lines', name='Best Ask', line=dict(color='red', width=1)), row=1, col=1)
    fig.add_trace(go.Scatter(x=data.index, y=data['mid'], mode='lines', name='Mid Price', line=dict(color='black', width=1, dash='dot')), row=1, col=1)
    fig.add_trace(go.Scatter(x=data.index, y=data['vwap'], mode='lines', name='VWAP', line=dict(color='blue', width=1, dash='dot')), row=1, col=1)

    # Plot Bands
    fig.add_trace(go.Scatter(x=upper_band.index, y=upper_band, mode='lines', name='Upper Band', line=dict(color='orange', width=2)), row=1, col=1)
    fig.add_trace(go.Scatter(x=lower_band.index, y=lower_band, mode='lines', name='Lower Band', line=dict(color='purple', width=2)), row=1, col=1)

    # Plot Signals
    # Buy Signals
    buy_indices = signals[signals == 1].index
    buy_indices = buy_indices.intersection(data.index)
    buy_signals = data.loc[buy_indices]

    if not buy_signals.empty:
        fig.add_trace(go.Scatter(
            x=buy_signals.index, 
            y=buy_signals['best_ask'], # Buy at Ask
            mode='markers', 
            name='Buy Signal', 
            marker=dict(symbol='triangle-up', size=10, color='blue')
        ), row=1, col=1)
        
    # Sell Signals
    sell_indices = signals[signals == -1].index
    sell_indices = sell_indices.intersection(data.index)
    sell_signals = data.loc[sell_indices]

    if not sell_signals.empty:
        fig.add_trace(go.Scatter(
            x=sell_signals.index, 
            y=sell_signals['best_bid'], # Sell at Bid
            mode='markers', 
            name='Sell Signal', 
            marker=dict(symbol='triangle-down', size=10, color='black')
        ), row=1, col=1)

    # --- Row 2: Best Bid/Ask Volume ---
    fig.add_trace(go.Bar(
        x=data.index,
        y=data['bid_qty'],
        name='Best Bid Volume',
        marker_color='green',
        opacity=0.6,
        showlegend=False
    ), row=2, col=1)

    fig.add_trace(go.Bar(
        x=data.index,
        y=data['ask_qty'],
        name='Best Ask Volume',
        marker_color='red',
        opacity=0.6,
        showlegend=False
    ), row=2, col=1)

    # --- Row 3: Total Order Book Depth ---
    fig.add_trace(go.Bar(
        x=data.index,
        y=data['total_bid_depth'],
        name='Total Bid Depth',
        marker_color='green',
        opacity=0.6,
        showlegend=False
    ), row=3, col=1)

    fig.add_trace(go.Bar(
        x=data.index,
        y=data['total_ask_depth'],
        name='Total Ask Depth',
        marker_color='red',
        opacity=0.6,
        showlegend=False
    ), row=3, col=1)

    # --- Row 4: Traded Volume ---
    fig.add_trace(go.Bar(
        x=data.index,
        y=data['buy_vol'],
        name='Buy Volume',
        marker_color='green',
        showlegend=False
    ), row=4, col=1)

    fig.add_trace(go.Bar(
        x=data.index,
        y=data['sell_vol'],
        name='Sell Volume',
        marker_color='red',
        showlegend=False
    ), row=4, col=1)

    fig.update_layout(
        yaxis_title="Price",
        yaxis2_title="Best Prices' Volumes",
        yaxis3_title="Order Book Depth",
        yaxis4_title="Traded Volume",
        height=1200,
        template="plotly_white",
        barmode='stack' # Stack the bars in Rows 2 and 3
    )

    st.plotly_chart(fig, width='stretch')

# --- Main ---
def main():
    st.title("Dual Thrust Strategy Visualization")
    
    with st.spinner("Loading data..."):
        engine = load_engine()
        
    selected_product, n, k1, k2, window_open_m, window_close_m, show_quarter_hour = render_sidebar(engine)
    
    # Prepare Data
    # We use the ticker data from the engine
    ticker_df = engine.ticker_df
    trades_df = engine.trades_df
    
    # Resample and prepare
    strategy_data = prepare_data_for_strategy(ticker_df, trades_df, selected_product)
    
    if not strategy_data.empty:
        # Run Strategy
        trading_window_open = timedelta(minutes=window_open_m)
        trading_window_close = timedelta(minutes=window_close_m)
        
        signals, upper_band, lower_band = dual_thrust(
            strategy_data, 
            n, 
            k1, 
            k2, 
            selected_product, 
            trading_window_open, 
            trading_window_close
        )
        
        # Render
        render_chart(strategy_data, signals, upper_band, lower_band, selected_product)
        
        # Show Data
        with st.expander("View Data"):
            st.dataframe(strategy_data.join(pd.DataFrame({'Signal': signals, 'Upper': upper_band, 'Lower': lower_band})))
    else:
        st.warning("No data available for the selected product.")

if __name__ == "__main__":
    main()
