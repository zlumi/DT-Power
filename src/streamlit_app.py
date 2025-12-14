import streamlit as st
import pandas as pd
import plotly.graph_objects as go
from replay_engine import ReplayEngine
import datetime

# --- Constants & Config ---
FILEPATH = r"c:\Users\hankz\Documents\GitHub\DT-Power\data\Continuous_Orders-NL-20210626-20210628T042947000Z.csv"

st.set_page_config(layout="wide", page_title="Order Book Replay")

# --- Data Loading ---
@st.cache_resource
def load_engine():
    engine = ReplayEngine(FILEPATH)
    engine.load_data()
    engine.precompute_ticker()
    return engine

# --- State Management ---
def init_session_state(min_time, max_time):
    if 'replay_start' not in st.session_state:
        st.session_state.replay_start = min_time
    if 'replay_end' not in st.session_state:
        st.session_state.replay_end = max_time
    if 'include_fragmented' not in st.session_state:
        st.session_state.include_fragmented = False

def update_slider_callback():
    s, e = st.session_state.time_slider
    st.session_state.replay_start = s
    st.session_state.replay_end = e
    # Update manual input keys
    st.session_state.s_date = s.date()
    st.session_state.s_time = s.time()
    st.session_state.e_date = e.date()
    st.session_state.e_time = e.time()

def update_manual_callback(min_time, max_time):
    try:
        # Reconstruct datetimes from manual inputs
        s_dt = datetime.datetime.combine(st.session_state.s_date, st.session_state.s_time).replace(tzinfo=datetime.timezone.utc)
        e_dt = datetime.datetime.combine(st.session_state.e_date, st.session_state.e_time).replace(tzinfo=datetime.timezone.utc)

        if s_dt > e_dt:
            st.sidebar.error("Start time cannot be after end time")
        else:
            # Clamp to min/max
            s_dt = max(min_time, min(s_dt, max_time))
            e_dt = max(min_time, min(e_dt, max_time))
            st.session_state.replay_start = s_dt
            st.session_state.replay_end = e_dt
    except Exception:
        pass

def update_fragmented_callback():
    st.session_state.include_fragmented = st.session_state.include_fragmented_checkbox

# --- UI Components ---
def render_time_controls(min_time, max_time):
    st.sidebar.header("Replay Controls")
    
    # Slider
    st.sidebar.slider(
        "Replay Time Range",
        min_value=min_time,
        max_value=max_time,
        value=(st.session_state.replay_start, st.session_state.replay_end),
        format="MM-DD HH:mm:ss",
        step=datetime.timedelta(seconds=1),
        key='time_slider',
        on_change=update_slider_callback
    )

    # Precise Selectors
    with st.sidebar.expander("Precise Time Selection", expanded=True):
        st.caption("Start Time")
        c1, c2 = st.columns(2)
        c1.date_input("Date", value=st.session_state.replay_start.date(), min_value=min_time.date(), max_value=max_time.date(), key='s_date', on_change=lambda: update_manual_callback(min_time, max_time))
        c2.time_input("Time", value=st.session_state.replay_start.time(), step=60, key='s_time', on_change=lambda: update_manual_callback(min_time, max_time))
        
        st.caption("End Time")
        c3, c4 = st.columns(2)
        c3.date_input("Date", value=st.session_state.replay_end.date(), min_value=min_time.date(), max_value=max_time.date(), key='e_date', on_change=lambda: update_manual_callback(min_time, max_time))
        c4.time_input("Time", value=st.session_state.replay_end.time(), step=60, key='e_time', on_change=lambda: update_manual_callback(min_time, max_time))

    return st.session_state.replay_start, st.session_state.replay_end

def get_available_products(engine, history, window_minutes):
    # Compute duration (minutes) for each distinct delivery product from the original dataframe
    dur_df = engine.df[["DeliveryStart", "DeliveryEnd"]].drop_duplicates().copy()
    dur_df["DurationMin"] = ((dur_df["DeliveryEnd"] - dur_df["DeliveryStart"]).dt.total_seconds() / 60).astype(int)

    # Get available products (sorted) within the selected time range
    available_products = sorted(history['Product'].unique())

    # Filter available products by the selected delivery window size
    available_products = [p for p in available_products if int(dur_df.loc[dur_df['DeliveryStart'] == p, 'DurationMin'].iloc[0]) == int(window_minutes)]
    
    return available_products

def render_product_selector(available_products):
    # Update the product selection to show only available products
    filtered_product_labels = [p.strftime("%Y-%m-%d %H:%M") for p in available_products]
    filtered_product_map = dict(zip(filtered_product_labels, available_products))

    selected_labels = st.sidebar.multiselect(
        "Select Delivery Hours",
        options=filtered_product_labels,
        default=filtered_product_labels  # Default to all available products
    )

    selected_products = [filtered_product_map[l] for l in selected_labels]
    return sorted(selected_products)

def render_y_axis_controls(history, selected_products, start_time, end_time):
    st.sidebar.header("Y-Axis Range")

    # Filter history to only include selected products for range calculation
    if selected_products:
        relevant_history = history[history['Product'].isin(selected_products)]
    else:
        relevant_history = history

    # Calculate the minimum and maximum of the entire price series
    if not relevant_history.empty:
        price_min = int(min(relevant_history['BestBid'].min(), relevant_history['BestAsk'].min()))
        price_max = int(max(relevant_history['BestBid'].max(), relevant_history['BestAsk'].max()))
    else:
        price_min = 0
        price_max = 1000

    # Ensure valid range for slider
    if price_max <= price_min:
        price_max = price_min + 100

    y_min, y_max = st.sidebar.slider(
        "Range for Price Axis (EUR)",
        min_value=price_min,
        max_value=price_max,
        value=(price_min, price_max),
        step=1,
        key=f"y_slider_{start_time.timestamp()}_{end_time.timestamp()}_{str(selected_products)}"
    )
    return y_min, y_max

def render_chart(product, history, snapshot, window_minutes, include_fragmented, y_range, end_time):
    # Filter history for this product (time series of precomputed bests)
    p_history = history[history['Product'] == product]

    # Compute included orders from snapshot according to inclusion rule
    if snapshot is not None and not snapshot.empty:
        # product start and end
        p_start = product
        p_end = p_start + datetime.timedelta(minutes=int(window_minutes))

        if include_fragmented:
            # include orders that overlap the product window
            mask = (snapshot['DeliveryStart'] < p_end) & (snapshot['DeliveryEnd'] > p_start)
        else:
            # include only orders that fully contain the product window
            mask = (snapshot['DeliveryStart'] <= p_start) & (snapshot['DeliveryEnd'] >= p_end)

        p_orders = snapshot.loc[mask]
    else:
        p_orders = pd.DataFrame()

    # Get current best bid/ask from included orders
    if not p_orders.empty:
        bids = p_orders[p_orders['Side'] == 'BUY']
        asks = p_orders[p_orders['Side'] == 'SELL']
        curr_bid = bids['Price'].max() if not bids.empty else None
        curr_ask = asks['Price'].min() if not asks.empty else None
    else:
        curr_bid = None
        curr_ask = None
        
    # Create Plotly Chart
    fig = go.Figure()
    
    # We want to plot the step-line of Bid and Ask
    # We need to ensure the line continues to the current replay_time
    if not p_history.empty:
        # Add a point at replay_time with the last value to extend the line
        last_row = p_history.iloc[[-1]].copy()
        last_row['Time'] = end_time
        plot_data = pd.concat([p_history, last_row])
        
        fig.add_trace(go.Scatter(
            x=plot_data['Time'], 
            y=plot_data['BestBid'],
            mode='lines',
            name='Best Bid',
            line=dict(color='green', shape='hv')
        ))
        
        fig.add_trace(go.Scatter(
            x=plot_data['Time'], 
            y=plot_data['BestAsk'],
            mode='lines',
            name='Best Ask',
            line=dict(color='red', shape='hv')
        ))
        
    fig.update_layout(
        yaxis=dict(range=y_range),
        title=f"Delivery: {product.strftime('%H:%M')}",
        xaxis_title="Time",
        yaxis_title="Price (EUR)",
        height=300,
        margin=dict(l=0, r=0, t=30, b=0),
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1)
    )
    
    st.plotly_chart(fig, width='stretch')
    
    # Display current metrics below the chart
    m_col1, m_col2, m_col3, m_col4, m_col5 = st.columns(5)
    m_col1.metric("Time to Delivery", f"{int((product - end_time).total_seconds() / 60)} min")
    m_col2.metric("Best Bid", f"{curr_bid:.2f}" if curr_bid is not None else "-")
    m_col3.metric("Buy Volume", f"{p_orders[p_orders['Side']=='BUY']['Quantity'].sum()}" if not p_orders.empty else "0")
    m_col4.metric("Best Ask", f"{curr_ask:.2f}" if curr_ask is not None else "-")
    m_col5.metric("Sell Volume", f"{p_orders[p_orders['Side']=='SELL']['Quantity'].sum()}" if not p_orders.empty else "0")

    # Small expander to show included orders for this product
    with st.expander("Included orders (snapshot)"):
        if not p_orders.empty:
            display_cols = ['Price', 'Quantity', 'Side', 'DeliveryStart', 'DeliveryEnd', 'ActionCode', 'TransactionTime']
            st.dataframe(p_orders[display_cols].sort_values(['Price'], ascending=False))
        else:
            st.write("No included orders for this delivery window.")

    st.divider()

def render_fragmented_controls():
    st.sidebar.checkbox(
        "Include fragmented orders (partial overlap)",
        value=st.session_state.include_fragmented,
        key='include_fragmented_checkbox',
        on_change=update_fragmented_callback
    )
    return st.session_state.include_fragmented

# --- Main ---
def main():
    st.title("Power Trading Replay Engine")
    
    with st.spinner("Loading data and precomputing state..."):
        engine = load_engine()
        
    min_time = engine.min_time.to_pydatetime()
    max_time = engine.max_time.to_pydatetime()

    # Ensure UTC timezone for min/max_time if not present
    if min_time.tzinfo is None:
        min_time = min_time.replace(tzinfo=datetime.timezone.utc)
    if max_time.tzinfo is None:
        max_time = max_time.replace(tzinfo=datetime.timezone.utc)
    
    init_session_state(min_time, max_time)
    
    start_time, end_time = render_time_controls(min_time, max_time)
    
    delivery_window_minutes = st.sidebar.slider(
        "Delivery Window Size (minutes)",
        min_value=15,
        max_value=60,
        value=60,
        step=15,
    )

    # Get Ticker History for the selected range
    ticker_df = engine.ticker_df
    history = ticker_df[(ticker_df['Time'] >= start_time) & (ticker_df['Time'] <= end_time)]

    available_products = get_available_products(engine, history, delivery_window_minutes)
    selected_products = render_product_selector(available_products)
    
    include_fragmented = render_fragmented_controls()
    y_range = render_y_axis_controls(history, selected_products, start_time, end_time)

    # Main Content
    st.subheader(f"Market State from {start_time.strftime('%Y-%m-%d %H:%M:%S')} to {end_time.strftime('%Y-%m-%d %H:%M:%S')}")

    # Snapshot of active orders at the end of the selected range
    snapshot = engine.get_snapshot(end_time)

    cols = st.columns(2) # 2 columns grid
    for idx, product in enumerate(selected_products):
        col = cols[idx % 2]
        with col:
            render_chart(product, history, snapshot, delivery_window_minutes, include_fragmented, y_range, end_time)

if __name__ == "__main__":
    main()