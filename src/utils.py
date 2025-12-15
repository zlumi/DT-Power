import streamlit as st
from replay_engine import ReplayEngine
from config import FILEPATH

@st.cache_resource
def load_engine():
    engine = ReplayEngine(FILEPATH)
    engine.load_data()
    engine.precompute_ticker()
    return engine
