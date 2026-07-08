import streamlit as st
import pandas as pd

from dashboard.layout import dashboard_header, display_ohlcv, display_signals, sidebar_controls
from dashboard.data_feeds import get_all_symbols, get_ohlcv, get_signals

from streamlit_autorefresh import st_autorefresh


st.set_page_config(
     page_title = "MarketStream",
    layout     = "wide",
    initial_sidebar_state = "expanded",
)


all_symbols = get_all_symbols()

symbol, ohlcv_limit, signals_limit, auto_refresh  = sidebar_controls(all_symbols)

if auto_refresh:
    st_autorefresh(interval=1000, key="data_refresh")

ohlcv_df = get_ohlcv(symbol , limit=ohlcv_limit)
signals_df = get_signals(symbol, limit=signals_limit)


dashboard_header()
display_ohlcv(ohlcv_df,     symbol)
display_signals(signals_df, symbol)