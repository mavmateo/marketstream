import streamlit as st
import pandas as pd
import plotly.graph_objects as go



def dashboard_header(title="Market Dashboard"):
    st.title(title)
    st.markdown("**Live Market Data • TimescaleDB**")

def sidebar_controls(symbols: list[str]):
    
    with st.sidebar:
        st.header("Controls")
        
        symbol = st.selectbox("Select Symbol", options=symbols, index=0)
        
        ohlcv_limit = st.slider("OHLCV Candles", min_value=20, max_value=500, value=120, step=20)
        signals_limit = st.slider("Recent Signals", min_value=5, max_value=50, value=20)
        
        auto_refresh = st.checkbox("Auto-refresh every 10s", value=False)
        
        st.divider()
        return symbol, ohlcv_limit, signals_limit, auto_refresh


def display_ohlcv(df: pd.DataFrame, symbol: str):
    if df.empty:
        st.warning(f"No OHLCV data found for {symbol}")
        return
    
    st.subheader(f"{symbol} — OHLCV")
    
    col1, col2 = st.columns([3, 2])
    
    with col1:
        fig = go.Figure(data=[go.Candlestick(x=df['time'], open=df['open'], high=df['high'], low=df['low'], close=df['close'])])
        st.plotly_chart(fig, use_container_width=True)
    
    with col2:
        st.dataframe(
            df[['time', 'open', 'high', 'low', 'close', 'volume']].head(20),
            use_container_width=True,
            hide_index=True
        )
    
    
    latest = df.iloc[-1]
    st.metric("Latest Close", f"${latest['close']:.4f}", 
              f"{(latest['close'] - df.iloc[-2]['close']):+.4f}")


def display_signals(df: pd.DataFrame, symbol: str):
    if df.empty:
        st.info(f"No AI signals available for {symbol}")
        return
    
    st.subheader(f"Recent AI Signals — {symbol}")
    st.dataframe(
        df[['time', 'signal_type', 'direction', 'confidence', 'predicted_price', 'details']],
        use_container_width=True,
        hide_index=True
    )