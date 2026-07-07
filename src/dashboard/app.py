import streamlit as st
import pandas as pd
from layout import dashboard_header, main_content, sidebar_controls

# Sample data (replace with your real data loading)
@st.cache_data
def load_data():
    dates = pd.date_range(start="2025-01-01", periods=180)
    return pd.DataFrame({
        'date': dates,
        'value': pd.Series(range(180)).rolling(30).mean() + pd.Series(range(180)).sample(frac=1).values * 0.3,
        'category': pd.Series(['A', 'B', 'C', 'D']).sample(180, replace=True).values
    })

# Page config
st.set_page_config(page_title="My Dashboard", layout="wide", initial_sidebar_state="expanded")

# Load data
df = load_data()

# Sidebar controls
date_range, categories, show_raw = sidebar_controls()

# Filter data based on sidebar input
filtered_df = df.copy()  # Add your actual filtering logic here

# Render layout
dashboard_header("Business Dashboard", "Real-time Overview • July 2026")

main_content(filtered_df)

if show_raw:
    st.divider()
    st.subheader("Full Dataset")
    st.dataframe(filtered_df, use_container_width=True)