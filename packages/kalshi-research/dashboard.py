"""Optional Streamlit reporting scaffold; it only reads paper-trading data."""
import streamlit as st

st.set_page_config(page_title="Kalshi Research", layout="wide")
st.title("Kalshi Research — Paper Trading")
st.warning("Research mode only. This dashboard cannot place live orders.")
st.metric("Status", "Scaffold ready")
st.caption("Connect this view to the PostgreSQL-ready paper_trades and market_snapshots tables as ingestion is enabled.")
