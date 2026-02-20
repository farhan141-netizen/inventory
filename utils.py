import streamlit as st
from streamlit_gsheets import GSheetsConnection
import pandas as pd

# --- SHARED CLOUD CONNECTION (lazy) ---
_conn = None


def _get_conn():
    """Return the cached Google Sheets connection, creating it on first use."""
    global _conn
    if _conn is None:
        _conn = st.connection("gsheets", type=GSheetsConnection)
    return _conn


def clean_dataframe(df):
    """Removes ghost/unnamed columns and ensures unique headers."""
    if df is None or df.empty:
        return df
    df = df.loc[:, ~df.columns.str.contains('^Unnamed', na=False)]
    df = df.dropna(axis=1, how='all')
    df = df.loc[:, ~df.columns.duplicated()]
    df.columns = [str(col).strip() for col in df.columns]
    return df


def load_from_sheet(worksheet_name, default_cols=None):
    """Safely load and clean data from Google Sheets.

    Returns an empty DataFrame (with optional default columns) on any error
    or when the sheet is empty.
    """
    try:
        df = _get_conn().read(worksheet=worksheet_name, ttl="2s")
        df = clean_dataframe(df)
        if df is None or df.empty:
            return pd.DataFrame(columns=default_cols) if default_cols else pd.DataFrame()
        return df
    except Exception:
        return pd.DataFrame(columns=default_cols) if default_cols else pd.DataFrame()


def save_to_sheet(df, worksheet_name):
    """Save cleaned data to Google Sheets and clear Streamlit cache."""
    df = clean_dataframe(df)
    _get_conn().update(worksheet=worksheet_name, data=df)
    st.cache_data.clear()
