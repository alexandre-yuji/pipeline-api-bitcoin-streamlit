import streamlit as st
import psycopg2
import pandas as pd
import os
from dotenv import load_dotenv


load_dotenv()

DATABASE_URL = os.getenv("DATABASE_KEY")

def read_data_postgres():
    """Read data from PostgreSQL and return a DataFrame."""
    try:
        conn = psycopg2.connect(DATABASE_URL)
        query = "SELECT * FROM bitcoin_dados ORDER BY timestamp DESC"
        df = pd.read_sql(query, conn)
        conn.close()
        return df
    except Exception as e:
        st.error(f"Error trying to connect to PostgresL: {e}")
        return pd.DataFrame()

def main():
    st.set_page_config(page_title="Dashboard Bitcoin Price", layout="wide")
    st.title("📊 Dashboard Bitcoin Price")
    st.write("This dashboard displays Bitcoin price data collected periodically in a PostgreSQL database.")

    df = read_data_postgres()

    if not df.empty:
        st.subheader("📋 Recent Data")
        st.dataframe(df)

        df['timestamp'] = pd.to_datetime(df['timestamp'])
        df = df.sort_values(by='timestamp')
        
        st.subheader("📈 Bitcoin Price Evolution")
        st.line_chart(data=df, x='timestamp', y='price', use_container_width=True)

        st.subheader("🔢 General Statistics")
        col1, col2, col3 = st.columns(3)
        col1.metric("Current Price", f"${df['price'].iloc[-1]:,.2f}")
        col2.metric("Max Price", f"${df['price'].max():,.2f}")
        col3.metric("Min Price", f"${df['price'].min():,.2f}")
    else:
        st.warning("No data found in the PostgreSQL database.")

if __name__ == "__main__":
    main()