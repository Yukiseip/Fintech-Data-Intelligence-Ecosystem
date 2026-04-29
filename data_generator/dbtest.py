import os, pandas as pd
from sqlalchemy import create_engine

conn_str = os.environ.get("STREAMLIT_DB_CONNECTION", "")
print("Connection:", conn_str[:80])
engine = create_engine(conn_str)
with engine.connect() as conn:
    df = pd.read_sql("SELECT txn_date, daily_tpv FROM gold.mart_executive_kpis ORDER BY txn_date LIMIT 5", conn)
    print("ROWS:", len(df))
    print(df)
