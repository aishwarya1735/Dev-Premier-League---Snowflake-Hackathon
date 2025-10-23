# sf_utils.py
import os
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas
from dotenv import load_dotenv

# Load variables from .env
load_dotenv()

def get_sf_conn():
    conn = snowflake.connector.connect(
        user=os.environ["SF_USER"],
        password=os.environ["SF_PASSWORD"],
        account=os.environ["SF_ACCOUNT"],
        warehouse=os.environ["SF_WAREHOUSE"],
        database=os.environ["SF_DATABASE"],
        schema=os.environ["SF_SCHEMA"]
    )
    return conn

def write_df_to_snowflake(df, table_name):
    conn = get_sf_conn()
    try:
        success, nchunks, nrows, _ = write_pandas(conn, df, table_name)
        print(f"Inserted {nrows} rows into {table_name}")
    finally:
        conn.close()
