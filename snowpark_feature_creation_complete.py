    # "account": "FHCUOSH-QR53339",   # your SF account
    # "user": "Aishwarya",
    # "password": "Aishwarya_Snowflake1735",
    # "role": "SYSADMIN",
    # "warehouse": "COMPUTE_WH",
    # "database": "HACKATHON_DB",
    # "schema": "MARKET_DATA"

from snowflake.snowpark import Session
from snowflake.snowpark.functions import col, avg, coalesce, lit

# Connection parameters (replace with your details)
connection_parameters = {
    "account": "FHCUOSH-QR53339",   # your SF account
    "user": "Aishwarya",
    "password": "Aishwarya_Snowflake1735",
    "role": "SYSADMIN",
    "warehouse": "COMPUTE_WH",
    "database": "HACKATHON_DB",
    "schema": "MARKET_DATA"
}

session = Session.builder.configs(connection_parameters).create()
print("✅ Connected to Snowflake successfully!")

# Load tables
news_df = session.table("NEWS_ARTICLES_SENTIMENT")
stock_df = session.table("STOCK_PRICES")
print("✅ Loaded tables: NEWS_ARTICLES_SENTIMENT, STOCK_PRICES")

# Sentiment features
sentiment_features = (
    news_df.group_by("SYMBOL")
           .agg(coalesce(avg(col("SENTIMENT_SCORE")), lit(0)).alias("AVG_SENTIMENT"))
)
print("✅ Sentiment features calculated")

# Stock features (use exact column names from table)
stock_features = (
    stock_df.with_column("DAILY_RETURN", (col("CLOSE") - col("OPEN")) / col("OPEN"))
             .group_by("SYMBOL")
             .agg(coalesce(avg(col("DAILY_RETURN")), lit(0)).alias("AVG_DAILY_RETURN"))
)
print("✅ Stock features calculated")

# Join features
features = stock_features.join(sentiment_features, "SYMBOL", "left")
print("✅ Features joined successfully")

# Show final features
features.show()
print("✅ Feature creation completed! Data is in session DataFrame.")

# Close session
session.close()
print("✅ Snowflake session closed")

