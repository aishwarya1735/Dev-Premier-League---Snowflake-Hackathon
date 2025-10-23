from snowflake.snowpark import Session
from snowflake.snowpark.functions import col, avg, coalesce, lit

# ----------------------------
# 1. Connection parameters
# ----------------------------
connection_parameters = {
    "account": "FHCUOSH-QR53339",   # your SF account
    "user": "Aishwarya",
    "password": "Aishwarya_Snowflake1735",
    "role": "SYSADMIN",
    "warehouse": "COMPUTE_WH",
    "database": "HACKATHON_DB",
    "schema": "MARKET_DATA"
}

# ----------------------------
# 2. Create Snowflake session
# ----------------------------
session = Session.builder.configs(connection_parameters).create()
print("✅ Connected to Snowflake successfully!")

# ----------------------------
# 3. Load tables
# ----------------------------
news_table = "NEWS_ARTICLES_SENTIMENT"
stock_table = "STOCK_PRICES"

news_df = session.table(news_table)
stock_df = session.table(stock_table)
print(f"✅ Loaded tables: {news_table}, {stock_table}")

# ----------------------------
# 4. Create sentiment features
# ----------------------------
sentiment_features = (
    news_df.group_by("SYMBOL")
           .agg(coalesce(avg(col("SENTIMENT_SCORE")), lit(0)).alias("AVG_SENTIMENT"))
)
print("✅ Sentiment features calculated")

# ----------------------------
# 5. Create stock features
# ----------------------------
stock_features = (
    stock_df.with_column("DAILY_RETURN", (col("CLOSE") - col("OPEN")) / col("OPEN"))
             .group_by("SYMBOL")
             .agg(coalesce(avg(col("DAILY_RETURN")), lit(0)).alias("AVG_DAILY_RETURN"))
)
print("✅ Stock features calculated")

# ----------------------------
# 6. Join features
# ----------------------------
features = stock_features.join(sentiment_features, "SYMBOL", "left")
print("✅ Features joined successfully")

# ----------------------------
# 7. Save locally as CSV
# ----------------------------
features_pd = features.to_pandas()
features_pd.to_csv("stock_sentiment_features.csv", index=False)
print("✅ Features saved locally as 'stock_sentiment_features.csv'")

# ----------------------------
# 8. Close session
# ----------------------------
session.close()
print("✅ Snowflake session closed")
