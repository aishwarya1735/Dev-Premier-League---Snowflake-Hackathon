from snowflake.snowpark import Session
from snowflake.snowpark.functions import col, avg, max as sf_max, min as sf_min, count, coalesce, lit, max_by

connection_parameters = {
    "account": "FHCUOSH-QR53339",  
    "user": "Aishwarya",
    "password": "Aishwarya_Snowflake1735",
    "role": "SYSADMIN",
    "warehouse": "COMPUTE_WH",
    "database": "HACKATHON_DB",
    "schema": "MARKET_DATA"
}

session = Session.builder.configs(connection_parameters).create()
print("✅ Connected to Snowflake successfully!")
news_table = "NEWS_ARTICLES_SENTIMENT"
stock_table = "STOCK_PRICES"

news_df = session.table(news_table)
stock_df = session.table(stock_table)
print(f"✅ Loaded tables: {news_table}, {stock_table}")
sentiment_features = (
    news_df.group_by("SYMBOL")
           .agg(coalesce(avg(col("SENTIMENT_SCORE")), lit(0)).alias("AVG_SENTIMENT"))
)
print("✅ Sentiment features calculated")
stock_features = (
    stock_df.with_column("DAILY_RETURN", (col("CLOSE") - col("OPEN")) / col("OPEN"))
             .group_by("SYMBOL")
             .agg(
                 coalesce(avg(col("DAILY_RETURN")), lit(0)).alias("AVG_DAILY_RETURN"),
                 sf_max(col("DAILY_RETURN")).alias("MAX_DAILY_RETURN"),
                 sf_min(col("DAILY_RETURN")).alias("MIN_DAILY_RETURN"),
                 max_by(col("CLOSE"), col("TS")).alias("LAST_CLOSE_PRICE")  # latest closing price
             )
)
print("✅ Stock features calculated")
features = stock_features.join(sentiment_features, "SYMBOL", "left")
print("✅ Features joined successfully")

local_csv_path = "stock_sentiment_features.csv"
features.to_pandas().to_csv(local_csv_path, index=False)
print(f"✅ Features saved to local CSV: {local_csv_path}")

session.close()
print("✅ Snowflake session closed")
