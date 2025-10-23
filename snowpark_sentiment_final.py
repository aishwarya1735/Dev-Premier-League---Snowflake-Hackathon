    # "account": "FHCUOSH-QR53339",   # your SF account
    # "user": "Aishwarya",
    # "password": "Aishwarya_Snowflake1735",
    # "role": "SYSADMIN",
    # "warehouse": "COMPUTE_WH",
    # "database": "HACKATHON_DB",
    # "schema": "MARKET_DATA"

# snowpark_sentiment_final.py

from snowflake.snowpark import Session
from snowflake.snowpark.functions import col
from snowflake.snowpark.types import StructType, StructField, StringType, FloatType, TimestampType
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
import pandas as pd

# ---------------------------
# 1️⃣ Connect to Snowflake
# ---------------------------
session = Session.builder.configs({
    "account": "FHCUOSH-QR53339",   # your SF account
    "user": "Aishwarya",
    "password": "Aishwarya_Snowflake1735",
    "role": "SYSADMIN",
    "warehouse": "COMPUTE_WH",
    "database": "HACKATHON_DB",
    "schema": "MARKET_DATA"
}).create()

print("✅ Connected to Snowflake successfully!")

# ---------------------------
# 2️⃣ Fetch NEWS_ARTICLES
# ---------------------------
news_df = session.table("NEWS_ARTICLES").to_pandas()

if news_df.empty:
    print("⚠️ No news articles found. Exiting...")
    session.close()
    exit()

print(f"Fetched {len(news_df)} news articles")

# ---------------------------
# 3️⃣ Sentiment Analysis via Python UDF
# ---------------------------
analyzer = SentimentIntensityAnalyzer()

def sentiment_score(content: str) -> float:
    if not content:
        return 0.0
    return float(analyzer.polarity_scores(str(content))["compound"])

# Register UDF with Snowpark types
from snowflake.snowpark.types import FloatType, StringType

sentiment_udf = session.udf.register(
    sentiment_score,
    return_type=FloatType(),
    input_types=[StringType()]
)

# Add sentiment column
news_df["SENTIMENT_SCORE"] = news_df["CONTENT"].apply(lambda x: sentiment_score(x))
print("✅ Sentiment analysis completed!")

# ---------------------------
# 4️⃣ Write results back to Snowflake
# ---------------------------
# Ensure SYMBOL column exists
if "SYMBOL" not in news_df.columns:
    news_df["SYMBOL"] = "AAPL"

# Define schema
schema = StructType([
    StructField("TITLE", StringType()),
    StructField("CONTENT", StringType()),
    StructField("PUBLISHED_AT", TimestampType()),
    StructField("SYMBOL", StringType()),
    StructField("SENTIMENT_SCORE", FloatType())
])

# Convert to Snowpark DataFrame
snowpark_df = session.create_dataframe(news_df, schema=schema)

# Save results to a new table or overwrite existing
snowpark_df.write.mode("overwrite").save_as_table("NEWS_ARTICLES_SENTIMENT")
print("✅ Sentiment results saved to NEWS_ARTICLES_SENTIMENT")

# ---------------------------
# 5️⃣ Close session
# ---------------------------
session.close()
print("✅ Snowflake session closed")

