# snowpark_sentiment_local.py

from snowflake.snowpark import Session
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
# 2️⃣ Pull news articles
# ---------------------------
news_df = session.table("NEWS_ARTICLES").to_pandas()

if news_df.empty:
    print("⚠️ No news articles found. Exiting...")
    session.close()
    exit()

print(f"Fetched {len(news_df)} news articles")

# ---------------------------
# 3️⃣ Run sentiment analysis
# ---------------------------
analyzer = SentimentIntensityAnalyzer()
news_df["SENTIMENT_SCORE"] = news_df["CONTENT"].apply(lambda x: analyzer.polarity_scores(str(x))["compound"])

print("✅ Sentiment analysis completed!")

# ---------------------------
# 4️⃣ Save results locally
# ---------------------------
output_file = "news_articles_sentiment.csv"
news_df.to_csv(output_file, index=False)
print(f"✅ Sentiment results saved locally to {output_file}")

# ---------------------------
# 5️⃣ Close session
# ---------------------------
session.close()
print("✅ Snowflake session closed")
