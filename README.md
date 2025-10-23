# Dev-Premier-League---Snowflake-Hackathon
Signal Extraction from Market &amp; News Data

The work done so far for this problem statement : 
1. Creating snowflake trail account
2. Access the API Keys from given two dataset sources
3. Using python,in vs code & command line, linked snowflake account to local to make changes.
4. Created schema & table structure in snowflake platform.
4. Ingested the datasets data into the snowflake platform for further analysis.
5. Got the understanding of given existing dataset attributes using basic sql functions on the snowflake platform to perform further analysis.
6. Then, since cortex access is not present in trail account, using snowpark in the local to perform sentiment analysis of news data.
7. Since there was no access to create a new table to store the analysis outputs which can be used further, stored that output data in excel file which is access in the later analysis.
8. Used the sentiment analysis scores & stock data to create feature sets and stored the outputs in another excel csv file.
9. Got some metric analysis of the given data like min, avg & max daily returns, last close price, avg sentiment score etc

- Problem Statement link : https://vision.hack2skill.com/event/gcc-dev-premier-league-2025/dashboard/challenges?utm_source=hack2skill&utm_medium=homepage&utm_campaign=&utm_term=&utm_content=

- Snowflake trail account link : https://app.snowflake.com/me-central2.gcp/ex77894/#/workspaces/ws/USER%24/PUBLIC/DEFAULT%24
- Snowflake Dashboard link : https://app.snowflake.com/me-central2.gcp/ex77894/#/dev-premier-league-hackathon-analysis-dPGTDHVsn
- Stock Dataset API key access : https://www.alphavantage.co/support/#api-key
- News Dataset API key access : https://newsapi.org/account
