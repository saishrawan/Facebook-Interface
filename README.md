# Facebook-Interface

The Facebook Ads Data Interface module extracts campaign-level, adset-level, and ad-level performance metrics (impressions, spend, link clicks) via the Facebook Marketing API, merges them into a single table, and writes the result to the Spark table pocn_data.silver.facebook_insights.

[Facebook Marketing API]
         ↓ (REST via facebook_business SDK, pagination)
     [Python Script]
         ↓ (in-memory JSON → pandas DataFrames)
     [pandas merge & transform]
         ↓ (add date column)
     [PySpark conversion]
         ↓ (append)
[Spark Table: pocn_data.silver.facebook_insights]
