from facebook_business.adobjects.adaccount import AdAccount
from facebook_business.api import FacebookAdsApi
import json

# Set your access credentials
access_token = 'XXX'
app_secret = 'XXX'
app_id = '490189623364204'
FacebookAdsApi.init(access_token=access_token)

# Define the fields
fields = [
    'impressions',
    'spend',
    'campaign_name',
    'campaign_id',
    'actions'
]

# Define the parameters
params = {
    'time_range': {'since': begin_date, 'until': ending_date},
    'level': 'campaign',
    'breakdowns': [],
}

# Fetch insights with pagination
all_insights_campaign = []
cursor = AdAccount(ad_account_id).get_insights(
    fields=fields,
    params=params,
)

page_count = 0
while True:
    for entry in cursor:
        data = entry.export_all_data()
        # Extract link_clicks as Clicks or set to None
        if 'actions' in data:
            link_clicks = next((action['value'] for action in data['actions'] if action['action_type'] == 'link_click'), None)
        else:
            link_clicks = None
        data['clicks'] = link_clicks
        # Remove the actions field if it exists
        if 'actions' in data:
            del data['actions']
        all_insights_campaign.append(data)
    page_count += 1
    print(f"Processed page {page_count}, total records so far: {len(all_insights_campaign)}")
    if cursor.load_next_page():
        cursor = cursor.load_next_page()
    else:
        break

# Check if insights data is empty
if not all_insights_campaign:
    print("No data returned. Check the date range, fields, and permissions.")
else:
    # Print all insights data as JSON
    print(json.dumps(all_insights_campaign, indent=4))

from facebook_business.adobjects.adaccount import AdAccount
from facebook_business.api import FacebookAdsApi
import json

# Set your access credentials
access_token = 'XXX'
ad_account_id = 'act_450690052572589'
app_secret = 'XXX'
app_id = '490189623364204'
FacebookAdsApi.init(access_token=access_token)

# Define the fields
fields = [
    'impressions',
    'spend',
    'adset_name',
    'adset_id',
    'actions',
    'campaign_id'
]

# Define the parameters
params = {
    'time_range': {'since': begin_date, 'until': ending_date},
    'level': 'adset',
    'breakdowns': [],
}

# Fetch insights with pagination
all_insights_adset = []
cursor = AdAccount(ad_account_id).get_insights(
    fields=fields,
    params=params,
)

page_count = 0
while True:
    for entry in cursor:
        data = entry.export_all_data()
        # Extract link_clicks as Clicks or set to None
        if 'actions' in data:
            link_clicks = next((action['value'] for action in data['actions'] if action['action_type'] == 'link_click'), None)
        else:
            link_clicks = None
        data['clicks'] = link_clicks
        # Remove the actions field if it exists
        if 'actions' in data:
            del data['actions']
        all_insights_adset.append(data)
    page_count += 1
    print(f"Processed page {page_count}, total records so far: {len(all_insights_adset)}")
    if cursor.load_next_page():
        cursor = cursor.load_next_page()
    else:
        break

# Check if insights data is empty
if not all_insights_adset:
    print("No data returned. Check the date range, fields, and permissions.")
else:
    # Print all insights data as JSON
    print(json.dumps(all_insights_adset, indent=4))

from facebook_business.adobjects.adaccount import AdAccount
from facebook_business.api import FacebookAdsApi
import json

# Set your access credentials
access_token = 'XXX'
ad_account_id = 'act_450690052572589'
app_secret = 'XXX'
app_id = '490189623364204'
FacebookAdsApi.init(access_token=access_token)

# Define the fields
fields = [
    'impressions',
    'spend',
    'ad_name',
    'ad_id',
    'actions',
    'campaign_id',
    'adset_id'
]

# Define the parameters
params = {
    'time_range': {'since': begin_date, 'until': ending_date},
    'level': 'ad',
    'breakdowns': [],
}

# Fetch insights with pagination
all_insights_ad = []
cursor = AdAccount(ad_account_id).get_insights(
    fields=fields,
    params=params,
)

page_count = 0
while True:
    for entry in cursor:
        data = entry.export_all_data()
        # Extract link_clicks as Clicks or set to None
        if 'actions' in data:
            link_clicks = next((action['value'] for action in data['actions'] if action['action_type'] == 'link_click'), None)
        else:
            link_clicks = None
        data['clicks'] = link_clicks
        # Remove the actions field if it exists
        if 'actions' in data:
            del data['actions']
        all_insights_ad.append(data)
    page_count += 1
    print(f"Processed page {page_count}, total records so far: {len(all_insights_ad)}")
    if cursor.load_next_page():
        cursor = cursor.load_next_page()
    else:
        break

# Check if insights data is empty
if not all_insights_ad:
    print("No data returned. Check the date range, fields, and permissions.")
else:
    # Print all insights data as JSON
    print(json.dumps(all_insights_ad, indent=4))

import pandas as pd

# Convert JSON data to pandas DataFrames
df_campaigns = pd.DataFrame(all_insights_campaign)
df_adsets = pd.DataFrame(all_insights_adset)
df_ads = pd.DataFrame(all_insights_ad)

# Merge the DataFrames
df_merged = df_campaigns.merge(df_adsets, on="campaign_id", suffixes=('_campaign', '_adset'))
df_merged = df_merged.merge(df_ads, on="adset_id", suffixes=('', '_ad'))

# Add the 'date' column with yesterday's date
df_merged['date'] = begin_date

#display(df_merged.head(5))

# Select and rename columns to match the desired output
df_final = df_merged[['campaign_name', 'adset_name', 'ad_name', 'impressions', 'clicks', 'spend','date']]
df_final.columns = ['Campaign', 'Ad_Set', 'Ad', 'Impressions', 'Clicks', 'Spend','Date']

# Order the DataFrame by 'Campaign'
df_final = df_final.sort_values(by='Campaign')

# Convert the pandas DataFrame to a Spark DataFrame
spark_df = spark.createDataFrame(df_final)

# Save the Spark DataFrame as a table in the Databricks catalog
spark_df.write.mode("append").saveAsTable("pocn_data.silver.facebook_insights")
