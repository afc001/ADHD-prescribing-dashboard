#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sat Feb 28 19:26:05 2026

@author: adamchidlow
"""

import requests
import pandas as pd
import webbrowser
import time

# ---- CONFIG ----
DW_API_KEY = "yOGGCC6s1pyvkj6Lm5pryT2Gg4tmfJo4WAFDPGKH16FQ2KoYtCor2iaellkOVxXg"
DW_HEADERS = {
    "Authorization": f"Bearer {DW_API_KEY}",
    "Content-Type": "application/json"
}
CHART_ID = 'nVnu8'  # Leave as None first run

# ---- FETCH FUNCTION ----
def fetch_ons_annual(series_id):
    url = f"https://www.ons.gov.uk/economy/nationalaccounts/satelliteaccounts/timeseries/{series_id}/ct/data"
    r = requests.get(url)
    r.raise_for_status()
    years = r.json().get('years', [])
    df = pd.DataFrame(years)[['date', 'value']].rename(
        columns={'date': 'Year', 'value': series_id}
    )
    df['Year'] = df['Year'].astype(int)
    df[series_id] = pd.to_numeric(df[series_id], errors='coerce')
    return df[df['Year'] >= 1985]

# ---- FETCH DATA ----
print("Fetching ONS data...")
alcohol_home = fetch_ons_annual("adit")  # Off-trade alcohol CVM NSA
eating_out   = fetch_ons_annual("admk")  # Restaurants & hotels CVM NSA
recreation   = fetch_ons_annual("adjz")  # Recreational & cultural services CVM NSA
alcohol_tobacco = fetch_ons_annual("adis") 

# ---- MERGE ----
df = alcohol_home.merge(eating_out, on='Year', how='inner')
df = df.merge(recreation,           on='Year', how='inner')
df = df.merge(alcohol_tobacco,             on='Year', how='inner')
df = df.rename(columns={
    'adit': 'Alcohol_at_home',
    'admk': 'Eating_and_drinking_out',
    'adjz': 'Recreation_and_culture',
    'adis': 'Alcohol_and_tobacco'
})

# Index to 2019 = 100
base = df[df['Year'] == 2019].iloc[0]
df['Alcohol_at_home']         = (df['Alcohol_at_home']         / base['Alcohol_at_home'])         * 100
df['Eating_and_drinking_out'] = (df['Eating_and_drinking_out'] / base['Eating_and_drinking_out']) * 100
df['Recreation_and_culture']  = (df['Recreation_and_culture']  / base['Recreation_and_culture'])  * 100
df['Alcohol_and_tobacco']        = (df['Alcohol_and_tobacco']        / base['Alcohol_and_tobacco'])        * 100

df = df.sort_values('Year')
print(df.to_string())

# ---- FORMAT FOR DATAWRAPPER ----
df = df[['Year', 'Alcohol_and_tobacco', 'Eating_and_drinking_out', 'Recreation_and_culture', 'Alcohol_at_home']]
csv_data = df.to_csv(index=False)

# ---- CREATE OR UPDATE CHART ----
if CHART_ID is None:
    print("Creating new Datawrapper chart...")
    payload = {
        "title": "How Britons spend on fun, 1985–present",
        "type": "d3-lines",
        "metadata": {
            "describe": {
                "intro": "Household expenditure in real terms (Chain Volume Measure, £ million), not seasonally adjusted",
                "source-name": "ONS Consumer Trends (CT)",
                "source-url": "https://www.ons.gov.uk/economy/nationalaccounts/satelliteaccounts/datasets/consumertrends"
            }
        }
    }
    r = requests.post(
        "https://api.datawrapper.de/v3/charts",
        headers=DW_HEADERS,
        json=payload
    )
    r.raise_for_status()
    CHART_ID = r.json()['id']
    print(f"Chart created. ID: {CHART_ID}")
    print(">>> Paste this ID into CHART_ID at the top of the script <<<")

# ---- UPLOAD DATA ----
print(f"Uploading data to chart {CHART_ID}...")
r_upload = requests.put(
    f"https://api.datawrapper.de/v3/charts/{CHART_ID}/data",
    headers={**DW_HEADERS, "Content-Type": "text/csv"},
    data=csv_data.encode('utf-8')
)
print(f"Upload status: {r_upload.status_code}")

time.sleep(2)

# ---- PUBLISH ----
print("Publishing...")
r_publish = requests.post(
    f"https://api.datawrapper.de/v3/charts/{CHART_ID}/publish",
    headers=DW_HEADERS
)
print(f"Publish status: {r_publish.status_code}")

public_url = r_publish.json()['data']['publicUrl']
print(f"Done. Chart published: {public_url}")
webbrowser.open(public_url)