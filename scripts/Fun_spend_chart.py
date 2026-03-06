#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sat Feb 28 18:37:18 2026

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
CHART_ID = 'hqL30'

# ---- FETCH FUNCTION ----
def fetch_ons_annual(series_id):
    """Fetch annual data from ONS Consumer Trends dataset"""
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
alcohol_home = fetch_ons_annual("adfm")
eating_out   = fetch_ons_annual("adif")
recreation   = fetch_ons_annual("adia")
total_spend  = fetch_ons_annual("abjq")

# ---- MERGE AND CALCULATE % ----
df = alcohol_home.merge(eating_out,  on='Year', how='inner')
df = df.merge(recreation,            on='Year', how='inner')
df = df.merge(total_spend,           on='Year', how='inner')

df['Alcohol_at_home_%']          = (df['adfm'] / df['abjq']) * 100
df['Eating_and_drinking_out_%']  = (df['adif'] / df['abjq']) * 100
df['Recreation_and_culture_%']   = (df['adia'] / df['abjq']) * 100

df = df[['Year', 'Alcohol_at_home_%',
         'Eating_and_drinking_out_%',
         'Recreation_and_culture_%']]

print(df.to_string())

# ---- FORMAT FOR DATAWRAPPER ----
csv_data = df.to_csv(index=False)

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