#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sat Feb 28 18:47:41 2026

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
CHART_ID = 'WFQe1'  # Leave as None first run

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

def fetch_cpi():
    url = "https://www.ons.gov.uk/economy/inflationandpriceindices/timeseries/czmt/mm23/data"
    r = requests.get(url)
    r.raise_for_status()
    years = r.json().get('years', [])
    df = pd.DataFrame(years)[['date', 'value']].rename(
        columns={'date': 'Year', 'value': 'cpi'}
    )
    df['Year'] = df['Year'].astype(int)
    df['cpi'] = pd.to_numeric(df['cpi'], errors='coerce')
    # Drop any zero or null values which cause division errors
    df = df[df['cpi'] > 0]
    return df[df['Year'] >= 1985]

# ---- FETCH DATA ----
print("Fetching ONS data...")
alcohol_home = fetch_ons_annual("adfm")
eating_out   = fetch_ons_annual("adif")
recreation   = fetch_ons_annual("adia")
cpi          = fetch_cpi()
print(cpi.to_string())

# ---- MERGE ----
df = alcohol_home.merge(eating_out, on='Year', how='inner')
df = df.merge(recreation,           on='Year', how='inner')
df = df.merge(cpi,                  on='Year', how='inner')

# ---- ADJUST FOR INFLATION (2023 prices) ----
# Divide each series by CPI then multiply by CPI in base year (2023)
base_cpi = df.loc[df['Year'] == 2023, 'cpi'].values[0]

df['Alcohol_at_home_real']         = (df['adfm'] / df['cpi']) * base_cpi
df['Eating_and_drinking_out_real'] = (df['adif'] / df['cpi']) * base_cpi
df['Recreation_and_culture_real']  = (df['adia'] / df['cpi']) * base_cpi

df = df[['Year', 'Alcohol_at_home_real',
         'Eating_and_drinking_out_real',
         'Recreation_and_culture_real']]

print(df.to_string())

# ---- FORMAT FOR DATAWRAPPER ----
csv_data = df.to_csv(index=False)

# ---- CREATE NEW CHART ----
if CHART_ID is None:
    print("Creating new Datawrapper chart...")
    payload = {
        "title": "How Britons spend on fun, in real terms (2023 prices)",
        "type": "d3-lines",
        "metadata": {
            "describe": {
                "intro": "Household expenditure adjusted for inflation, £ million in 2023 prices",
                "source-name": "ONS Consumer Trends; ONS CPI (D7G7)",
                "source-url": "https://www.ons.gov.uk"
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