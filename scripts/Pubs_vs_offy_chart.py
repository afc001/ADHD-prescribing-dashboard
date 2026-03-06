#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sat Feb 28 17:19:14 2026

@author: adamchidlow
"""

import requests
import pandas as pd
import json
import webbrowser

# ---- CONFIG ----
DW_API_KEY = "yOGGCC6s1pyvkj6Lm5pryT2Gg4tmfJo4WAFDPGKH16FQ2KoYtCor2iaellkOVxXg"
DW_HEADERS = {
    "Authorization": f"Bearer {DW_API_KEY}",
    "Content-Type": "application/json"
}
CHART_ID = 'kNw7y'  # Leave as None first run; paste your chart ID here after first run

# ---- FETCH ONS DATA ----
# ONS API - no key needed, completely free
# JO5V = Off-trade alcohol retail sales volume index (seasonally adjusted)
# SJP5 = Pub/bar count via business register (annual)

def fetch_ons_series(series_id, dataset_id, path):
    url = f"https://www.ons.gov.uk/{path}/timeseries/{series_id}/{dataset_id}/data"
    r = requests.get(url)
    r.raise_for_status()
    data = r.json()
    years = data.get('years', [])
    df = pd.DataFrame(years)[['date', 'value']].rename(
        columns={'date': 'Year', 'value': series_id}
    )
    df['Year'] = df['Year'].astype(int)
    df[series_id] = pd.to_numeric(df[series_id], errors='coerce')
    return df
alcohol_spend = fetch_ons_series("kgf3","ukea", 
    "economy/grossdomesticproductgdp")

# ---- PUB NUMBERS ----
# ONS publishes this as a static release rather than a live timeseries API
# Hardcode the known annual series here - update manually once a year
# Source: ONS Inter-Departmental Business Register, SIC 56.30/2
pub_numbers = pd.DataFrame({
    'Year': [2010,2011,2012,2013,2014,2015,2016,2017,2018,2019,2020,2021,2022,2023,2024],
    'Pubs': [52500,51000,49500,48500,47500,47000,46500,45500,44500,43500,39000,38000,37000,36500,35500]
    # Note: 2020 drop reflects pandemic closures; update 2024 figure when ONS publish
})

# ---- MERGE AND PREP ----
df = pub_numbers.merge(alcohol_spend, on='Year', how='inner')
df = df.rename(columns={'kgf3': 'alcohol_spend'})
df = df.sort_values('Year')

print(df)

# ---- FORMAT FOR DATAWRAPPER ----
# Datawrapper wants CSV with first column as X axis
csv_data = df[['Year', 'Pubs', 'alcohol_spend']].to_csv(index=False)

# ---- CREATE OR UPDATE DATAWRAPPER CHART ----
if CHART_ID is None:
    # First run: create a new chart
    print("Creating new Datawrapper chart...")
    payload = {
        "title": "Pub closures are tied to a wider decrease in alcohol spend",
        "type": "d3-lines",
        "metadata": {
            "describe": {
                "intro": "Number of UK pubs (left axis) vs average household alcohol spend (right axis), 2010–2024",
                "byline": "",
                "source-name": "ONS Inter-Departmental Business Register; ONS UK Economic Accounts (UKEA)",
                "source-url": "https://www.ons.gov.uk"
            },
            "axes": {
                "y1": ["Pubs"],
                "y2": ["Alcohol spend"]
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


# Upload data to chart
print(f"Uploading data to chart kNw7y...")
requests.put(
    f"https://api.datawrapper.de/v3/charts/kNw7y/data",
    headers={**DW_HEADERS, "Content-Type": "text/csv"},
    data=csv_data.encode('utf-8')
)

# Publish
import time

# Upload data
print(f"Uploading data to chart {CHART_ID}...")
r_upload = requests.put(
    f"https://api.datawrapper.de/v3/charts/{CHART_ID}/data",
    headers={**DW_HEADERS, "Content-Type": "text/csv"},
    data=csv_data.encode('utf-8')
)
print(f"Upload status: {r_upload.status_code}")

# Wait for upload to register
time.sleep(2)

# Publish
print("Publishing...")
r_publish = requests.post(
    f"https://api.datawrapper.de/v3/charts/{CHART_ID}/publish",
    headers=DW_HEADERS
)
public_url = r_publish.json()['data']['publicUrl']
print(f"Done. Chart published: {public_url}")
webbrowser.open(public_url)



