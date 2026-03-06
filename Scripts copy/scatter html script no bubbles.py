#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Mar  4 21:02:21 2026

@author: adamchidlow
"""

import pandas as pd

files = {
    "TimeSeries": "/Users/adamchidlow/Desktop/Spectator/Data/NHS_talking_therapies_activity.csv",      # <- replace
}

for label, path in files.items():
    print(f"\n{'='*60}")
    print(f"  {label}")
    print(f"  {path}")
    print(f"{'='*60}")
    try:
        df = pd.read_csv(path, low_memory=False)
        print(f"  Rows: {len(df):,}  |  Cols: {len(df.columns)}")
        print(f"\n  Headers:")
        for col in df.columns:
            print(f"    {col}")
        print(f"\n  First 5 rows:")
        print(df.head().to_string())
    except Exception as e:
        print(f"  ERROR: {e}")

print("\n-- Done --")