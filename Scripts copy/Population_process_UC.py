#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Mar  2 16:58:29 2026

@author: adamchidlow
"""

import pandas as pd

# === CONFIGURATION ===
INPUT_FILE = "/Users/adamchidlow/Desktop/Spectator/Data/UC claimants/population_file.xlsx"  # Change this

# === LOAD DATA ===
df = pd.read_excel(INPUT_FILE, header=4, engine="openpyxl")


# Filter to mid-2024, England and Great Britain only
df = df[df["Year_ending_30_June"] == 2024]
df = df[df["Area_name"].isin(["England", "Great Britain"])]
df = df[df["Sex"].isin(["Male", "Female"])]

# === BUILD AGE COLUMNS MAPPING TO 5-YEAR BANDS ===
age_bands = {
    "15-19": [f"Aged_{i}_years" for i in range(15, 20)],
    "20-24": [f"Aged_{i}_years" for i in range(20, 25)],
    "25-29": [f"Aged_{i}_years" for i in range(25, 30)],
    "30-34": [f"Aged_{i}_years" for i in range(30, 35)],
    "35-39": [f"Aged_{i}_years" for i in range(35, 40)],
    "40-44": [f"Aged_{i}_years" for i in range(40, 45)],
    "45-49": [f"Aged_{i}_years" for i in range(45, 50)],
    "50-54": [f"Aged_{i}_years" for i in range(50, 55)],
    "55-59": [f"Aged_{i}_years" for i in range(55, 60)],
    "60-64": [f"Aged_{i}_years" for i in range(60, 65)],
    "65-70": [f"Aged_{i}_years" for i in range(65, 71)],
    "71+":   [f"Aged_{i}_years" for i in range(71, 90)] + ["Aged_90_years_and_over"],
}

# Verify columns exist
actual_cols = df.columns.tolist()
print("Sample age columns found:", [c for c in actual_cols if c.startswith("Aged")][:5])

# === AGGREGATE INTO BANDS ===
rows = []
for _, row in df.iterrows():
    area = row["Area_name"]
    sex = row["Sex"]
    
    for band_name, cols in age_bands.items():
        total = 0
        missing_cols = []
        for col in cols:
            if col in actual_cols:
                total += row[col]
            else:
                missing_cols.append(col)
        
        if missing_cols:
            print(f"Warning: missing columns for {band_name}: {missing_cols}")
        
        rows.append({
            "Area": area,
            "Sex": sex,
            "Age_band": band_name,
            "Population": total
        })

result = pd.DataFrame(rows)

# === PIVOT AND DISPLAY ===
for area in result["Area"].unique():
    print(f"\n{'='*50}")
    print(f"Mid-2024 Population: {area}")
    print(f"{'='*50}")
    
    pivot = result[result["Area"] == area].pivot_table(
        index="Age_band", 
        columns="Sex", 
        values="Population",
        sort=False
    )
    pivot = pivot.reindex(list(age_bands.keys()))
    pivot.loc["TOTAL"] = pivot.sum()
    print(pivot.to_string())

# === SAVE ===
OUTPUT_FILE = "population_by_age_band_and_sex_2024.csv"
result.to_csv(OUTPUT_FILE, index=False)
print(f"\nSaved to {OUTPUT_FILE}")

# === GB/ENGLAND RATIO ===
eng = result[result["Area"] == "England"]["Population"].sum()
gb = result[result["Area"] == "Great Britain"]["Population"].sum()
if eng > 0 and gb > 0:
    ratio = gb / eng
    print(f"\nGB/England ratio (ages 15+): {ratio:.4f}")
    print(f"Multiply England figures by {ratio:.4f} to scale to GB")