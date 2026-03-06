#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Mar  4 17:32:56 2026

@author: adamchidlow
"""

"""
Join Script: ADHD Prescription Growth x NHS Talking Therapies Access Rate
==========================================================================
Computes prescription growth over three windows and correlates each
against the current 6-week Talking Therapies access rate by ICB.

Outputs:
  - scatter_data.csv   — one row per ICB with all growth windows + access rate
"""

import pandas as pd
import re

# ── PATHS ─────────────────────────────────────────────────────────────────────
PRESCRIPTIONS   = "/Users/adamchidlow/Desktop/Spectator/Data/nhsbsa_processed/nhsbsa_regional_standardised.csv"
TALKING_THERAPY = "/Users/adamchidlow/Desktop/Spectator/Scripts/icb_latest.csv"
OUTPUT          = "/Users/adamchidlow/Desktop/Spectator/Data/nhsbsa_processed/scatter_data.csv"

# ── GROWTH WINDOWS ────────────────────────────────────────────────────────────
WINDOWS = {
    "10yr": ("2015/2016", "2024/2025"),
    "5yr":  ("2019/2020", "2024/2025"),
    "3yr":  ("2021/2022", "2024/2025"),
    "1yr":  ("2024/2025", "2024/2025"),   # single year — uses absolute value, not growth
}

# ── LOAD ──────────────────────────────────────────────────────────────────────
rx = pd.read_csv(PRESCRIPTIONS, low_memory=False)
tt = pd.read_csv(TALKING_THERAPY, low_memory=False)

print(f"Prescriptions: {len(rx):,} rows | FYs: {sorted(rx['financial_year'].unique())}")
print(f"Talking therapies: {len(tt):,} rows")

# ── NORMALISE ICB NAMES ───────────────────────────────────────────────────────
def normalise_icb(name):
    s = str(name).upper()
    s = re.sub(r"\bNHS\b", "", s)
    s = re.sub(r"\bINTEGRATED CARE BOARD\b", "", s)
    s = re.sub(r"\bICB\b", "", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s

rx["icb_key"] = rx["icb_name"].apply(normalise_icb)
tt["icb_key"] = tt["ICB_NAME"].apply(normalise_icb)

# Drop non-ICB rows from talking therapies
non_icb_patterns = ["COMMISSIONING HUB", "NATIONAL COMMISSIONING", "Unknown"]
for pat in non_icb_patterns:
    tt = tt[~tt["ICB_NAME"].str.contains(pat, na=False)]
print(f"Talking therapies after filtering: {len(tt)} ICBs")

# ── BUILD GROWTH METRICS FOR EACH WINDOW ─────────────────────────────────────
growth_frames = []

for label, (fy_start, fy_end) in WINDOWS.items():

    start = rx[rx["financial_year"] == fy_start][
        ["icb_key", "patients_per_100k", "items_per_100k"]
    ].rename(columns={
        "patients_per_100k": "patients_start",
        "items_per_100k":    "items_start",
    })

    end = rx[rx["financial_year"] == fy_end][
        ["icb_key", "patients_per_100k", "items_per_100k"]
    ].rename(columns={
        "patients_per_100k": "patients_end",
        "items_per_100k":    "items_end",
    })

    merged = start.merge(end, on="icb_key", how="inner")
    n = len(merged)

    if fy_start == fy_end:
        # Single year: use absolute value as the metric
        merged[f"patients_growth_pct_{label}"] = merged["patients_end"]
        merged[f"patients_growth_abs_{label}"] = merged["patients_end"]
        print(f"  {label}: {n} ICBs (single year — using absolute patients_per_100k)")
    else:
        merged[f"patients_growth_pct_{label}"] = (
            (merged["patients_end"] - merged["patients_start"])
            / merged["patients_start"] * 100
        ).round(1)
        merged[f"patients_growth_abs_{label}"] = (
            merged["patients_end"] - merged["patients_start"]
        ).round(1)
        print(f"  {label}: {n} ICBs | "
              f"growth range: {merged[f'patients_growth_pct_{label}'].min():.0f}% "
              f"– {merged[f'patients_growth_pct_{label}'].max():.0f}%")

    keep = ["icb_key",
            f"patients_growth_pct_{label}",
            f"patients_growth_abs_{label}"]
    # Also keep start/end for the 10yr window as reference columns
    if label == "10yr":
        keep += ["patients_start", "patients_end"]
        merged = merged.rename(columns={
            "patients_start": "patients_per_100k_2015",
            "patients_end":   "patients_per_100k_2024",
        })
        keep = ["icb_key",
                "patients_per_100k_2015",
                "patients_per_100k_2024",
                f"patients_growth_pct_{label}",
                f"patients_growth_abs_{label}"]

    growth_frames.append(merged[keep])

# Merge all windows into one frame
from functools import reduce
growth = reduce(lambda a, b: a.merge(b, on="icb_key", how="outer"), growth_frames)

# ── JOIN WITH TALKING THERAPIES ───────────────────────────────────────────────
tt_slim = tt[["icb_key", "ICB_NAME", "mean_6wk_pct", "n_months"]].copy()
scatter = growth.merge(tt_slim, on="icb_key", how="inner")

# Add clean label
icb_names = rx[["icb_key","icb_name","icb_code"]].drop_duplicates("icb_key")
scatter = scatter.merge(icb_names, on="icb_key", how="left")
scatter["icb_label"] = (
    scatter["icb_name"]
    .str.replace("INTEGRATED CARE BOARD", "", regex=False)
    .str.replace("NHS ", "", regex=False)
    .str.strip()
    .str.title()
)

print(f"\n✅ Final dataset: {len(scatter)} ICBs")

# ── CORRELATIONS ─────────────────────────────────────────────────────────────
print("\n" + "="*55)
print("  CORRELATION RESULTS")
print("="*55)
print(f"  {'Window':<10} {'Metric':<25} {'r':>7}  {'Interpretation'}")
print(f"  {'-'*10} {'-'*25} {'-'*7}  {'-'*30}")

for label in ["10yr", "5yr"]:
    col = f"patients_growth_pct_{label}"
    clean = scatter.dropna(subset=[col, "mean_6wk_pct"])
    r = clean[col].corr(clean["mean_6wk_pct"])
    interp = "negative ✓" if r < -0.2 else ("positive (?)" if r > 0.2 else "weak")
    print(f"  {label:<10} {'% growth vs 6wk access':<25} {r:>7.3f}  {interp}")

    col_abs = f"patients_growth_abs_{label}"
    r_abs = clean[col_abs].corr(clean["mean_6wk_pct"])
    interp_abs = "negative ✓" if r_abs < -0.2 else ("positive (?)" if r_abs > 0.2 else "weak")
    print(f"  {label:<10} {'abs growth vs 6wk access':<25} {r_abs:>7.3f}  {interp_abs}")

# 1yr: correlate absolute prescribing level (not growth)
col_1yr = "patients_growth_pct_1yr"  # actually absolute value for 1yr
clean_1yr = scatter.dropna(subset=[col_1yr, "mean_6wk_pct"])
r_1yr = clean_1yr[col_1yr].corr(clean_1yr["mean_6wk_pct"])
print(f"  {'1yr':<10} {'abs level vs 6wk access':<25} {r_1yr:>7.3f}  {'negative ✓' if r_1yr < -0.2 else 'weak'}")

# ── PRINT FULL TABLE ──────────────────────────────────────────────────────────
print(f"\n── Full table (sorted by 5yr growth %) ──")
display_cols = [
    "icb_label",
    "patients_per_100k_2015",
    "patients_per_100k_2024",
    "patients_growth_pct_10yr",
    "patients_growth_pct_5yr",
    "patients_growth_abs_5yr",
    "patients_growth_pct_3yr",
    "patients_growth_pct_1yr",
    "mean_6wk_pct",
]
available = [c for c in display_cols if c in scatter.columns]
print(scatter[available].sort_values("patients_growth_pct_5yr", ascending=False).to_string())

# ── SAVE ─────────────────────────────────────────────────────────────────────
scatter.to_csv(OUTPUT, index=False)
print(f"\n✅ Saved {OUTPUT}")
print("\n── Done ──")