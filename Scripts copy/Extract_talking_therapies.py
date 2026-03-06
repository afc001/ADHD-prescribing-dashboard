#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Mar  4 16:56:54 2026

@author: adamchidlow
"""

"""
NHS Talking Therapies Monthly Activity Data — Extraction Script
===============================================================
Extracts ICB-level 6-week access rates for scatter chart use.

Outputs:
  1. icb_access_rates_annual.csv   — annual mean access rate per ICB, 2015/16–2024/25
  2. icb_access_rates_latest.csv   — single most recent full year per ICB (for scatter X-axis)
  3. subicb_access_rates.csv       — sub-ICB detail for reference
  4. measure_ids_found.csv         — inventory of all MEASURE_IDs in the file (for QA)
"""

"""
NHS Talking Therapies Monthly Activity Data — Extraction Script (v2)
=====================================================================
Key fixes from v1:
  - Hardcoded to M053 (Percentage_AccessingServices6WeeksFinishedCourseTreatment)
  - No ICB group exists in this file: geography is SubICB and CommissioningRegion only
  - Strategy A: aggregate SubICB rows to ICB using ORG_CODE2/ORG_NAME2
    (in SubICB rows, ORG_CODE2 is the parent ICB)
  - Strategy B: use CommissioningRegion as a coarser fallback
  - Both approaches output to separate CSVs so you can compare

Outputs:
  1. icb_from_subicb_annual.csv    — ICB-level annual mean, aggregated from SubICB
  2. icb_from_subicb_latest.csv    — latest FY only, for scatter X-axis
  3. icb_from_subicb_wide.csv      — wide format (one col per FY)
  4. region_annual.csv             — CommissioningRegion level (coarser)
  5. subicb_annual.csv             — SubICB level for reference
"""

import pandas as pd
import re

INPUT_FILE = "/Users/adamchidlow/Desktop/Spectator/Data/NHS_Talking_Therapies_Activity.csv"   # ← adjust if needed

TARGET_MEASURE_ID = "M053"   # Percentage_AccessingServices6WeeksFinishedCourseTreatment

# ── LOAD & FILTER ─────────────────────────────────────────────────────────────
print("Loading CSV...")
df = pd.read_csv(INPUT_FILE, low_memory=False)
print(f"  Rows: {len(df):,}  |  Columns: {list(df.columns)}")

print(f"\n── GROUP_TYPE counts ──")
print(df["GROUP_TYPE"].value_counts().to_string())

df_m = df[df["MEASURE_ID"] == TARGET_MEASURE_ID].copy()
print(f"\n  Rows for M053: {len(df_m):,}")
print(f"  GROUP_TYPE counts for M053:")
print(df_m["GROUP_TYPE"].value_counts().to_string())

# ── CLEAN VALUE ───────────────────────────────────────────────────────────────
df_m["VALUE"] = pd.to_numeric(
    df_m["MEASURE_VALUE"].astype(str).str.replace("[^0-9.]", "", regex=True),
    errors="coerce"
)
suppressed = df_m["VALUE"].isna().sum()
print(f"\n  Suppressed/unparseable values: {suppressed:,} ({100*suppressed/len(df_m):.1f}%)")

# ── PARSE DATES & FINANCIAL YEAR ─────────────────────────────────────────────
df_m["PERIOD_START"] = pd.to_datetime(df_m["REPORTING_PERIOD_START"], dayfirst=True, errors="coerce")
df_m["FY"] = df_m["PERIOD_START"].apply(
    lambda d: d.year if pd.notna(d) and d.month >= 4 else (d.year - 1 if pd.notna(d) else None)
)
df_m["FY_LABEL"] = df_m["FY"].apply(
    lambda y: f"{int(y)}/{str(int(y)+1)[-2:]}" if pd.notna(y) else None
)

print(f"\n── Financial years present ──")
print(df_m["FY_LABEL"].value_counts().sort_index().to_string())

# ── SubICB only ───────────────────────────────────────────────────────────────
df_sub = df_m[df_m["GROUP_TYPE"] == "SubICB"].copy()
print(f"\n  SubICB rows for M053: {len(df_sub):,}")

# ── EXTRACT ICB from SubICB name ──────────────────────────────────────────────
# Pattern: "NHS GREATER MANCHESTER ICB - 00T"  →  ICB = "NHS GREATER MANCHESTER ICB"
# Also handles "NHS NORTH EAST AND NORTH CUMBRIA ICB - 00L" etc.

def extract_icb_name(subicb_name):
    """Extract the ICB portion from a SubICB name like 'NHS X ICB - CODE'"""
    m = re.match(r"^(.*?ICB)\s*-\s*\w+\s*$", str(subicb_name), re.IGNORECASE)
    if m:
        return m.group(1).strip()
    return subicb_name  # fallback: return as-is

def extract_icb_code(subicb_name):
    """Extract the SubICB code suffix, use first code per ICB as the ICB identifier"""
    m = re.match(r"^.*ICB\s*-\s*(\w+)\s*$", str(subicb_name), re.IGNORECASE)
    if m:
        return m.group(1).strip()
    return None

df_sub["ICB_NAME"] = df_sub["ORG_NAME1"].apply(extract_icb_name)
df_sub["SUBICB_CODE"] = df_sub["ORG_CODE1"]

# Build a mapping from ICB_NAME to a canonical ICB code
# (use ORG_CODE1 of the first SubICB per ICB as a proxy, but we'll replace with
# a proper ICB code lookup below)
icb_name_check = df_sub[["ICB_NAME","ORG_NAME1","ORG_CODE1"]].drop_duplicates()
print(f"\n── ICB names extracted from SubICB names (check for parsing errors) ──")
print(icb_name_check.sort_values("ICB_NAME").to_string())

# ── AGGREGATE: SubICB → ICB, per FY ──────────────────────────────────────────
# Simple mean across sub-ICBs within each ICB for each financial year
# Note: unweighted — see comment below about weighted alternative

icb_monthly = (
    df_sub
    .groupby(["ICB_NAME", "FY_LABEL", "PERIOD_START"], as_index=False)
    .agg(
        mean_6wk_pct=("VALUE", "mean"),
        n_subicbs=("VALUE", "count"),
    )
)

icb_annual = (
    icb_monthly
    .groupby(["ICB_NAME", "FY_LABEL"], as_index=False)
    .agg(
        mean_6wk_pct=("mean_6wk_pct", "mean"),
        n_months=("PERIOD_START", "nunique"),
        n_subicbs=("n_subicbs", "mean"),
    )
    .sort_values(["ICB_NAME", "FY_LABEL"])
)

print(f"\n── ICBs found ──")
print(f"  Unique ICBs: {icb_annual['ICB_NAME'].nunique()}")
print(f"  FY range: {icb_annual['FY_LABEL'].min()} – {icb_annual['FY_LABEL'].max()}")

# Flag incomplete years (< 10 months)
icb_annual["incomplete_year"] = icb_annual["n_months"] < 10
print(f"\n  Rows with <10 months data: {icb_annual['incomplete_year'].sum()}")

icb_annual.to_csv("icb_annual.csv", index=False)
print(f"\n✅ Saved icb_annual.csv  ({len(icb_annual):,} rows)")
print(icb_annual.to_string())

# ── Latest complete FY for scatter ───────────────────────────────────────────
# Find FYs where most ICBs have >=10 months
fy_coverage = icb_annual.groupby("FY_LABEL").apply(
    lambda g: (g["n_months"] >= 10).mean()
).sort_index()
print(f"\n── % ICBs with >=10 months data, by FY ──")
print(fy_coverage.to_string())

complete_fys = fy_coverage[fy_coverage >= 0.8].index  # 80%+ ICBs have full year
latest_complete_fy = complete_fys.max() if len(complete_fys) > 0 else icb_annual["FY_LABEL"].max()
print(f"\n  Selected FY for scatter: {latest_complete_fy}")

icb_latest = (
    icb_annual[icb_annual["FY_LABEL"] == latest_complete_fy]
    .copy()
    .sort_values("mean_6wk_pct")
)
icb_latest.to_csv("icb_latest.csv", index=False)
print(f"\n✅ Saved icb_latest.csv  ({len(icb_latest)} ICBs)")
print(icb_latest[["ICB_NAME","mean_6wk_pct","n_months","n_subicbs"]].to_string())

# ── Range check: min/max access rates ────────────────────────────────────────
print(f"\n── Access rate range for scatter FY ({latest_complete_fy}) ──")
print(f"  Min: {icb_latest['mean_6wk_pct'].min():.1f}% — {icb_latest.loc[icb_latest['mean_6wk_pct'].idxmin(), 'ICB_NAME']}")
print(f"  Max: {icb_latest['mean_6wk_pct'].max():.1f}% — {icb_latest.loc[icb_latest['mean_6wk_pct'].idxmax(), 'ICB_NAME']}")
print(f"  National target: 75%")

# ── Wide format ──────────────────────────────────────────────────────────────
icb_wide = icb_annual.pivot(
    index="ICB_NAME", columns="FY_LABEL", values="mean_6wk_pct"
)
icb_wide.columns.name = None
icb_wide = icb_wide.reset_index()
icb_wide.to_csv("icb_wide.csv", index=False)
print(f"\n✅ Saved icb_wide.csv  ({len(icb_wide)} ICBs × {len(icb_wide.columns)-1} years)")
print(icb_wide.to_string())

# ── SubICB annual (for reference / weighted re-aggregation later) ─────────────
subicb_annual = (
    df_sub
    .groupby(["ORG_CODE1","ORG_NAME1","ICB_NAME","FY_LABEL"], as_index=False)
    .agg(mean_6wk_pct=("VALUE", "mean"), n_months=("PERIOD_START", "nunique"))
    .sort_values(["ICB_NAME","ORG_NAME1","FY_LABEL"])
)
subicb_annual.to_csv("subicb_annual.csv", index=False)
print(f"\n✅ Saved subicb_annual.csv  ({subicb_annual['ORG_CODE1'].nunique()} sub-ICBs)")

# ── CommissioningRegion ───────────────────────────────────────────────────────
df_region = df_m[df_m["GROUP_TYPE"] == "CommissioningRegion"].copy()
df_region = df_region[~df_region["ORG_CODE1"].str.contains("Invalid", na=False)]
region_annual = (
    df_region
    .groupby(["ORG_CODE1","ORG_NAME1","FY_LABEL"], as_index=False)["VALUE"]
    .mean()
    .rename(columns={"VALUE":"mean_6wk_pct","ORG_CODE1":"REGION_CODE","ORG_NAME1":"REGION_NAME"})
    .sort_values(["REGION_CODE","FY_LABEL"])
)
region_annual.to_csv("region_annual.csv", index=False)
print(f"\n✅ Saved region_annual.csv")
print(region_annual.to_string())

print("\n── Done ──")