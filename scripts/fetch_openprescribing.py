"""
fetch_openprescribing.py
Fetches ADHD/stimulant prescribing data from the OpenPrescribing API
(Bennett Institute, University of Oxford).

BNF codes covered:
    0404000M0  - Methylphenidate hydrochloride
    0404000U0  - Lisdexamfetamine dimesylate
    0404000S0  - Atomoxetine hydrochloride
    0404000L0  - Dexamfetamine sulphate
    0404000V0  - Guanfacine
    0404         - Full CNS stimulants & ADHD section

API docs: https://openprescribing.net/api/
Data goes back to August 2010 via /api/1.0/spending/
"""

import requests
import pandas as pd
from pathlib import Path
import time

# ── Output directory ────────────────────────────────────────────────────────
OUTPUT_DIR = Path(r"/Users/adamchidlow/Desktop/Spectator/Data").parent
OUTPUT_DIR.mkdir(exist_ok=True)

BASE_URL = "https://openprescribing.net/api/1.0"

# ── Drug codes to fetch ──────────────────────────────────────────────────────
DRUGS = {
    "methylphenidate":       "0404000M0",
    "lisdexamfetamine":      "0404000U0",
    "atomoxetine":           "0404000S0",
    "dexamfetamine":         "0404000L0",
    "guanfacine":            "0404000V0",
    "all_adhd_section":      "0404",
}

# ── NHS England regions (ICB codes, current as of 2024) ─────────────────────
# For regional breakdown — fetches spending by ICB organisation type
REGIONS = {
    "QE1": "North East and Yorkshire",
    "QHG": "Cheshire and Merseyside",
    "QOQ": "North West",
    "QJK": "West Yorkshire",
    "QVV": "Humber and North Yorkshire",
    "QR1": "South Yorkshire",
    "QYG": "Midlands",
    "QHL": "Birmingham and Solihull",
    "QK1": "East of England",
    "QMJ": "East London",
    "QMF": "North Central London",
    "QWE": "North East London",
    "QNQ": "North West London",
    "QKK": "South East London",
    "QHM": "South West London",
    "QXU": "Kent and Medway",
    "QJG": "South East",
    "QNX": "Devon",
    "QT6": "Somerset",
    "QSL": "Sussex",
    "QU9": "Surrey Heartlands",
    "QWO": "Bath and North East Somerset",
    "QNC": "Bristol",
    "Q99": "Gloucestershire",
    "QJ2": "South West",
}


def fetch_spending_by_code(bnf_code: str, drug_name: str) -> pd.DataFrame:
    """
    Fetches total national spending + items by month for a BNF code.
    Returns ~15 years of monthly data going back to August 2010.
    """
    url = f"{BASE_URL}/spending/"
    params = {"code": bnf_code, "format": "json"}
    headers = {"User-Agent": "Mozilla/5.0 (research project; contact: achidlow@me.com)"}
    
    print(f"  Fetching national totals: {drug_name} ({bnf_code})...")
    resp = requests.get(url, params=params, timeout=30)
    resp.raise_for_status()

    data = resp.json()
    if not data:
        print(f"    ⚠  No data returned for {drug_name}")
        return pd.DataFrame()
    
    df = pd.DataFrame(data)
    df["drug"] = drug_name
    df["bnf_code"] = bnf_code
    df["date"] = pd.to_datetime(df["date"])
    df = df.sort_values("date").reset_index(drop=True)

    # Add derived columns
    df["year"] = df["date"].dt.year
    df["financial_year"] = df["date"].apply(
        lambda d: f"{d.year}/{str(d.year+1)[-2:]}" if d.month >= 4
        else f"{d.year-1}/{str(d.year)[-2:]}"
    )
    return df


def fetch_spending_by_icb(bnf_code: str, drug_name: str) -> pd.DataFrame:
    """
    Fetches spending broken down by ICB (regional) for a BNF code.
    Good for regional variation chart.
    """
    url = f"{BASE_URL}/spending_by_org/"
    params = {"org_type": "icb", "code": bnf_code, "format": "json"}
    headers = {"User-Agent": "Mozilla/5.0 (research project; contact: achidlow@me.com)"}

    print(f"  Fetching ICB breakdown: {drug_name} ({bnf_code})...")
    resp = requests.get(url, params=params, timeout=60)
    resp.raise_for_status()

    data = resp.json()
    if not data:
        print(f"    ⚠  No ICB data for {drug_name}")
        return pd.DataFrame()

    df = pd.DataFrame(data)
    df["drug"] = drug_name
    df["bnf_code"] = bnf_code
    df["date"] = pd.to_datetime(df["date"])
    return df


def aggregate_annual(df: pd.DataFrame) -> pd.DataFrame:
    """Aggregate monthly data to financial-year totals."""
    return (
        df.groupby(["financial_year", "drug"])
        .agg(
            items=("items", "sum"),
            quantity=("quantity", "sum"),
            actual_cost=("actual_cost", "sum"),
        )
        .reset_index()
        .sort_values(["drug", "financial_year"])
    )


def main():
    print("=" * 60)
    print("OpenPrescribing ADHD Data Fetch")
    print("=" * 60)

    all_monthly = []
    all_icb = []

    for drug_name, bnf_code in DRUGS.items():
        # ── National monthly totals ──────────────────────────────────────
        df_monthly = fetch_spending_by_code(bnf_code, drug_name)
        if not df_monthly.empty:
            all_monthly.append(df_monthly)

        # ── ICB (regional) breakdown — only for the full ADHD section ────
        # to avoid hammering the API; individual drugs can be added later
        if drug_name == "all_adhd_section":
            df_icb = fetch_spending_by_icb(bnf_code, drug_name)
            if not df_icb.empty:
                all_icb.append(df_icb)

        time.sleep(0.5)  # be polite to the API

    # ── Combine & save monthly data ──────────────────────────────────────────
    if all_monthly:
        df_monthly_all = pd.concat(all_monthly, ignore_index=True)

        # Pivot so each drug is a column — useful for drug comparison chart
        df_pivot = (
            df_monthly_all[df_monthly_all["drug"] != "all_adhd_section"]
            .pivot_table(
                index="date",
                columns="drug",
                values="items",
                aggfunc="sum",
            )
            .reset_index()
        )

        df_monthly_all.to_csv(OUTPUT_DIR / "openprescribing_monthly_long.csv", index=False)
        df_pivot.to_csv(OUTPUT_DIR / "openprescribing_monthly_pivot.csv", index=False)

        # Annual summaries
        df_annual = aggregate_annual(df_monthly_all)
        df_annual.to_csv(OUTPUT_DIR / "openprescribing_annual.csv", index=False)

        print(f"\n✓ Monthly data saved  → openprescribing_monthly_long.csv")
        print(f"✓ Drug pivot saved    → openprescribing_monthly_pivot.csv")
        print(f"✓ Annual data saved   → openprescribing_annual.csv")
        print(f"  Rows: {len(df_monthly_all):,}  |  Date range: "
              f"{df_monthly_all['date'].min().date()} → {df_monthly_all['date'].max().date()}")

    # ── Combine & save ICB data ──────────────────────────────────────────────
    if all_icb:
        df_icb_all = pd.concat(all_icb, ignore_index=True)
        df_icb_all.to_csv(OUTPUT_DIR / "openprescribing_icb_regional.csv", index=False)
        print(f"✓ Regional ICB data   → openprescribing_icb_regional.csv")
        print(f"  ICB organisations:  {df_icb_all['row_name'].nunique()}")

    print("\nDone. Next step: download NHSBSA Excel files (see README.md).")


if __name__ == "__main__":
    main()
