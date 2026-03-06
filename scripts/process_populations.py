"""
process_populations.py
Processes ONS ICB population estimates and merges with NHSBSA prescribing data
to produce population-standardised rates (patients per 100,000) by region.

Sources:
  - ONS Mid-2011 to Mid-2022 ICB 2024 geography
  - ONS Mid-2022 to Mid-2024 ICB 2024 geography
  - nhsbsa_regional_annual.csv (from process_nhsbsa.py)

Output:
  - nhsbsa_regional_standardised.csv  (patients per 100,000 by ICB and year)
"""

import pandas as pd
from pathlib import Path

# ── Paths ─────────────────────────────────────────────────────────────────────
POP_DIR    = Path(r"/Users/adamchidlow/Desktop/Spectator/Data/ONS_populations")
NHSBSA_DIR = Path(r"/Users/adamchidlow/Desktop/Spectator/Data/nhsbsa_processed")
OUT_DIR    = Path(r"/Users/adamchidlow/Desktop/Spectator/Data/nhsbsa_processed")

POP_FILE_1 = POP_DIR / "2011_2022.xlsx"   # Mid-2011 to Mid-2022
POP_FILE_2 = POP_DIR / "2022_2024.xlsx"   # Mid-2022 to Mid-2024


# ── Step 1: Load and combine population files ─────────────────────────────────
def load_population_file(filepath: Path) -> pd.DataFrame:
    xl     = pd.ExcelFile(filepath)
    frames = []

    for sheet in xl.sheet_names:
        if not any(c.isdigit() for c in sheet):
            continue

        df = xl.parse(sheet, skiprows=3, usecols="A:G")
        df.columns = df.columns.str.strip()

        df = df.rename(columns={
            'ICB 2024 Code': 'icb_code',
            'ICB 2024 Name': 'icb_name_ons',
            'Total':         'population'
        })

        if 'icb_code' not in df.columns:
            print(f"  ⚠  Sheet '{sheet}': could not find ICB code column — skipping")
            continue

        df = df[df['icb_code'].astype(str).str.startswith('E54', na=False)]

        year_str = ''.join(filter(str.isdigit, sheet.split()[0]))
        if not year_str:
            continue
        df['mid_year'] = int(year_str)

        df_icb = (
            df.groupby(['icb_code', 'icb_name_ons', 'mid_year'])
              .agg(population=('population', 'sum'))
              .reset_index()
        )
        frames.append(df_icb)
        print(f"  ✓ Sheet '{sheet}': {len(df_icb)} ICBs, total pop {df_icb['population'].sum():,.0f}")

    return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()

def build_population_series() -> pd.DataFrame:
    print("\n[1/3] Loading ONS population files...")

    df1 = load_population_file(POP_FILE_1)
    df2 = load_population_file(POP_FILE_2)

    if df1.empty and df2.empty:
        raise ValueError("No population data loaded — check file paths and sheet names")

    # Combine, drop duplicate mid-year 2022 (keep revised version from file 2)
    df_all = pd.concat([df1, df2], ignore_index=True)
    df_all = df_all.sort_values(['icb_code', 'mid_year'])

    # Keep the revised 2022 estimate (from file 2) by dropping duplicates keeping last
    df_all = df_all.drop_duplicates(subset=['icb_code', 'mid_year'], keep='last')

    print(f"\n  Combined population series:")
    print(f"  ICBs: {df_all['icb_code'].nunique()}")
    print(f"  Years: {sorted(df_all['mid_year'].unique())}")

    return df_all


# ── Step 2: Map financial years to mid-year population estimates ───────────────
def financial_year_to_mid_year(fy: str) -> int:
    """
    Maps NHSBSA financial year to the closest ONS mid-year estimate.
    Financial year runs Apr–Mar, so mid-year (June) of the first calendar
    year is the best match.
    e.g. '2019/20' → 2019, '2022/23' → 2022
    """
    return int(fy.split('/')[0])


# ── Step 3: Match ICB names between NHSBSA and ONS ────────────────────────────
def normalise_name(name: str) -> str:
    """Lowercase + strip for consistent matching."""
    return str(name).lower().strip()


def merge_prescribing_with_population(df_pop: pd.DataFrame) -> pd.DataFrame:
    print("\n[2/3] Merging prescribing data with population estimates...")

    nhsbsa = pd.read_csv(NHSBSA_DIR / "nhsbsa_regional_annual.csv")

    # Add mid_year to NHSBSA data
    nhsbsa['mid_year'] = nhsbsa['financial_year'].apply(financial_year_to_mid_year)

    # Normalise ICB names for matching
    nhsbsa['icb_key'] = nhsbsa['icb_name'].apply(normalise_name)
    df_pop['icb_key'] = df_pop['icb_name_ons'].apply(normalise_name)

    # Merge on normalised name + mid_year
    merged = nhsbsa.merge(
        df_pop[['icb_key', 'mid_year', 'population', 'icb_code']],
        on=['icb_key', 'mid_year'],
        how='left'
    )

    # Report any unmatched rows
    unmatched = merged[merged['population'].isna()]['icb_name'].unique()
    if len(unmatched) > 0:
        print(f"\n  ⚠  {len(unmatched)} ICB(s) could not be matched to population data:")
        for name in unmatched:
            print(f"     - {name}")
        print("  These will be excluded from the standardised output.")
    else:
        print("  ✓ All ICBs matched successfully")

    return merged


# ── Step 4: Calculate rates per 100,000 ──────────────────────────────────────
def calculate_rates(merged: pd.DataFrame) -> pd.DataFrame:
    print("\n[3/3] Calculating rates per 100,000 population...")

    df = merged[merged['population'].notna()].copy()
    df['patients_per_100k'] = (df['identified_patients'] / df['population'] * 100000).round(1)
    df['items_per_100k']    = (df['total_items']         / df['population'] * 100000).round(1)

    # Clean up output columns
    df_out = df[[
        'financial_year', 'mid_year', 'icb_name', 'icb_code',
        'identified_patients', 'total_items', 'population',
        'patients_per_100k', 'items_per_100k'
    ]].copy()

    df_out = df_out.sort_values(['financial_year', 'patients_per_100k'], ascending=[True, False])

    path = OUT_DIR / "nhsbsa_regional_standardised.csv"
    df_out.to_csv(path, index=False)

    print(f"  ✓ {len(df_out)} rows saved → {path.name}")
    print(f"\n  Top 5 ICBs by patients per 100k (most recent year):")
    latest = df_out[df_out['financial_year'] == df_out['financial_year'].max()]
    print(latest[['icb_name', 'patients_per_100k']].head(5).to_string(index=False))

    print(f"\n  Bottom 5 ICBs by patients per 100k (most recent year):")
    print(latest[['icb_name', 'patients_per_100k']].tail(5).to_string(index=False))

    return df_out


# ── Main ──────────────────────────────────────────────────────────────────────
def main():
    print("=" * 60)
    print("ICB Population Standardisation")
    print("=" * 60)

    try:
        df_pop    = build_population_series()
        merged    = merge_prescribing_with_population(df_pop)
        df_rates  = calculate_rates(merged)

        print("\n" + "=" * 60)
        print("Done. nhsbsa_regional_standardised.csv is ready.")
        print("Update DATA_DIR in index.html and chart 05 will use")
        print("patients_per_100k instead of percentage change.")

    except Exception as e:
        print(f"\n✗ Failed: {e}")
        raise


if __name__ == "__main__":
    main()
