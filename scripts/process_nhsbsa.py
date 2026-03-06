"""
process_nhsbsa.py
Processes NHSBSA "Medicines Used in Mental Health" Excel files.

Expected files in nhsbsa_raw/:
  - Financial year CNS stimulants summary tables (.xlsx)
  - Calendar year CNS stimulants summary tables (.xlsx)
  - Quarterly CNS stimulants summary tables (.xlsx)

Produces clean CSVs ready for the dashboard:
  - nhsbsa_children_trend.csv         → children vs adults by financial year
  - nhsbsa_drug_breakdown_monthly.csv → drug comparison by month (2015-2025)
  - nhsbsa_age_band_quarterly.csv     → age band breakdown by quarter
  - nhsbsa_gender_annual.csv          → gender split by financial year
  - nhsbsa_regional_annual.csv        → ICB regional breakdown by financial year
"""

import pandas as pd
from pathlib import Path

# ── UPDATE THESE PATHS ────────────────────────────────────────────────────────
RAW_DIR = Path(r"/Users/adamchidlow/Desktop/Spectator/Data/nhsbsa_raw")
OUT_DIR = Path(r"/Users/adamchidlow/Desktop/Spectator/Data/nhsbsa_processed")

# ── ADHD drugs to keep (excludes caffeine, caffeine citrate, modafinil) ───────
ADHD_DRUGS = {
    "Methylphenidate hydrochloride": "methylphenidate",
    "Lisdexamfetamine dimesylate":   "lisdexamfetamine",
    "Atomoxetine hydrochloride":     "atomoxetine",
    "Dexamfetamine sulfate":        "dexamfetamine",
}

# 15-19 band straddles the children/adult boundary — flagged in output
CHILD_BANDS = {"0 to 4", "5 to 9", "10 to 14", "15 to 19"}


def find_file(keyword: str) -> Path:
    """Find an Excel file in RAW_DIR whose name contains keyword."""
    files = list(RAW_DIR.glob("*.xlsx")) + list(RAW_DIR.glob("*.xls"))
    matches = [f for f in files if keyword.lower() in f.name.lower()]
    if not matches:
        raise FileNotFoundError(
            f"\nNo file matching '{keyword}' found in:\n  {RAW_DIR}"
            f"\nFiles present: {[f.name for f in files]}"
            f"\nCheck RAW_DIR path and that files are downloaded."
        )
    print(f"  Using file: {matches[0].name}")
    return matches[0]


def get_col(df: pd.DataFrame, keyword: str) -> str:
    """Return first column name containing keyword (case-insensitive)."""
    matches = [c for c in df.columns if keyword.lower() in c.lower()]
    if not matches:
        raise KeyError(f"No column containing '{keyword}' found. Columns: {list(df.columns)}")
    return matches[0]


# ── 1. Children vs Adults trend ───────────────────────────────────────────────
def process_children_trend() -> pd.DataFrame:
    """
    Financial year file → Prescribing_in_children tab (headers row 5)
    Age Band values: '17 and under', '18 and over', 'Unknown'
    """
    print("\n[1/5] Children vs adults trend...")
    f = find_file("financial")
    df = pd.read_excel(f, sheet_name="Prescribing_in_children", skiprows=4)
    df.columns = df.columns.str.strip()

    # Keep only children and adults rows
    df = df[df["Age Band"].isin(["17 and under", "18 and over"])]

    df_agg = (
        df.groupby(["Financial Year", "Age Band"])
        .agg(
            identified_patients=(get_col(df, "Total Identified Patients"), "sum"),
            total_items=(get_col(df, "Total Items"), "sum"),
            nic_gbp=(get_col(df, "Net Ingredient Cost"), "sum"),
        )
        .reset_index()
    )

    df_agg["age_group"] = df_agg["Age Band"].map({
        "17 and under": "children",
        "18 and over":  "adults",
    })

    df_out = df_agg[["Financial Year", "age_group", "identified_patients",
                      "total_items", "nic_gbp"]].copy()
    df_out.columns = ["financial_year", "age_group", "identified_patients",
                      "total_items", "nic_gbp"]

    path = OUT_DIR / "nhsbsa_children_trend.csv"
    df_out.to_csv(path, index=False)
    print(f"  ✓ {len(df_out)} rows saved → {path.name}")
    print(f"    Range: {df_out['financial_year'].min()} → {df_out['financial_year'].max()}")
    return df_out


# ── 2. Monthly drug breakdown ─────────────────────────────────────────────────
def process_drug_breakdown_monthly() -> pd.DataFrame:
    """
    Quarterly file → Monthly_Chemical_Substance tab (headers row 6)
    Year Month format: 201504
    14 rows per month: 7 drugs × identified/non-identified
    """
    print("\n[2/5] Monthly drug breakdown...")
    f = find_file("quarterly")
    df = pd.read_excel(f, sheet_name="Monthly_Chemical_Substance", skiprows=5)
    df.columns = df.columns.str.strip()

    # Find drug name column by searching for known drug name
    drug_col = None
    for col in df.columns:
        if df[col].astype(str).str.contains("Methylphenidate", case=False, na=False).any():
            drug_col = col
            break
    if not drug_col:
        raise ValueError(
            "Could not find drug name column. "
            f"Check Monthly_Chemical_Substance tab. Columns: {list(df.columns)}"
        )

    # Filter: identified patients only + ADHD drugs only
    id_col = get_col(df, "Identified Patient Flag")
    df = df[df[id_col].astype(str).str.strip() == "Y"]
    df = df[df[drug_col].isin(ADHD_DRUGS.keys())]

    # Parse Year Month column (201504 → 2015-04-01)
    ym_col = get_col(df, "Year Month")
    df["date"] = pd.to_datetime(df[ym_col].astype(str).str.strip(), format="%Y%m")
    df["drug"] = df[drug_col].map(ADHD_DRUGS)

    items_col = get_col(df, "Total Items")
    nic_col   = get_col(df, "Net Ingredient Cost")

    df_out = (
        df[["date", "drug", items_col, nic_col]]
        .copy()
        .rename(columns={items_col: "total_items", nic_col: "nic_gbp"})
        .sort_values(["drug", "date"])
        .reset_index(drop=True)
    )

    path = OUT_DIR / "nhsbsa_drug_breakdown_monthly.csv"
    df_out.to_csv(path, index=False)
    print(f"  ✓ {len(df_out)} rows saved → {path.name}")
    print(f"    Date range: {df_out['date'].min().date()} → {df_out['date'].max().date()}")
    print(f"    Drugs: {sorted(df_out['drug'].unique())}")
    return df_out


# ── 3. Age band breakdown ─────────────────────────────────────────────────────
def process_age_band() -> pd.DataFrame:
    """
    Quarterly file → Age_Band tab (headers row 6)
    21 rows per quarter: 19 age bands + 2 unknowns (identified + non-identified)
    Note: 15-19 band straddles children/adult boundary
    """
    print("\n[3/5] Age band breakdown...")
    f = find_file("quarterly")
    df = pd.read_excel(f, sheet_name="Age_Band", skiprows=5)
    df.columns = df.columns.str.strip()

    # Keep identified patients, remove Unknown age bands
    id_col = get_col(df, "Identified Patient Flag")
    df = df[df[id_col].astype(str).str.strip() == "Y"]
    df = df[~df["Age Band"].astype(str).str.lower().str.contains("unknown", na=False)]

    patients_col = get_col(df, "Total Identified Patients")
    items_col    = get_col(df, "Total Items")
    nic_col      = get_col(df, "Net Ingredient Cost")
    fy_col       = get_col(df, "Financial Year")
    fq_col       = get_col(df, "Financial Quarter")

    df_agg = (
        df.groupby([fy_col, fq_col, "Age Band"])
        .agg(
            identified_patients=(patients_col, "sum"),
            total_items=(items_col, "sum"),
            nic_gbp=(nic_col, "sum"),
        )
        .reset_index()
        .rename(columns={fy_col: "financial_year", fq_col: "financial_quarter",
                         "Age Band": "age_band"})
    )

    # Flag children vs adults (note 15-19 straddles boundary)
    df_agg["age_group"] = df_agg["age_band"].apply(
        lambda x: "children_adolescents" if x in CHILD_BANDS else "adults"
    )
    df_agg["note"] = df_agg["age_band"].apply(
        lambda x: "straddles_boundary" if x == "15 to 19" else ""
    )

    path = OUT_DIR / "nhsbsa_age_band_quarterly.csv"
    df_agg.to_csv(path, index=False)
    print(f"  ✓ {len(df_agg)} rows saved → {path.name}")
    print(f"    Age bands: {sorted(df_agg['age_band'].unique())}")
    return df_agg


# ── 4. Gender split ───────────────────────────────────────────────────────────
def process_gender() -> pd.DataFrame:
    """
    Financial year file → Gender tab (headers row 7)
    """
    print("\n[4/5] Gender split...")
    f = find_file("financial")
    df = pd.read_excel(f, sheet_name="Gender", skiprows=6)
    df.columns = df.columns.str.strip()

    id_col     = get_col(df, "Identified Patient Flag")
    gender_col = get_col(df, "Gender")
    fy_col     = get_col(df, "Financial Year")

    # Keep identified patients, remove unknown gender
    df = df[df[id_col].astype(str).str.strip() == "Y"]
    df = df[~df[gender_col].astype(str).str.lower().str.contains("unknown", na=False)]

    patients_col = get_col(df, "Total Identified Patients")
    items_col    = get_col(df, "Total Items")
    nic_col      = get_col(df, "Net Ingredient Cost")

    df_agg = (
        df.groupby([fy_col, gender_col])
        .agg(
            identified_patients=(patients_col, "sum"),
            total_items=(items_col, "sum"),
            nic_gbp=(nic_col, "sum"),
        )
        .reset_index()
        .rename(columns={fy_col: "financial_year", gender_col: "gender"})
    )

    path = OUT_DIR / "nhsbsa_gender_annual.csv"
    df_agg.to_csv(path, index=False)
    print(f"  ✓ {len(df_agg)} rows saved → {path.name}")
    print(f"    Genders found: {list(df_agg['gender'].unique())}")
    return df_agg


# ── 5. Regional ICB breakdown ─────────────────────────────────────────────────
def process_regional() -> pd.DataFrame:
    """
    Financial year file → ICB tab
    Auto-detects header row by scanning for 'Financial Year' column.
    """
    print("\n[5/5] Regional ICB breakdown...")
    f = find_file("financial")

    df = None
    for skip in [4, 5, 6, 7]:
        test = pd.read_excel(f, sheet_name="ICB", skiprows=skip)
        test.columns = test.columns.str.strip()
        if any("financial year" in c.lower() for c in test.columns):
            df = test
            print(f"  Header row detected at skiprows={skip}")
            break

    if df is None:
        raise ValueError("Could not detect header row in ICB tab")

    # Keep identified patients only if flag column exists
    id_cols = [c for c in df.columns if "identified patient flag" in c.lower()]
    if id_cols:
        df = df[df[id_cols[0]].astype(str).str.strip() == "Y"]

    # Find ICB name column
    icb_candidates = [c for c in df.columns if "icb" in c.lower()]
    icb_col = icb_candidates[0] if icb_candidates else df.columns[1]

    fy_col       = get_col(df, "Financial Year")
    patients_col = get_col(df, "Total Identified Patients")
    items_col    = get_col(df, "Total Items")
    nic_col      = get_col(df, "Net Ingredient Cost")

    df_agg = (
        df.groupby([fy_col, icb_col])
        .agg(
            identified_patients=(patients_col, "sum"),
            total_items=(items_col, "sum"),
            nic_gbp=(nic_col, "sum"),
        )
        .reset_index()
        .rename(columns={fy_col: "financial_year", icb_col: "icb_name"})
    )

    path = OUT_DIR / "nhsbsa_regional_annual.csv"
    df_agg.to_csv(path, index=False)
    print(f"  ✓ {len(df_agg)} rows saved → {path.name}")
    print(f"    ICB regions: {df_agg['icb_name'].nunique()}")
    return df_agg

def process_age_gender_pyramid() -> pd.DataFrame:
    """
    Financial year file → Age_Band_and_Gender tab (skiprows=6)
    Produces age × gender × year data for pyramid chart.
    Includes % growth calculation over 5 years.
    """
    print("Processing age/gender pyramid...")
    f = find_file("financial")
    df = pd.read_excel(f, sheet_name='Age_Band_and_Gender', skiprows=6)
    df.columns = df.columns.str.strip()

    # Identified patients only, known gender only
    df = df[df['Identified Patient Flag'].astype(str).str.strip() == 'Y']
    df = df[df['Patient Gender'].isin(['Male', 'Female'])]

    # Keep relevant age bands only (exclude Unknown)
    bands = ['0 to 4','5 to 9','10 to 14','15 to 19','20 to 24','25 to 29',
             '30 to 34','35 to 39','40 to 44','45 to 49','50 to 54','55 to 59',
             '60 to 64','65 to 69','70 to 74','75 to 79','80 to 84','85 to 89','90+']
    df = df[df['Age Band'].isin(bands)]

    # Aggregate by year, age band, gender
    df_agg = (
        df.groupby(['Financial Year', 'Age Band', 'Patient Gender'])
          .agg(identified_patients=('Total Identified Patients', 'sum'))
          .reset_index()
          .rename(columns={'Financial Year':'financial_year',
                           'Age Band':'age_band',
                           'Patient Gender':'gender'})
    )

    # Standardise financial year format 2015/2016 → 2015/16
    df_agg['financial_year'] = df_agg['financial_year'].str.replace(
        r'(\d{4})/(\d{4})', lambda m: f"{m.group(1)}/{m.group(2)[2:]}", regex=True
    )

    # Calculate 5-year growth: compare latest year to 5 years prior
    years = sorted(df_agg['financial_year'].unique())
    latest = years[-1]
    year_5y = years[-6] if len(years) >= 6 else years[0]
    print(f"  Growth comparison: {year_5y} → {latest}")

    df_latest = df_agg[df_agg['financial_year'] == latest].copy()
    df_base5  = df_agg[df_agg['financial_year'] == year_5y].copy()

    df_out = df_latest.merge(
        df_base5[['age_band','gender','identified_patients']],
        on=['age_band','gender'],
        suffixes=('', '_5y_ago')
    )
    df_out['growth_5y_pct'] = (
        (df_out['identified_patients'] - df_out['identified_patients_5y_ago'])
        / df_out['identified_patients_5y_ago'] * 100
    ).round(1)

    df_out['baseline_year'] = year_5y
    df_out['latest_year']   = latest

    path = OUT_DIR / 'nhsbsa_age_gender_pyramid.csv'
    df_out.to_csv(path, index=False)
    print(f"  ✓ {len(df_out)} rows saved → {path.name}")
    print(f"    Age bands: {df_out['age_band'].nunique()}, Genders: {df_out['gender'].unique()}")
    return df_out

# ── Main ──────────────────────────────────────────────────────────────────────
def main():
    print("=" * 60)
    print("NHSBSA CNS Stimulants Data Processor")
    print("=" * 60)
    print(f"Reading from: {RAW_DIR}")
    print(f"Saving to:    {OUT_DIR}")

    OUT_DIR.mkdir(parents=True, exist_ok=True)

    if not RAW_DIR.exists():
        print(f"\n✗ RAW_DIR not found: {RAW_DIR}")
        print("  Create the folder and add your three NHSBSA Excel files.")
        return

    success = 0
    for fn in [process_children_trend, process_drug_breakdown_monthly,
               process_age_band, process_gender, process_regional]:
        try:
            fn()
            success += 1
        except Exception as e:
            print(f"  ✗ {fn.__name__} failed: {e}")

    print("\n" + "=" * 60)
    print(f"Complete: {success}/5 datasets processed successfully.")
    if success == 5:
        print("All CSVs ready — next step is building the dashboard.")
    else:
        print("Fix errors above and re-run. Each function is independent.")
    try:
        process_age_gender_pyramid()
    except Exception as e:
        print(f"  ✗ Age/gender pyramid failed: {e}")

if __name__ == "__main__":
    main()
