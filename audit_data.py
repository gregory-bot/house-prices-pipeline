"""
audit_data.py — Data quality audit and cleaning for Nairobi property data
==========================================================================
Run:
    python3 audit_data.py           # audit only
    python3 audit_data.py --fix     # audit + write location_summary_clean.csv
"""

import argparse
import pandas as pd
import numpy as np

CSV_IN  = "location_summary.csv"
CSV_OUT = "location_summary_clean.csv"

# Minimum realistic KES price for a Nairobi property listing
MIN_VALID_PRICE = 500_000   # KES 500K — below this = bad data


def detect_unit_scale(series):
    """
    Use Q75 (not median) so a handful of bad low rows don't
    fool the detector. Nairobi top-quartile property: KES 10M-40M.
    """
    q75 = series[series > 0].quantile(0.75)
    if q75 < 1_000:
        return "millions", 1_000_000
    elif q75 < 1_000_000:
        return "thousands", 1_000
    else:
        return "kes", 1


def audit(df):
    print("\n" + "="*62)
    print("  DATA AUDIT REPORT")
    print("="*62)
    print(f"  Rows     : {len(df)}")
    print(f"  Columns  : {list(df.columns)}")

    print(f"\n  PRICE COLUMN STATS")
    print(f"  " + "-"*58)
    for col in ["avg_price", "median_price"]:
        if col not in df.columns:
            continue
        s = df[col]
        unit, scale = detect_unit_scale(s)
        print(f"\n  {col}:")
        print(f"    Min    : {s.min():>15,.0f}")
        print(f"    Q25    : {s.quantile(0.25):>15,.0f}")
        print(f"    Median : {s.median():>15,.0f}")
        print(f"    Q75    : {s.quantile(0.75):>15,.0f}")
        print(f"    Max    : {s.max():>15,.0f}")
        print(f"    Mean   : {s.mean():>15,.0f}")
        print(f"    Zeros  : {(s == 0).sum()}")
        print(f"    Nulls  : {s.isna().sum()}")
        print(f"  -> Unit detected: {unit}  (scale: x{scale:,})")

    print(f"\n  BAD ROWS  (avg_price < KES {MIN_VALID_PRICE:,})")
    print(f"  " + "-"*58)
    bad = df[df["avg_price"] < MIN_VALID_PRICE]
    if bad.empty:
        print("  None OK")
    else:
        print(f"  {len(bad)} bad rows:")
        for _, r in bad.iterrows():
            print(f"    * {str(r['location'])[:55]:55}  avg={r['avg_price']:>12,.0f}")

    print(f"\n  OUTLIER DETECTION  (IQR x3, on clean rows only)")
    print(f"  " + "-"*58)
    clean_df = df[df["avg_price"] >= MIN_VALID_PRICE]
    for col in ["avg_price", "avg_price_per_bedroom"]:
        if col not in df.columns:
            continue
        q1, q3 = clean_df[col].quantile(0.25), clean_df[col].quantile(0.75)
        iqr = q3 - q1
        lo, hi = q1 - 3*iqr, q3 + 3*iqr
        out = clean_df[(clean_df[col] < lo) | (clean_df[col] > hi)]
        print(f"\n  {col}  (fence: {lo:,.0f} to {hi:,.0f})")
        if out.empty:
            print("    No outliers OK")
        else:
            print(f"    {len(out)} outlier(s):")
            for _, r in out.iterrows():
                print(f"      * {str(r['location'])[:50]:50}  {r[col]:>14,.0f}")

    print(f"\n  BEDROOM SANITY")
    print(f"  " + "-"*58)
    if "median_bedrooms" in df.columns:
        print(f"  Unique values : {sorted(df['median_bedrooms'].dropna().unique())}")
        weird = df[~df["median_bedrooms"].between(1, 10)]
        print(f"  Out of 1-10   : {len(weird)} rows {'OK' if weird.empty else 'WARN'}")

    print(f"\n  AFFORDABILITY RANK")
    print(f"  " + "-"*58)
    if "affordability_rank" in df.columns:
        print(f"  Range  : {df['affordability_rank'].min()} to {df['affordability_rank'].max()}")
        print(f"  Nulls  : {df['affordability_rank'].isna().sum()}")
        dups = df["affordability_rank"].duplicated().sum()
        print(f"  Dups   : {dups} {'OK' if dups == 0 else 'WARN: ranks should be unique'}")

    print("\n" + "="*62)


def clean(df):
    original = len(df)
    report = []

    # 1. Drop bad rows first
    bad_mask = df["avg_price"] < MIN_VALID_PRICE
    if bad_mask.any():
        report.append(f"Dropped {bad_mask.sum()} rows with avg_price < KES {MIN_VALID_PRICE:,}")
        df = df[~bad_mask].copy()

    # 2. Detect unit on clean data
    unit, scale = detect_unit_scale(df["avg_price"])
    if scale > 1:
        price_cols = ["avg_price", "median_price",
                      "avg_price_per_bedroom", "median_price_per_bedroom"]
        for col in price_cols:
            if col in df.columns:
                df[col] = df[col] * scale
        report.append(f"Rescaled price columns x{scale:,} (was in {unit})")

    # 3. Fill missing price_per_bedroom
    if "avg_price_per_bedroom" in df.columns and "median_bedrooms" in df.columns:
        miss = df["avg_price_per_bedroom"].isna() | (df["avg_price_per_bedroom"] == 0)
        beds = df["median_bedrooms"].fillna(1).replace(0, 1)
        df.loc[miss, "avg_price_per_bedroom"] = df.loc[miss, "avg_price"] / beds[miss]
        if miss.any():
            report.append(f"Filled {miss.sum()} missing avg_price_per_bedroom values")

    # 4. Re-number affordability rank
    if "affordability_rank" in df.columns:
        df = df.sort_values("affordability_rank").reset_index(drop=True)
        df["affordability_rank"] = range(1, len(df) + 1)
        report.append("Re-numbered affordability_rank 1 to N")

    print(f"\n  FIXES APPLIED")
    print(f"  " + "-"*58)
    for r in report:
        print(f"  OK {r}")
    print(f"  Rows: {original} -> {len(df)}")
    print(f"\n  Post-clean stats:")
    print(f"    avg_price  min: KES {df['avg_price'].min()/1e6:.2f}M  "
          f"median: KES {df['avg_price'].median()/1e6:.2f}M  "
          f"max: KES {df['avg_price'].max()/1e6:.2f}M")
    return df.reset_index(drop=True)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--csv", default=CSV_IN)
    parser.add_argument("--out", default=CSV_OUT)
    parser.add_argument("--fix", action="store_true")
    args = parser.parse_args()

    df = pd.read_csv(args.csv)
    audit(df)

    if args.fix:
        df_clean = clean(df.copy())
        df_clean.to_csv(args.out, index=False)
        print(f"\n  Saved -> {args.out}")
        print(f"  Next steps:")
        print(f"    python3 eda.py --csv {args.out}")
        print(f"    Update map_nairobi.py csv_path to {args.out}\n")
    else:
        print(f"\n  Run with --fix to auto-clean:")
        print(f"  python3 audit_data.py --fix\n")


if __name__ == "__main__":
    main()
