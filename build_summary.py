"""
build_summary.py — Rebuild location_summary.csv from raw listing data
======================================================================
Run:
    python3 build_summary.py              # Sale listings only (default)
    python3 build_summary.py --type Rent  # Rent analysis
    python3 build_summary.py --inspect    # overview only, no file written
"""

import argparse
import pandas as pd
import numpy as np

RAW_CSV      = "cleaned_properties.csv"
OUT_CSV      = "location_summary_clean.csv"
MIN_PRICE    = 1_000_000
MAX_PRICE    = 500_000_000
MIN_LISTINGS = 1


def inspect(df):
    print(f"\n{'='*58}\n  RAW DATA OVERVIEW\n{'='*58}")
    print(f"  Rows     : {len(df)}")
    print(f"  Columns  : {list(df.columns)}")
    if "listing_type" in df.columns:
        for t, n in df["listing_type"].value_counts().items():
            print(f"  {t}: {n}")
    if "price_normalized" in df.columns:
        s = df["price_normalized"].dropna()
        print(f"  price min={s.min():.0f}  median={s.median():.0f}  max={s.max():.0f}")
    print(f"{'='*58}")


def clean_listings(df, listing_type="Sale"):
    price_col = "price_normalized" if "price_normalized" in df.columns else "price_no"
    df = df.copy()
    df["_price"] = pd.to_numeric(df[price_col], errors="coerce")

    if listing_type != "Both" and "listing_type" in df.columns:
        df = df[df["listing_type"].str.strip().str.lower() == listing_type.lower()].copy()

    df = df.dropna(subset=["_price"])
    df = df[(df["_price"] >= MIN_PRICE) & (df["_price"] <= MAX_PRICE)].copy()

    if "bedrooms" in df.columns:
        df["_beds"] = pd.to_numeric(df["bedrooms"], errors="coerce")
    elif "bedroom_label" in df.columns:
        df["_beds"] = df["bedroom_label"].str.extract(r"(\d+)").astype(float)
    else:
        df["_beds"] = np.nan

    print(f"  Clean {listing_type} listings: {len(df)}")
    return df.reset_index(drop=True)


def remove_outliers_iqr(group, k=2.5):
    q1 = group["_price"].quantile(0.25)
    q3 = group["_price"].quantile(0.75)
    iqr = q3 - q1
    if iqr == 0:
        return group
    return group[(group["_price"] >= q1 - k*iqr) & (group["_price"] <= q3 + k*iqr)]


def build_summary(df, min_listings=MIN_LISTINGS):
    # Remove per-location outliers — keep location column intact
    cleaned_parts = []
    for loc, grp in df.groupby("location"):
        cleaned_parts.append(remove_outliers_iqr(grp))
    df = pd.concat(cleaned_parts, ignore_index=True)

    # Aggregate
    agg = df.groupby("location").agg(
        avg_price       = ("_price", "mean"),
        median_price    = ("_price", "median"),
        median_bedrooms = ("_beds",  "median"),
        listing_count   = ("_price", "count"),
    ).reset_index()

    # Filter min listings
    agg = agg[agg["listing_count"] >= min_listings].copy()

    # Price per bedroom
    beds = agg["median_bedrooms"].fillna(1).replace(0, 1)
    agg["avg_price_per_bedroom"]    = (agg["avg_price"]    / beds).round(0).astype(int)
    agg["median_price_per_bedroom"] = (agg["median_price"] / beds).round(0).astype(int)
    agg["avg_price"]    = agg["avg_price"].round(0).astype(int)
    agg["median_price"] = agg["median_price"].round(0).astype(int)

    # Affordability rank
    agg = agg.sort_values("avg_price_per_bedroom").reset_index(drop=True)
    agg["affordability_rank"] = range(1, len(agg) + 1)

    return agg[[
        "location", "avg_price", "median_price", "median_bedrooms",
        "avg_price_per_bedroom", "median_price_per_bedroom",
        "affordability_rank", "listing_count"
    ]]


def print_stats(df):
    print(f"\n{'='*58}\n  SUMMARY STATS\n{'='*58}")
    print(f"  Locations : {len(df)}")
    print(f"  Listings  : {df['listing_count'].sum()}")
    print(f"  avg_price  min : KES {df['avg_price'].min():>12,.0f}")
    print(f"  avg_price  med : KES {df['avg_price'].median():>12,.0f}")
    print(f"  avg_price  max : KES {df['avg_price'].max():>12,.0f}")
    print(f"\n  Top 10 most affordable:")
    for _, r in df.nsmallest(10, "avg_price_per_bedroom").iterrows():
        print(f"    #{int(r['affordability_rank']):3}  "
              f"{r['location'].split(',')[0].strip()[:38]:38}  "
              f"KES {r['avg_price']/1e6:.1f}M  n={int(r['listing_count'])}")
    print(f"{'='*58}")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--csv",     default=RAW_CSV)
    parser.add_argument("--out",     default=OUT_CSV)
    parser.add_argument("--type",    default="Sale")
    parser.add_argument("--min",     default=MIN_LISTINGS, type=int)
    parser.add_argument("--inspect", action="store_true")
    args = parser.parse_args()

    df = pd.read_csv(args.csv)
    inspect(df)

    if args.inspect:
        return

    df_clean = clean_listings(df, listing_type=args.type)
    summary  = build_summary(df_clean, min_listings=args.min)
    print_stats(summary)

    summary.to_csv(args.out, index=False)
    print(f"\n  Saved -> {args.out}")
    print(f"  Next:  python3 eda.py --csv {args.out}\n")


if __name__ == "__main__":
    main()
