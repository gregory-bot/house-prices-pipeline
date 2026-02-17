import sys
import pandas as pd
import re

def extract_bedrooms(text):
    """
    Extract bedroom count from a string (title or URL).
    Examples: '2-bedroom apartment', '4-bedroom-apartment-flat-for-sale'
    """
    if pd.isna(text):
        return None
    match = re.search(r'(\d+)\s*[- ]?\s*bed(room)?', str(text).lower())
    if match:
        return int(match.group(1))
    return None

def prepare_properties(path):
    # Load raw dataset
    df = pd.read_csv(path)

    # Drop columns that are completely empty
    df = df.dropna(axis=1, how="all")

    # Convert bedrooms to numeric (force errors to NaN)
    df["bedrooms"] = pd.to_numeric(df.get("bedrooms"), errors="coerce")

    # Fill missing bedrooms from title
    parsed_bedrooms_title = df.loc[df["bedrooms"].isna(), "title"].apply(extract_bedrooms)
    df.loc[df["bedrooms"].isna(), "bedrooms"] = pd.to_numeric(parsed_bedrooms_title, errors="coerce")

    # Fill still-missing bedrooms from URL
    parsed_bedrooms_url = df.loc[df["bedrooms"].isna(), "url"].apply(extract_bedrooms)
    df.loc[df["bedrooms"].isna(), "bedrooms"] = pd.to_numeric(parsed_bedrooms_url, errors="coerce")

    # Add human-readable bedroom label
    df["bedroom_label"] = df["bedrooms"].apply(
        lambda x: f"{int(x)} Bedrooms" if pd.notna(x) and x > 0 else "Unknown"
    )

    # Add numeric price column
    df["price_no"] = (
        df["price"]
        .astype(str)
        .str.replace(r"[^\d]", "", regex=True)
        .replace("", "0")
        .astype(float)
    )

    # Normalize price (placeholder — adjust if you have specific logic)
    df["price_normalized"] = df["price_no"]

    # Price per bedroom (avoid division by zero or NaN)
    df["price_per_bedroom"] = df.apply(
        lambda row: row["price_normalized"] / row["bedrooms"]
        if pd.notna(row["bedrooms"]) and row["bedrooms"] > 0
        else None,
        axis=1
    )

    return df

if __name__ == "__main__":
    # Allow passing filename as argument, default to all_raw_listings.csv
    filename = sys.argv[1] if len(sys.argv) > 1 else "all_raw_listings.csv"
    df = prepare_properties(filename)

    print("\n=== Enriched Preview ===")
    print(df[["url", "title", "bedrooms", "bedroom_label", "price_normalized", "price_per_bedroom"]].head(20))

    # Save enriched dataset with all original + new columns
    df.to_csv("nairobi_properties_full.csv", index=False)

    print("\nSaved enriched dataset to nairobi_properties_full.csv")

# After df = prepare_properties(filename)

# Save enriched dataset
df.to_csv("nairobi_properties_full.csv", index=False)

# Location summary (average price per location)
location_summary = df.groupby("location").agg(
    avg_price=("price_normalized", "mean"),
    avg_price_per_bedroom=("price_per_bedroom", "mean"),
    count=("url", "count")
).reset_index()
location_summary.to_csv("location_summary.csv", index=False)

# Properties with summary (merge back averages into each row)
df_with_summary = df.merge(location_summary, on="location", how="left")
df_with_summary.to_csv("properties_with_summary.csv", index=False)
