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

    # Upload enriched data to PostgreSQL
    import psycopg2
    def upload_to_postgres(csv_path):
        import os
        conn = psycopg2.connect(
            host=os.environ.get('DB_HOST'),
            port=int(os.environ.get('DB_PORT', 5432)),
            user=os.environ.get('DB_USER'),
            password=os.environ.get('DB_PASSWORD'),
            dbname=os.environ.get('DB_NAME')
        )
        cur = conn.cursor()
        df = pd.read_csv(csv_path)
        table_name = "nairobi_properties"
        # Drop table if exists, then create with correct columns
        cur.execute(f"DROP TABLE IF EXISTS {table_name}")
        conn.commit()
        cur.execute(f"""
            CREATE TABLE {table_name} (
                source TEXT,
                listing_type TEXT,
                title TEXT,
                price TEXT,
                location TEXT,
                bedrooms FLOAT,
                url TEXT,
                scraped_at TEXT,
                bedroom_label TEXT,
                price_no FLOAT,
                price_normalized FLOAT,
                price_per_bedroom FLOAT
            )
        """)
        conn.commit()
        # Clear table before inserting new data
        cur.execute(f"DELETE FROM {table_name}")
        conn.commit()
        # Insert data
        for _, row in df.iterrows():
            values = [
                row.get('source'),
                row.get('listing_type'),
                row.get('title'),
                row.get('price'),
                row.get('location'),
                row.get('bedrooms'),
                row.get('url'),
                row.get('scraped_at'),
                row.get('bedroom_label'),
                row.get('price_no'),
                row.get('price_normalized'),
                row.get('price_per_bedroom')
            ]
            cur.execute(f"""
                INSERT INTO {table_name} (source, listing_type, title, price, location, bedrooms, url, scraped_at, bedroom_label, price_no, price_normalized, price_per_bedroom)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, values)
        conn.commit()
        cur.close()
        conn.close()
        print("Uploaded enriched data to PostgreSQL.")

    # Call upload function after saving enriched CSV
    upload_to_postgres("nairobi_properties_full.csv")
