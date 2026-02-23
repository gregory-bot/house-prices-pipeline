import pandas as pd

def clean_properties(path):
    # Load dataset
    df = pd.read_csv(path)
    print(f"Loaded dataset: {df.shape[0]} rows, {df.shape[1]} columns")

    # Remove duplicates
    before = df.shape[0]
    df = df.drop_duplicates()
    after = df.shape[0]
    print(f"Removed {before - after} duplicate rows")

    # Handle missing values (example: bedrooms)
    missing_bedrooms = df['bedrooms'].isna().sum()
    if missing_bedrooms > 0:
        df['bedrooms'] = df['bedrooms'].fillna(df['bedrooms'].median())
        print(f"Filled {missing_bedrooms} missing bedroom values with median")


    # Standardize location names
    df['location'] = df['location'].str.strip().str.lower()

    # Extract numeric value from price column and create price_no
    import re
    def extract_price(price_str):
        if pd.isna(price_str):
            return pd.NA
        # Remove non-digit characters
        price_digits = re.sub(r'[^0-9]', '', str(price_str))
        return int(price_digits) if price_digits else pd.NA
    df['price_no'] = df['price'].apply(extract_price)

    # Remove extreme outliers (price)
    Q1 = df['price_no'].quantile(0.25)
    Q3 = df['price_no'].quantile(0.75)
    IQR = Q3 - Q1
    before = df.shape[0]
    df = df[(df['price_no'] >= Q1 - 1.5*IQR) & (df['price_no'] <= Q3 + 1.5*IQR)]
    after = df.shape[0]
    print(f"Removed {before - after} outlier rows based on price")

    # Add month feature
    df['month'] = pd.to_datetime(df['scraped_at']).dt.month

    # Ensure price_per_bedroom is computed safely
    df['price_per_bedroom'] = df['price_no'] / df['bedrooms'].replace(0, pd.NA)

    # Save cleaned dataset
    df.to_csv("cleaned_properties.csv", index=False)
    print("Cleaned dataset saved as cleaned_properties.csv")

    # Generate location summary
    location_summary = df.groupby('location').agg({
        'price_no': ['mean', 'median'],
        'bedrooms': 'median',
        'price_per_bedroom': ['mean', 'median']
    }).reset_index()

    # Flatten column names
    location_summary.columns = [
        'location', 'avg_price', 'median_price',
        'median_bedrooms', 'avg_price_per_bedroom', 'median_price_per_bedroom'
    ]

    # Sort by affordability (lowest avg_price_per_bedroom first)
    location_summary = location_summary.sort_values(by='avg_price_per_bedroom', ascending=True)

    # Add rank column
    location_summary['affordability_rank'] = range(1, len(location_summary) + 1)

    # Save location summary
    location_summary.to_csv("location_summary.csv", index=False)
    print("Location summary saved as location_summary.csv (ranked by affordability)")

    return df, location_summary

if __name__ == "__main__":
    df, location_summary = clean_properties("nairobi_properties_full.csv")

    print("\nTop 5 most affordable neighborhoods:")
    print(location_summary[['affordability_rank', 'location', 'avg_price_per_bedroom']].head())

    print("\nTop 5 least affordable neighborhoods:")
    print(location_summary[['affordability_rank', 'location', 'avg_price_per_bedroom']].tail())
