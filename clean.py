import json
import pandas as pd
from datetime import datetime, timezone

def fix_all_articles_date(filepath="news_data/all_articles.json"):
    # Load the data
    df = pd.read_json(filepath).T.reset_index(drop=True)

    # Fix the date column by casting to string first
    df["date"] = pd.to_datetime(df["date"].astype(str), errors="coerce")

    # Make sure date column is timezone-aware (UTC)
    df["date"] = df["date"].dt.tz_localize("UTC", ambiguous='NaT', nonexistent='NaT')

    # Recalculate 't' = days since publication
    df["t"] = (datetime.now(timezone.utc) - df["date"]).dt.days

    # Convert date back to readable string format
    df["date"] = df["date"].dt.strftime('%Y-%m-%d')

    # Reorder columns if needed
    column_order = [
        "source", "url", "date", "time", "title", "body",
        "summary", "keywords", "image_url", "clean_body", "t"
    ]
    existing_columns = [col for col in column_order if col in df.columns]
    df = df[existing_columns + [col for col in df.columns if col not in existing_columns]]

    # Save cleaned result
    df.to_json(filepath, orient="index", indent=4)
    print("✅ Dates and 't' values fixed in 'all_articles.json'.")

# Run it
fix_all_articles_date()
