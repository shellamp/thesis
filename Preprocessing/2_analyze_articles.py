import json
import pandas as pd
from datetime import datetime

# === CONFIG ===
INPUT_FILE = "news_data/all_articles_cleaned.json"
REFERENCE_DATE = datetime.strptime("2025-05-12", "%Y-%m-%d")

# === LOAD DATA ===
with open(INPUT_FILE, "r") as f:
    data = json.load(f)

# === CONVERT TO DATAFRAME ===
df = pd.DataFrame(data.values())

# === CONVERT DATES ===
df["date"] = pd.to_datetime(df["date"], errors="coerce")

# === T BINNING LOGIC ===
def get_t_bin(t):
    if 0 <= t <= 7:
        return "Fresh"
    elif 8 <= t <= 30:
        return "Recent"
    elif 31 <= t <= 90:
        return "Mid-aged"
    elif 91 <= t <= 180:
        return "Old"
    elif t > 180:
        return "Very Old"
    else:
        return "Invalid"

df["t"] = df["t"].astype(int)
df["t_bin"] = df["t"].apply(get_t_bin)

# === MONTHLY DISTRIBUTION ===
df["month"] = df["date"].dt.to_period("M")
monthly_distribution = df.groupby("month").size().reset_index(name="article_count")

# === BIN COUNTS ===
bin_distribution = df["t_bin"].value_counts().reindex(["Fresh", "Recent", "Mid-aged", "Old", "Very Old"]).fillna(0).astype(int)

# === BASIC STATS ===
total_articles = len(df)
unique_dates = df["date"].nunique()
date_min = df["date"].min()
date_max = df["date"].max()
missing_titles = df["title"].isnull().sum()
missing_bodies = df["body"].isnull().sum()
unique_sources = df["source"].nunique()
top_sources = df["source"].value_counts().head(10)

# === SUMMARY PRINT ===
print("\n📊 SUMMARY REPORT")
print(f"Total Articles            : {total_articles}")
print(f"Unique Publish Dates      : {unique_dates}")
print(f"Date Range                : {date_min.date()} to {date_max.date()}")
print(f"Missing Titles            : {missing_titles}")
print(f"Missing Body Texts        : {missing_bodies}")
print(f"Unique Sources            : {unique_sources}")

print("\nTop 10 Sources:")
print(top_sources)

print("\n📦 Articles by Age Bin (based on t):")
print(bin_distribution)

print("\n📅 Articles by Month:")
for _, row in monthly_distribution.iterrows():
    print(f"{row['month']}: {row['article_count']} article(s)")

print("\n🗂️  Age Bin Legend:")
print("| Bin Name     | `t` Range (days) | Publication Date Range          |")
print("| ------------ | ---------------- | ------------------------------- |")
print("| **Fresh**    | 0–7 days         | **May 5 – May 12, 2025**        |")
print("| **Recent**   | 8–30 days        | **April 12 – May 4, 2025**      |")
print("| **Mid-aged** | 31–90 days       | **Feb 11 – April 11, 2025**     |")
print("| **Old**      | 91–180 days      | **Nov 14, 2024 – Feb 10, 2025** |")
print("| **Very Old** | 181+ days        | **Before Nov 13, 2024**         |")
