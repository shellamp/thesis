import pandas as pd

# Load data
df = pd.read_json("data/cleaned_older_articles.json").T.reset_index(drop=True)

# Ensure date column is datetime
df["date"] = pd.to_datetime(df["date"], errors="coerce")

# Total number of articles
total_articles = df.shape[0]

# Date range
earliest = df["date"].min()
latest = df["date"].max()

# Count of articles per month-year
articles_by_month = df["date"].dt.to_period("M").value_counts().sort_index()

# Top 5 sources
top_sources = df["source"].value_counts().head(5)

# Average article length
avg_length = df["body"].apply(lambda x: len(x) if isinstance(x, str) else 0).mean()

# Summary output
print(f"📝 Summary of older_news_articles.json")
print(f"--------------------------------------")
print(f"Total articles         : {total_articles}")
print(f"Earliest publication   : {earliest.strftime('%Y-%m-%d')}")
print(f"Latest publication     : {latest.strftime('%Y-%m-%d')}")
print(f"Average article length : {avg_length:.1f} characters\n")

print("📅 Articles per Month:")
print(articles_by_month)

print("\n🏷️  Top 5 Sources:")
print(top_sources)
