import pandas as pd

# Load the original JSON file
df = pd.read_json("news_data/all_articles.json").T.reset_index(drop=True)

# Drop the unwanted columns
df = df.drop(columns=["sentiment", "sentiment_category"], errors="ignore")

# Save the cleaned DataFrame to a new file
df.to_json("news_data/all_articles.json", orient="index", indent=4)

print("✅ Columns removed and saved as 'news_data/all_articles.json'")
