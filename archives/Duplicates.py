import pandas as pd

# Load the cleaned JSON file
df = pd.read_json("data/older_news_articles.json").T.reset_index(drop=True)

# Check for duplicate URLs
duplicate_urls = df[df.duplicated(subset=["url"], keep=False)]

# Print summary
print(f"🔍 Found {duplicate_urls.shape[0]} duplicate entries based on URL")
print(duplicate_urls[["title", "url"]].sort_values(by="url").head(10))
