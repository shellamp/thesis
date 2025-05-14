import json
import pandas as pd
from datetime import datetime

# === CONFIG ===
REFERENCE_DATE = datetime(2025, 5, 12)
ARTICLE_FILE = "news_data/all_articles_cleaned.json"

# === LOAD ARTICLES ===
with open(ARTICLE_FILE, "r") as f:
    articles = json.load(f)

# === BUILD METADATA TABLE ===
metadata = []
for i, (k, article) in enumerate(articles.items()):
    try:
        pub_date = datetime.strptime(article["date"], "%Y-%m-%d")
        t = (REFERENCE_DATE - pub_date).days

        metadata.append({
            "article_id": f"A{i:04d}",
            "published_date": pub_date.strftime("%Y-%m-%d"),
            "t": t,
            "title": article.get("title", ""),
            "source": article.get("source", ""),
            "url": article.get("url", ""),
            "clean_body": article.get("clean_body", "")
        })
    except Exception as e:
        print(f"⚠️ Skipping article {i}: {e}")

df = pd.DataFrame(metadata)

# === ASSIGN T_BIN ===
df["t_bin"] = pd.cut(
    df["t"],
    bins=[-1, 7, 30, 90, 180, float("inf")],
    labels=["Fresh", "Recent", "Mid-aged", "Old", "Very Old"]
)

df.to_csv("Labelling/metadata_base.csv", index=False)
print("✅ metadata_base.csv created.")
