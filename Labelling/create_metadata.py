import json
import pandas as pd
from datetime import datetime

# === CONFIG ===
REFERENCE_DATE = datetime(2025, 5, 12)
ARTICLE_FILE = "news_data/all_articles_cleaned.json"
OUTPUT_CSV = "Labelling/metadata_base.csv"

# === LOAD ARTICLES ===
with open(ARTICLE_FILE, "r") as f:
    articles = json.load(f)

# === BUILD METADATA TABLE WITH LABELING COLUMNS ===
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
            "clean_body": article.get("clean_body", ""),
            # === LABELING COLUMNS ===
            "rule_label": None,
            "rule_reason": None,
            "llm_label": None,
            "llm_reason": None,
            "kg_validation_result": None,
            "final_label": None,
            "label_source": None
        })
    except Exception as e:
        print(f"⚠️ Skipping article {i}: {e}")

# === CREATE DATAFRAME ===
df = pd.DataFrame(metadata)

# === ASSIGN T_BIN FOR AGE GROUPING ===
df["t_bin"] = pd.cut(
    df["t"],
    bins=[-1, 7, 30, 90, 180, float("inf")],
    labels=["Fresh", "Recent", "Mid-aged", "Old", "Very Old"]
)

# === SAVE TO CSV ===
df.to_csv(OUTPUT_CSV, index=False)
print("✅ metadata_base.csv with labeling structure created at:", OUTPUT_CSV)
