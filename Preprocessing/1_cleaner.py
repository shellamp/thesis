import json
import os
from datetime import datetime
import string
import re

# === CONFIG ===
INPUT_FILE = "news_data/all_articles.json"
OUTPUT_FILE = "news_data/all_articles_cleaned.json"
REMOVED_FILE = "news_data/removed_articles.json"
EXPECTED_COLUMNS = [
    "source", "url", "date", "time",
    "title", "body", "clean_body",
    "summary", "keywords", "image_url", "t"
]
REFERENCE_DATE = datetime.strptime("2025-05-12", "%Y-%m-%d")

def clean_text(text):
    text = text.lower()
    text = text.translate(str.maketrans('', '', string.punctuation))
    text = re.sub(r'\s+', ' ', text)
    return text.strip()

# === LOAD DATA ===
with open(INPUT_FILE, 'r') as f:
    articles = json.load(f)

print(f"📂 Loaded {len(articles)} articles")

# === CLEAN DATA ===
cleaned_articles = {}
removed_articles = {}
removed_unknown = 0
removed_invalid_time = 0
fixed_missing_columns = 0
recalculated_t = 0
duplicates_removed = 0
clean_body_created = 0
seen_keys = set()

for url, article in articles.items():
    remove_reason = None

    # === FILTER: unknown date or time == 00:00:00 ===
    if article.get("date") == "unknown":
        remove_reason = "unknown_date"
        removed_unknown += 1
    elif article.get("time") == "00:00:00":
        remove_reason = "00_time"
        removed_invalid_time += 1

    if remove_reason:
        article["remove_reason"] = remove_reason
        removed_articles[url] = article
        continue

    # === Recalculate 't' ===
    try:
        pub_date = datetime.strptime(article["date"], "%Y-%m-%d")
        article["t"] = (REFERENCE_DATE - pub_date).days
        recalculated_t += 1
    except:
        article["remove_reason"] = "invalid_date_format"
        removed_articles[url] = article
        removed_unknown += 1
        continue

    # === Fill missing columns ===
    for col in EXPECTED_COLUMNS:
        if col not in article:
            if col == "keywords":
                article[col] = []
            elif col == "clean_body":
                body_text = article.get("body", "")
                article["clean_body"] = clean_text(body_text)
                clean_body_created += 1
            else:
                article[col] = ""
            fixed_missing_columns += 1

    # === Deduplicate ===
    key = (article.get("title", "").strip(), article.get("clean_body", "").strip())
    if key not in seen_keys:
        seen_keys.add(key)
        cleaned_articles[url] = article
    else:
        duplicates_removed += 1

# === SORT ARTICLES (oldest to newest) ===
sorted_articles = dict(sorted(
    cleaned_articles.items(),
    key=lambda item: item[1].get("date", "9999-99-99")
))

# === SAVE CLEANED FILE ===
with open(OUTPUT_FILE, 'w') as f:
    json.dump(sorted_articles, f, indent=2)

# === SAVE REMOVED ARTICLES ===
with open(REMOVED_FILE, 'w') as f:
    json.dump(removed_articles, f, indent=2)

# === PRINT SUMMARY ===
print("\n🧾 CLEANING SUMMARY")
print(f"✅ Total articles loaded             : {len(articles)}")
print(f"🗑️  Articles removed (unknown date)   : {removed_unknown}")
print(f"🗑️  Articles removed (00:00:00 time) : {removed_invalid_time}")
print(f"🧼 Missing columns filled            : {fixed_missing_columns}")
print(f"🧪 'clean_body' created               : {clean_body_created}")
print(f"📆 Recalculated 't' values           : {recalculated_t}")
print(f"🧹 Duplicate articles removed        : {duplicates_removed}")
print(f"📦 Final cleaned & sorted articles  : {len(sorted_articles)}")
print(f"💾 Saved cleaned file to            : {OUTPUT_FILE}")
print(f"💾 Removed articles saved to        : {REMOVED_FILE}")
