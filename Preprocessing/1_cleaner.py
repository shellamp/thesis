import json
import os
from datetime import datetime
import string
import re
import nltk
from nltk.corpus import stopwords

# === INITIAL SETUP ===
nltk.download("stopwords")
stop_words = set(stopwords.words("english"))

# === CONFIG ===
INPUT_FILE = "news_data/all_articles.json"
OUTPUT_FILE = "news_data/all_articles_cleaned.json"
REMOVED_FILE = "news_data/removed_articles.json"
REFERENCE_DATE = datetime.strptime("2025-05-12", "%Y-%m-%d")
EXPECTED_COLUMNS = [
    "source", "url", "date", "time",
    "title", "body", "clean_body",
    "summary", "keywords", "image_url", "t"
]
GENERIC_TITLES = {
    "the guardian", "bbc", "bbc news", "cnn", "reuters", "al jazeera",
    "dw", "euronews", "new york times", "ny times", "nyt", "abc news",
    "npr", "fox news", "nbc", "cbs", "the independent", "the times",
    "home", "news", "top news", "breaking news", "latest news",
    "world news", "opinion", "politics", "contact us", "about us",
    "archives", "read more", "sections", "sport", "culture",
    "editorial", "editor’s pick", "top stories", "features",
    "headline", "headlines", "front page", "latest", "unknown", ""
}
NOISE_PHRASES = [
    "sign up", "subscribe", "newsletter promotion", "enter your email",
    "get the latest", "breaking news alerts", "privacy notice", "privacy policy",
    "cookie policy", "terms of service", "use google recaptcha", "sponsored content",
    "follow us", "skip past", "see our", "watch the full story",
    "more from this section", "you might also like", "share this article"
]

# === UTILS ===
def clean_scraping_noise_with_location(text, noise_phrases=NOISE_PHRASES, location_cutoff=0.3):
    lines = text.splitlines()
    total_lines = len(lines)
    start_index = int(total_lines * (1 - location_cutoff))
    for i in range(start_index, total_lines):
        line = lines[i].lower()
        if any(phrase in line for phrase in noise_phrases):
            return '\n'.join(lines[:i]).strip()
    return text.strip()

def clean_text(text):
    text = text.lower()
    text = text.translate(str.maketrans('', '', string.punctuation))
    text = re.sub(r'\s+', ' ', text)
    return text.strip()

def remove_stopwords(text):
    return ' '.join(word for word in text.split() if word not in stop_words)

def is_too_short(text, min_words=50):
    return len(text.strip().split()) < min_words

# === LOAD DATA ===
with open(INPUT_FILE, 'r') as f:
    articles = json.load(f)

print(f"📂 Loaded {len(articles)} articles")

# === CLEAN DATA ===
cleaned_articles = {}
removed_articles = {}
removed_unknown = 0
removed_invalid_time = 0
removed_too_short = 0
removed_generic_title = 0
fixed_missing_columns = 0
recalculated_t = 0
duplicates_removed = 0
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
            else:
                article[col] = ""
            fixed_missing_columns += 1

    # === Noise Removal from body
    raw_body = article.get("body", "")
    filtered_body = clean_scraping_noise_with_location(raw_body)
    article["body_filtered"] = filtered_body

    # === Filter: Too short?
    if is_too_short(filtered_body):
        article["remove_reason"] = "too_short"
        removed_articles[url] = article
        removed_too_short += 1
        continue

    # === Filter: Generic or meaningless title?
    title = article.get("title", "").lower().strip()
    if title in GENERIC_TITLES:
        article["remove_reason"] = "generic_title"
        removed_articles[url] = article
        removed_generic_title += 1
        continue

    # === Preprocess text fields
    article["clean_body"] = clean_text(filtered_body)
    article["clean_body_nostop"] = remove_stopwords(article["clean_body"])
    article["title_nostop"] = remove_stopwords(title)
    article["summary_nostop"] = remove_stopwords(article.get("summary", "").lower())

    # === Deduplicate AFTER cleaning (best practice)
    key = (article["title"].strip(), article["clean_body"])
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

# === PRINT SUMMARY (Simplified) ===
print("\n🧾 CLEANING SUMMARY")
print(f"📄 Loaded articles               : {len(articles)}")
print(f"✅ Articles kept                : {len(sorted_articles)}")
print(f"🗑️  Removed: missing/invalid date : {removed_unknown + removed_invalid_time}")
print(f"🗑️  Removed: too short content     : {removed_too_short}")
print(f"🗑️  Removed: generic title         : {removed_generic_title}")
print(f"🧹 Removed duplicates             : {duplicates_removed}")
print(f"💾 Saved cleaned data to         : {OUTPUT_FILE}")
