from googlesearch import search
from newspaper import Article
from bs4 import BeautifulSoup
import json
import os
import time
import re
from datetime import datetime
from collections import defaultdict
import string

# === CONFIG ===
QUERY = "site:theguardian.com after:2024-11-13 before:2025-02-11"
NUM_RESULTS = 50
OUTPUT_FOLDER = "news_data/guardian_search"

# === SETUP ===
os.makedirs(OUTPUT_FOLDER, exist_ok=True)
article_urls = []

print(f"🔍 Searching Google for: {QUERY}")

# === STEP 1: Get article URLs from Google ===
try:
    for url in search(QUERY, num_results=NUM_RESULTS):
        if url.startswith("https://www.theguardian.com/") and url not in article_urls:
            article_urls.append(url)
        time.sleep(1)  # avoid getting blocked
except Exception as e:
    print(f"⚠️ Error during search: {e}")

print(f"🔗 Found {len(article_urls)} article URLs")

# === STEP 2: Helper functions ===
def extract_date_from_url(url):
    match = re.search(r'/(\d{4})/(jan|feb|mar|apr|may|jun|jul|aug|sep|oct|nov|dec)/(\d{2})', url)
    if match:
        month_map = {
            'jan': '01', 'feb': '02', 'mar': '03', 'apr': '04',
            'may': '05', 'jun': '06', 'jul': '07', 'aug': '08',
            'sep': '09', 'oct': '10', 'nov': '11', 'dec': '12'
        }
        year, mon, day = match.groups()
        return f"{year}-{month_map[mon]}-{day}"
    return None

def extract_date_from_html(html_text):
    match = re.search(r'(\d{1,2})\s+(January|February|March|April|May|June|July|August|September|October|November|December)\s+(\d{4})', html_text)
    if match:
        try:
            return datetime.strptime(match.group(0), "%d %B %Y").strftime("%Y-%m-%d")
        except:
            return None
    return None

def clean_text(text):
    text = text.lower()
    text = text.translate(str.maketrans('', '', string.punctuation))
    text = re.sub(r'\s+', ' ', text)
    return text.strip()

# === STEP 3: Download and parse articles ===
articles = {}
print("📰 Downloading and parsing articles...")

for url in article_urls:
    try:
        article = Article(url)
        article.download()
        article.parse()
        article.nlp()

        if article.publish_date:
            pub_date = article.publish_date.strftime("%Y-%m-%d")
            pub_time = article.publish_date.strftime("%H:%M:%S")
        else:
            extracted_date = extract_date_from_url(url)
            if not extracted_date:
                soup = BeautifulSoup(article.html, "html.parser")
                html_date = extract_date_from_html(soup.get_text())
                pub_date = html_date if html_date else "unknown"
            else:
                pub_date = extracted_date
            pub_time = "00:00:00"

        clean_body = clean_text(article.text)

        articles[url] = {
            "source": "The Guardian",
            "url": url,
            "date": pub_date,
            "time": pub_time,
            "title": article.title,
            "body": article.text,
            "clean_body": clean_body,
            "summary": article.summary,
            "keywords": article.keywords,
            "image_url": article.top_image
        }

    except Exception as e:
        print(f"❌ Error processing {url}: {e}")

# === STEP 4: Save grouped raw files by publish month (Guardian-style) ===
RAW_FOLDER = "news_data/raw_articles"
MASTER_PATH = "news_data/all_articles.json"
SOURCE_TAG = "guardian"
os.makedirs(RAW_FOLDER, exist_ok=True)

grouped_articles = defaultdict(dict)

for url, article_data in articles.items():
    date = article_data["date"]
    if date != "unknown":
        try:
            month_tag = datetime.strptime(date, "%Y-%m-%d").strftime("%Y-%m")
        except:
            month_tag = "unknown"
    else:
        month_tag = "unknown"
    grouped_articles[month_tag][url] = article_data

for month_tag, data in grouped_articles.items():
    raw_file_path = f"{RAW_FOLDER}/{SOURCE_TAG}_{month_tag}.json"
    with open(raw_file_path, "w") as f:
        json.dump(data, f, indent=2)
    print(f"📂 Saved {len(data)} articles to {raw_file_path}")


# === STEP 5: Update and deduplicate master file ===
if os.path.exists(MASTER_PATH):
    with open(MASTER_PATH, 'r') as f:
        master_data = json.load(f)
else:
    master_data = {}

master_data.update(articles)

unique_articles = {}
seen_keys = set()

for url, article in master_data.items():
    key = (article.get("title", "").strip(), article.get("clean_body", "").strip())
    if key not in seen_keys:
        seen_keys.add(key)
        unique_articles[url] = article

print(f"🧹 Deduplicated: {len(master_data)} → {len(unique_articles)} articles")

with open(MASTER_PATH, "w") as f:
    json.dump(unique_articles, f, indent=2)

print(f"📦 Saved deduplicated master file with {len(unique_articles)} articles")
