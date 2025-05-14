import json
import os
from datetime import datetime, timezone
from bs4 import BeautifulSoup
import re
import string
from nltk.corpus import stopwords
from nltk.tokenize import word_tokenize
from nltk.stem import WordNetLemmatizer
from unidecode import unidecode
import nltk

# NLTK setup
nltk.download('wordnet')
nltk.download('omw-1.4')
nltk.download('punkt')
nltk.download('stopwords')

# === CONFIG ===
MASTER_PATH = 'news_data/all_articles.json'

# === CLEANING SETUP ===
stop_words = set(stopwords.words('english'))
lemmatizer = WordNetLemmatizer()

def clean_text(text):
    text = text.lower()
    text = BeautifulSoup(text, "html.parser").get_text()
    text = re.sub(r'http\\S+|www\\S+|\\S+@\\S+', '', text)
    text = re.sub(r'[0-9]', '', text)
    text = text.translate(str.maketrans('', '', string.punctuation))
    text = re.sub(r'\\s+', ' ', text).strip()
    text = unidecode(text)
    tokens = word_tokenize(text)
    tokens = [lemmatizer.lemmatize(w) for w in tokens if w not in stop_words]
    return ' '.join(tokens)

# === LOAD DATA ===
if not os.path.exists(MASTER_PATH):
    print("❌ File not found:", MASTER_PATH)
    exit()

with open(MASTER_PATH, 'r') as f:
    data = json.load(f)

print(f"🔍 Loaded {len(data)} articles from {MASTER_PATH}")

# === CHECK FOR DUPLICATES ===
title_body_seen = set()
duplicates = []

for url, article in data.items():
    key = (article.get('title', '').strip(), article.get('body', '').strip())
    if key in title_body_seen:
        duplicates.append(url)
    else:
        title_body_seen.add(key)

print(f"🧾 Found {len(duplicates)} duplicate articles (same title + body)")

# === REMOVE DUPLICATE ARTICLES ===
for url in duplicates:
    del data[url]
print(f"🗑 Removed {len(duplicates)} duplicate articles")

# === CHECK clean_body and t ===
missing_clean_body = []
missing_t = []

now = datetime.now(timezone.utc)

for url, article in data.items():
    if 'clean_body' not in article or not article['clean_body'].strip():
        body = article.get('body', '')
        if body:
            article['clean_body'] = clean_text(body)
        else:
            missing_clean_body.append(url)

    if 't' not in article:
        pub_date = article.get('date')
        if pub_date:
            try:
                dt = datetime.strptime(pub_date, "%Y-%m-%d").replace(tzinfo=timezone.utc)
                article['t'] = (now - dt).days
                article['date'] = dt.strftime('%Y-%m-%d')
            except:
                missing_t.append(url)
        else:
            missing_t.append(url)

print(f"🧼 Fixed missing clean_body for {len(data) - len(missing_clean_body)} articles")
print(f"⏳ Fixed missing 't' for {len(data) - len(missing_t)} articles")

# === SAVE BACK TO ORIGINAL FILE ===
with open(MASTER_PATH, 'w') as f:
    json.dump(data, f, indent=2)
print(f"✅ Cleaned data saved back to {MASTER_PATH}")
