import os
import json
import re
import string
from collections import defaultdict
from bs4 import BeautifulSoup
from nltk.corpus import stopwords
from nltk.tokenize import word_tokenize
from nltk.stem import WordNetLemmatizer
from unidecode import unidecode
import nltk

# Download required NLTK resources
nltk.download('wordnet')
nltk.download('omw-1.4')
nltk.download('punkt')
nltk.download('stopwords')

# === CONFIG ===
OLD_PATHS = ['data/article_cache.json', 'data/article_cache_new.json']
MASTER_PATH = 'news_data/all_articles.json'
RAW_FOLDER = 'news_data/raw_articles'

# === CLEANING FUNCTION (ENHANCED) ===
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

# === STEP 1: LOAD & COMBINE OLD ARTICLES ===
combined_articles = {}

for path in OLD_PATHS:
    if os.path.exists(path):
        with open(path, 'r') as f:
            data = json.load(f)
            print(f"🔹 Loaded {len(data)} articles from {path}")
            for url, article in data.items():
                if url in combined_articles:
                    continue  # skip duplicates
                combined_articles[url] = article

print(f"🔄 Total unique articles from both caches: {len(combined_articles)}")

# === STEP 2: FIX/RECALCULATE clean_body and 't' ===
from datetime import datetime, timezone

now = datetime.now(timezone.utc)

for url, article in combined_articles.items():
    body = article.get('body', '')
    if body:
        article['clean_body'] = clean_text(body)
        # Calculate 't' = days since publication if date exists
        pub_date = article.get('date')
        if pub_date:
            try:
                dt = datetime.strptime(pub_date, "%Y-%m-%d").replace(tzinfo=timezone.utc)
                article['t'] = (now - dt).days
            except Exception as e:
                print(f"⚠️ Could not parse date for article: {url} — {e}")

# === STEP 3: MERGE INTO MASTER ===
if os.path.exists(MASTER_PATH):
    with open(MASTER_PATH, 'r') as f:
        master_data = json.load(f)
else:
    master_data = {}

# Merge: old overwrites master if same URL
master_data.update(combined_articles)

with open(MASTER_PATH, 'w') as f:
    json.dump(master_data, f, indent=2)
print(f"✅ Master dataset updated: {len(master_data)} total articles")

# === STEP 4: SAVE ARTICLES BY DATE ===
os.makedirs(RAW_FOLDER, exist_ok=True)
articles_by_date = defaultdict(dict)

for url, article in combined_articles.items():
    date = article.get('date')
    if date:
        articles_by_date[date][url] = article

for date, articles in articles_by_date.items():
    path = os.path.join(RAW_FOLDER, f'{date}.json')
    with open(path, 'w') as f:
        json.dump(articles, f, indent=2)
    print(f"📁 Saved {len(articles)} articles to {path}")
