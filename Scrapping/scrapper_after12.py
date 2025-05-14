import feedparser as fp
import dateutil.parser
from newspaper import Article, Config
import logging
import json
from datetime import datetime, timedelta, timezone
import os
from nltk.corpus import stopwords
from nltk.stem import WordNetLemmatizer
import string
from nltk.tokenize import word_tokenize
from unidecode import unidecode
import re
from bs4 import BeautifulSoup
import nltk

# === DOWNLOAD NLTK RESOURCES ===
nltk.download('wordnet')
nltk.download('omw-1.4')
nltk.download('punkt')
nltk.download('stopwords')

# === LOGGING CONFIG ===
logging.basicConfig(filename='scrapper.log', level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# === NEWSPAPER CONFIG ===
config = Config()
config.fetch_images = False
config.memoize_articles = False
config.request_timeout = 10

# === TEXT CLEANING SETUP ===
stop_words = set(stopwords.words('english'))
lemmatizer = WordNetLemmatizer()

def clean_text(text):
    text = text.lower()
    text = BeautifulSoup(text, "html.parser").get_text()
    text = re.sub(r'http\S+|www\S+|\S+@\S+', '', text)
    text = re.sub(r'[0-9]', '', text)
    text = text.translate(str.maketrans('', '', string.punctuation))
    text = re.sub(r'\s+', ' ', text).strip()
    text = unidecode(text)
    tokens = word_tokenize(text)
    tokens = [lemmatizer.lemmatize(w) for w in tokens if w not in stop_words]
    return ' '.join(tokens)

def scrape_articles(sources, days):
    now = datetime.now(timezone.utc)
    articles = {}
    new_count = 0
    tzinfos = {
        'EDT': timezone(timedelta(hours=-4)), 'EST': timezone(timedelta(hours=-5)),
        'CDT': timezone(timedelta(hours=-5)), 'CST': timezone(timedelta(hours=-6)),
        'MDT': timezone(timedelta(hours=-6)), 'MST': timezone(timedelta(hours=-7)),
        'PDT': timezone(timedelta(hours=-7)), 'PST': timezone(timedelta(hours=-8)),
    }

    for source, content in sources.items():
        for url in content['rss']:
            try:
                d = fp.parse(url)
                for entry in d.entries:
                    if not hasattr(entry, 'published'):
                        continue
                    try:
                        pub_date = dateutil.parser.parse(entry.published, tzinfos=tzinfos).astimezone(timezone.utc)
                    except:
                        continue
                    if now - pub_date > timedelta(days=days):
                        continue
                    link = entry.link
                    if link in articles:
                        continue
                    try:
                        article = Article(link, config=config)
                        article.download()
                        article.parse()
                        article.nlp()
                        body = article.text
                        clean_body = clean_text(body)
                        t = (now - pub_date).days

                        articles[link] = {
                            'source': source,
                            'url': link,
                            'date': pub_date.strftime('%Y-%m-%d'),
                            'time': pub_date.strftime('%H:%M:%S %Z'),
                            'title': article.title,
                            'body': body,
                            'summary': article.summary,
                            'keywords': article.keywords,
                            'image_url': article.top_image,
                            'clean_body': clean_body,
                            't': t
                        }
                        new_count += 1
                    except Exception as e:
                        logging.warning(f"Error processing article {link}: {e}")
            except Exception as e:
                logging.warning(f"Error fetching RSS {url}: {e}")
    return articles, new_count

def save_new_articles_only(new_articles):
    folder_path = "news_data/after_12_May_25"
    os.makedirs(folder_path, exist_ok=True)
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    file_path = f"{folder_path}/{today}.json"
    with open(file_path, 'w') as f:
        json.dump(new_articles, f, indent=2)
    return len(new_articles)

# === MAIN SCRIPT ===
if __name__ == '__main__':
    logging.info("Start scraping script (daily save only)")
    with open('app/sources_2025-05.json', 'r') as file:
        sources = json.load(file)

    days = int(os.getenv('DAYS_TO_SCRAPE', 1))
    articles, new_count = scrape_articles(sources, days)

    if not articles:
        print("No new articles scraped.")
        logging.info("No new articles scraped.")
    else:
        saved = save_new_articles_only(articles)
        print(f"✅ {new_count} new articles scraped.")
        print(f"💾 Saved {saved} articles to daily file only.")
        logging.info(f"Scraped {new_count} articles, saved {saved} to daily file.")
