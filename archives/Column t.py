import feedparser as fp
import dateutil.parser
from newspaper import Article, Config
import logging
import pandas as pd
import json
from datetime import datetime, timedelta, timezone
import os
from nltk.corpus import stopwords
from nltk.stem import WordNetLemmatizer
import string
from nltk.tokenize import word_tokenize
from unidecode import unidecode
import time
import threading
import sys
import re
import nltk
from bs4 import BeautifulSoup
from collections import defaultdict

nltk.download('wordnet')
nltk.download('omw-1.4')
nltk.download('punkt')
nltk.download('stopwords')

# Logging setup
logging.basicConfig(filename='scrapper.log', level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class CacheManager:
    def __init__(self, cache_file='article_cache_new.json'):
        self.cache_file = cache_file
        self.load_cache()

    def load_cache(self):
        logging.info("Loading cache")
        if os.path.exists(self.cache_file):
            with open(self.cache_file, 'r') as f:
                self.cache = json.load(f)
        else:
            logging.info("Cache file not found, creating a new one")
            self.cache = {}
            self.save_cache()

    def save_cache(self):
        logging.info("Saving cache")
        with open(self.cache_file, 'w') as f:
            json.dump(self.cache, f, indent=4)

    def get_article(self, url):
        return self.cache.get(url, None)

    def add_article(self, url, article_data):
        logging.info(f'Adding article to cache: {url}')
        self.cache[url] = article_data
        self.save_cache()

class Scraper:
    def __init__(self, sources, days, cache_manager):
        self.sources = sources
        self.days = days
        self.cache_manager = cache_manager

    def scrape(self):
        start_time = time.time()
        articles_dict = {}
        new_articles_count = 0
        now = datetime.now(timezone.utc)

        tzinfos = {
            'EDT': timezone(timedelta(hours=-4)),
            'EST': timezone(timedelta(hours=-5)),
            'CDT': timezone(timedelta(hours=-5)),
            'CST': timezone(timedelta(hours=-6)),
            'MDT': timezone(timedelta(hours=-6)),
            'MST': timezone(timedelta(hours=-7)),
            'PDT': timezone(timedelta(hours=-7)),
            'PST': timezone(timedelta(hours=-8)),
        }

        for source, content in self.sources.items():
            logging.info(f'Source: {source}')
            for url in content['rss']:
                logging.info(f'Processing RSS feed: {url}')
                try:
                    d = fp.parse(url)
                except Exception as e:
                    logging.error(f'Error parsing RSS feed {url}: {e}')
                    continue

                for entry in d.entries:
                    if not hasattr(entry, 'published'):
                        continue

                    try:
                        article_date = dateutil.parser.parse(getattr(entry, 'published'), tzinfos=tzinfos)
                        article_date = article_date.astimezone(timezone.utc)
                    except Exception as e:
                        logging.error(f'Error parsing article date: {e}')
                        continue

                    if now - article_date <= timedelta(days=self.days):
                        url = entry.link
                        if self.cache_manager.get_article(url):
                            continue

                        try:
                            content = Article(url, config=config)
                            content.download()
                            content.parse()
                            content.nlp()

                            article = {
                                'source': source,
                                'url': url,
                                'date': article_date.strftime('%Y-%m-%d'),
                                'time': article_date.strftime('%H:%M:%S %Z'),
                                'title': content.title,
                                'body': content.text,
                                'summary': content.summary,
                                'keywords': content.keywords,
                                'image_url': content.top_image
                            }

                            articles_dict[url] = article
                            self.cache_manager.add_article(url, article)
                            new_articles_count += 1
                        except Exception as e:
                            logging.error(f'Error downloading/parsing article: {e}')
        end_time = time.time()
        logging.info(f"Scraping completed in {end_time - start_time:.2f} seconds")
        print(f"Scraping completed in {end_time - start_time:.2f} seconds")
        print(f"Total new articles scraped: {new_articles_count}")
        return articles_dict

def clean_articles(articles_dict):
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

    for url, article in articles_dict.items():
        body = article.get('body', '')
        if body:
            article['clean_body'] = clean_text(body)

    return articles_dict

def add_days_since_publication(filepath):
    df = pd.read_json(filepath).T.reset_index(drop=True)
    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    df["t"] = (datetime.now() - df["date"]).dt.days

    older_than_180 = df[df['t'] > 180].shape[0]
    logging.info(f"Articles older than 180 days: {older_than_180}")
    print(f"📌 Articles older than 180 days: {older_than_180}")

    column_order = [
        "source", "url", "date", "time", "title", "body",
        "summary", "keywords", "image_url", "clean_body", "t"
    ]
    existing_columns = [col for col in column_order if col in df.columns]
    df = df[existing_columns + [col for col in df.columns if col not in existing_columns]]

    df.to_json(filepath, orient="index", indent=4)
    print(f"✅ Updated '{filepath}' with 't' column and reordered columns.")

# Newspaper config
config = Config()
config.fetch_images = False
config.memoize_articles = False
config.request_timeout = 10

def show_blinking_message():
    while not scraper_done:
        for state in ["scraping   ", "scraping.  ", "scraping.. ", "scraping..."]:
            if scraper_done:
                break
            sys.stdout.write(f"\r{state}")
            sys.stdout.flush()
            time.sleep(0.5)
    sys.stdout.write("\rScraping completed!\n")
    sys.stdout.flush()

if __name__ == '__main__':
    logging.info("Starting main script")
    with open('app/sources_2025-05.json', 'r') as file:
        sources = json.load(file)

    days_to_scrape = int(os.getenv('DAYS_TO_SCRAPE', 1))
    cache_manager = CacheManager()
    scraper_done = False

    blinking_thread = threading.Thread(target=show_blinking_message)
    blinking_thread.start()

    scraper = Scraper(sources, days_to_scrape, cache_manager)
    try:
        articles_dict = scraper.scrape()
        scraper_done = True

        if not articles_dict:
            logging.warning('No articles were scraped.')
        else:
            articles_dict = clean_articles(articles_dict)

            os.makedirs('news_data/raw_articles', exist_ok=True)
            today_str = datetime.utcnow().strftime('%Y-%m-%d')
            raw_path = f'news_data/raw_articles/{today_str}.json'
            master_path = 'news_data/all_articles.json'
            index_path = 'news_data/index_by_date.json'

            with open(raw_path, 'w') as f:
                json.dump(articles_dict, f, indent=2)
            logging.info(f"Saved {len(articles_dict)} articles to {raw_path}")

            if os.path.exists(master_path):
                with open(master_path, 'r') as f:
                    master_data = json.load(f)
            else:
                master_data = {}

            master_data.update(articles_dict)
            with open(master_path, 'w') as f:
                json.dump(master_data, f, indent=2)
            logging.info(f"Updated master dataset with {len(articles_dict)} articles")

            # Automatically add 't' column after update and report outdated count
            add_days_since_publication(master_path)

            if os.path.exists(index_path):
                with open(index_path, 'r') as f:
                    index_data = json.load(f)
            else:
                index_data = {'collected_dates': []}

            if today_str not in index_data['collected_dates']:
                index_data['collected_dates'].append(today_str)
                index_data['collected_dates'].sort()
                with open(index_path, 'w') as f:
                    json.dump(index_data, f, indent=2)
                logging.info(f"Updated index with date {today_str}")

            for url, article in articles_dict.items():
                cache_manager.add_article(url, article)

    except Exception as e:
        logging.error(f'An error occurred: {e}')
        scraper_done = True
