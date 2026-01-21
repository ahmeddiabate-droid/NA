"""
TELUS Health News Aggregator - Version V4
Fetches news from Federal & Provincial regulators, Pension Industry, and Caribbean sources.
Author: Saad Khan, TELUS Health Pension Consulting
Date: January 2026
"""

import json
import requests
from datetime import datetime, timedelta
from bs4 import BeautifulSoup
import feedparser
from typing import List, Dict, Optional
import os
import logging
import time
import re
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
from webdriver_manager.chrome import ChromeDriverManager
from urllib.parse import urljoin, quote

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('news_aggregator.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Configuration
OUTPUT_FILE = "extract/newsletter_latest.json"
SOURCES_FILE = "sources/news_sources.json"
MAX_ARTICLES_PER_SOURCE = 10
DAYS_LOOKBACK = 45  # Increased lookback to capture more recent news if needed
FETCH_FULL_CONTENT = True


class NewsAggregator:
    """Main class for news aggregation"""

    def __init__(self, sources_file: str = SOURCES_FILE):
        self.sources_file = sources_file
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
        })
        self.driver = None

    def _matches_keywords(self, article: Dict, source: Dict) -> bool:
        keywords = source.get('keywords', [])
        if not keywords: return True
        text = (article.get('title', '') + ' ' + article.get('content', '')).lower()
        return any(k.lower() in text for k in keywords)
        
    def _extract_date_from_text(self, text: str) -> Optional[str]:
        """Regex-based date extraction for common formats"""
        if not text: return None
        
        # Pattern for YYYY-MM-DD
        iso_match = re.search(r'\d{4}-\d{2}-\d{2}', text)
        if iso_match: return iso_match.group(0)
        
        # Pattern for Month DD, YYYY
        month_match = re.search(r'(January|February|March|April|May|June|July|August|September|October|November|December)\s+\d{1,2},?\s+\d{4}', text, re.I)
        if month_match:
            try:
                date_obj = datetime.strptime(month_match.group(0).replace(',', ''), '%B %d %Y')
                return date_obj.strftime('%Y-%m-%d')
            except: pass

        return None

 def scrape_website(self, url: str, source_name: str, category: str) -> List[Dict]:
        articles = []
        try:
            logger.info(f"Scraping HTML from {source_name}...")
            if source_name == "Other News Sources":
                return self._scrape_google_news(url, source_name, category)

            response = self.session.get(url, timeout=15)
            response.raise_for_status()
            soup = BeautifulSoup(response.text, 'html.parser')


            if "Retraite Quebec" in source_name:
                articles = self._scrape_retraitequebec(soup, url, source_name, category)


            logger.info(f"✓ Scraped {len(articles)} articles from {source_name}")
        except Exception as e:
            logger.error(f"✗ Error scraping {source_name}: {str(e)}")
        return articles

  def _scrape_retraitequebec(self, soup: BeautifulSoup, base_url: str, source_name: str, category: str) -> List[Dict]:
        articles = []
        h2_tags = soup.find_all('h2')
        for h2 in h2_tags:
            link = h2.find('a', href=True)
            if not link: continue
            title_text = self._clean_text(h2.get_text())
            if not title_text or len(title_text) < 20 or 'Showing' in title_text: continue
            href = self._fix_relative_url(link['href'], base_url)
            
            # Retraite Quebec usually has the date in a </span> or nearby
            date_str = None
            detail_div = h2.find_next_sibling("div",class_="detail")
            if detail_div:
                date_span = detail_div.find("span", class_="layout-actualites-date")
                date_str = date_span.get_text(strip=True) if date_span else None  


            article = self._create_article(title_text, href, source_name, category, "", date_str)
            if article: articles.append(article)
            if len(articles) >= MAX_ARTICLES_PER_SOURCE: break
        return articles
