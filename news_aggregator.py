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

    def fetch_rss_feed(self, url: str, source_name: str, category: str) -> List[Dict]:
        articles = []
        try:
            logger.info(f"Fetching RSS from {source_name}...")
            feed = feedparser.parse(url)
            cutoff_date = datetime.now() - timedelta(days=DAYS_LOOKBACK)
            
            for entry in feed.entries[:MAX_ARTICLES_PER_SOURCE]:
                pub_date = self._parse_date(entry)
                if pub_date and pub_date < cutoff_date: continue
                
                url = self._validate_url(entry.get("link", ""), source_name)
                if not url: continue

                full_content = self._fetch_full_article_content(url)
                if not full_content:
                    full_content = self._clean_html(entry.get("summary", entry.get("description", "")))

                article = {
                    "title": self._clean_text(entry.get("title", "No title")),
                    "url": url,
                    "date": pub_date.strftime("%Y-%m-%d") if pub_date else datetime.now().strftime("%Y-%m-%d"),
                    "content": full_content,
                    "source": source_name,
                    "category": category,
                    "extraction_method": "rss"
                }
                articles.append(article)
        except Exception as e:
            logger.error(f"✗ Error fetching RSS from {source_name}: {str(e)}")
        return articles
    
    def scrape_website(self, url: str, source_name: str, category: str) -> List[Dict]:
        articles = []
        try:
            logger.info(f"Scraping HTML from {source_name}...")
            if source_name == "Other News Sources":
                return self._scrape_google_news(url, source_name, category)

            response = self.session.get(url, timeout=15)
            response.raise_for_status()
            soup = BeautifulSoup(response.text, 'html.parser')

            if "FCAA" in source_name:
                articles = self._scrape_fcaa(soup, url, source_name, category)
            elif "CRA" in source_name:
                articles = self._scrape_canada_news(soup, url, source_name, category)
            elif "Retraite Quebec" in source_name:
                articles = self._scrape_retraite_quebec(soup, url, source_name, category)
            elif "OSFI" in source_name:
                articles = self._scrape_osfi(soup, url, source_name, category)
            elif "FSRAO" in source_name:
                articles = self._scrape_fsrao(soup, url, source_name, category)
            elif "BCFSA" in source_name:
                articles = self._scrape_bcfsa(soup, url, source_name, category)
            elif "CAPSA" in source_name:
                articles = self._scrape_capsa(soup, url, source_name, category)
            elif "CIA" in source_name or "Actuaries" in source_name:
                articles = self._scrape_cia(soup, url, source_name, category)
            elif "ACPM" in source_name:
                articles = self._scrape_acpm(soup, url, source_name, category)
            elif "Alberta" in source_name:
                articles = self._scrape_alberta(soup, url, source_name, category)
            else:
                articles = self._scrape_generic(soup, url, source_name, category)

            logger.info(f"✓ Scraped {len(articles)} articles from {source_name}")
        except Exception as e:
            logger.error(f"✗ Error scraping {source_name}: {str(e)}")
        return articles

    def _scrape_google_news(self, url: str, source_name: str, category: str) -> List[Dict]:
        articles = []
        try:
            html = self._fetch_with_selenium(url, wait_time=10)
            if not html: return []
            soup = BeautifulSoup(html, 'html.parser')
            items = soup.find_all('article')
            for item in items[:MAX_ARTICLES_PER_SOURCE]:
                link = item.find('a', href=True)
                if not link: continue
                title_tag = item.find('h3') or item.find('h4') or link
                title = self._clean_text(title_tag.get_text())
                href = self._fix_relative_url(link['href'], "https://news.google.com")
                
                # Try to extract date from the article element (often in a <time> tag)
                time_tag = item.find('time')
                date_str = None
                if time_tag and time_tag.has_attr('datetime'):
                    date_str = time_tag['datetime'][:10]
                elif time_tag:
                    date_str = self._extract_date_from_text(time_tag.get_text())
                
                article = self._create_article(title, href, source_name, category, "", date_str)
                if article: articles.append(article)
        except Exception as e:
            logger.error(f"Error scraping Google News: {str(e)}")
        return articles

    def _create_article(self, title: str, url: str, source_name: str, category: str, snippet: str = "", date_str: str = None) -> Dict:
        if not url or any(skip in url.lower() for skip in ['mailto:', 'tel:', 'javascript:', 'whatsapp:']):
            return {}

        full_content = self._fetch_full_article_content(url)
        
        # If no date was provided, try to extract it from the full content
        if not date_str:
            date_str = self._extract_date_from_text(full_content)
            if not date_str:
                date_str = self._extract_date_from_text(snippet)
        
        # Fallback to today if still no date
        if not date_str:
            date_str = datetime.now().strftime("%Y-%m-%d")

        if not title or len(title) < 10: return {}

        return {
            "title": title,
            "url": url,
            "date": date_str,
            "content": full_content or snippet,
            "source": source_name,
            "category": category,
            "extraction_method": "scrape"
        }

    def _scrape_fcaa(self, soup: BeautifulSoup, base_url: str, source_name: str, category: str) -> List[Dict]:
        articles = []
        news_items = soup.find_all(['h2', 'h3', 'h4'])
        for item in news_items[:MAX_ARTICLES_PER_SOURCE * 2]:
            title = self._clean_text(item.get_text())
            if len(title) < 15 or title in ['News', 'Updates', 'Search']: continue
            link = item.find('a', href=True)
            if not link:
                parent = item.find_parent(['div', 'article', 'li'])
                if parent: link = parent.find('a', href=True)
            if link:
                href = self._fix_relative_url(link['href'], base_url)
                # FCAA often has date in a span or nearby div
                parent = item.find_parent(['div', 'article', 'li'])
                date_str = None
                if parent:
                    date_elem = parent.find(class_=re.compile(r'date|published|time', re.I))
                    if date_elem: date_str = self._extract_date_from_text(date_elem.get_text())
                
                article = self._create_article(title, href, source_name, category, "", date_str)
                if article: articles.append(article)
                if len(articles) >= MAX_ARTICLES_PER_SOURCE: break
        return articles
    
    def _scrape_retraite_quebec(self, soup: BeautifulSoup,base_url:str,source_name:str, category: str) -> List[Dict]:
        articles = []
        h2_tags =soup.find_all('h2', class_ = "layout-actualites") 
        for h2 in h2_tags:
            link = h2.find("a", href=True)
            if not link: continue
            title_text = link.get_text("", strip = True)
            href = link["href"]

        content =""
        detail_div = h2.find_next_sibling("div", class_ = "detail")
        
        for s in detail_copy.find_all("span", class_="layout-actualites-date"):
            s.decompose()
               
        content = detail_copy.get_text (" ", strip = True)
                
         # Retraite Quebec usually has the date in a <time> or nearby
        date_str= None 
        
        article = self._create_article(title_text, href, source_name, category, content, date_str)
        if article: articles.append(article)
        if len(articles) >= MAX_ARTICLES_PER_SOURCE: break
    return articles


    def extract_detail_text_simple(detail_div) -> str:
        detail_copy = BeautifulSoup(str(detail_div), "html.parser")
    def _scrape_canada_news(self, soup: BeautifulSoup, base_url: str, source_name: str, category: str) -> List[Dict]:
        articles = []
        h3_tags = soup.find_all('h3')
        for h3 in h3_tags:
            link = h3.find('a', href=True)
            if not link: continue
            title_text = self._clean_text(h3.get_text())
            if not title_text or len(title_text) < 20 or 'Showing' in title_text: continue
            href = self._fix_relative_url(link['href'], base_url)
            
            # Canada.ca usually has the date in a <span> or nearby
            date_str = None
            parent = h3.find_parent('div')
            if parent:
                time_tag = parent.find('time')
                if time_tag: date_str = time_tag.get_text().strip()

            article = self._create_article(title_text, href, source_name, category, "", date_str)
            if article: articles.append(article)
            if len(articles) >= MAX_ARTICLES_PER_SOURCE: break
        return articles

    def _scrape_osfi(self, soup: BeautifulSoup, base_url: str, source_name: str, category: str) -> List[Dict]:
        articles = []
        items = soup.find_all('article')
        for item in items:
            h3 = item.find('h3')
            link = item.find('a', href=True)
            if not h3 or not link: continue
            title = self._clean_text(h3.get_text())
            if title == "News" or "Media Center" in title: continue
            url = self._fix_relative_url(link['href'], base_url)
            
            # OSFI date extraction
            date_str = None
            time_tag = item.find('time')
            if time_tag: date_str = time_tag.get_text().strip()
            
            article = self._create_article(title, url, source_name, category, self._clean_text(item.get_text())[:500], date_str)
            if article: articles.append(article)
            if len(articles) >= MAX_ARTICLES_PER_SOURCE: break
        return articles

    def _scrape_fsrao(self, soup: BeautifulSoup, base_url: str, source_name: str, category: str) -> List[Dict]:
        articles = []
        rows = soup.find_all('div', class_='views-row', limit=MAX_ARTICLES_PER_SOURCE)
        for row in rows:
            link = row.find('a', href=True)
            if link:
                url = self._fix_relative_url(link['href'], base_url)
                date_str = None
                date_elem = row.find(class_=re.compile(r'date|created|posted', re.I))
                if date_elem: date_str = self._extract_date_from_text(date_elem.get_text())
                
                article = self._create_article(self._clean_text(link.get_text()), url, source_name, category, self._clean_text(row.get_text())[:500], date_str)
                if article: articles.append(article)
        return articles

    def _scrape_bcfsa(self, soup: BeautifulSoup, base_url: str, source_name: str, category: str) -> List[Dict]:
        articles = []
        html = self._fetch_with_selenium(base_url, wait_time=15)
        if not html: return articles
        soup = BeautifulSoup(html, 'html.parser')
        
        # BCFSA structure check
        items = soup.find_all('div', class_=re.compile(r'news-item|teaser', re.I))
        if not items:
            # Fallback to links
            links = soup.find_all('a', href=True)
            seen = set()
            for link in links:
                href = link.get('href', '')
                text = link.get_text().strip()
                if '/news/' in href and len(text) > 20:
                    href = self._fix_relative_url(href.split('?')[0], base_url)
                    if href in seen: continue
                    seen.add(href)
                    article = self._create_article(text, href, source_name, category, "")
                    if article: articles.append(article)
                    if len(articles) >= MAX_ARTICLES_PER_SOURCE: break
        else:
            for item in items[:MAX_ARTICLES_PER_SOURCE]:
                link = item.find('a', href=True)
                if not link: continue
                title = self._clean_text(link.get_text())
                href = self._fix_relative_url(link['href'], base_url)
                date_elem = item.find(class_=re.compile(r'date', re.I))
                date_str = self._extract_date_from_text(date_elem.get_text()) if date_elem else None
                article = self._create_article(title, href, source_name, category, "", date_str)
                if article: articles.append(article)
        return articles

    def _scrape_capsa(self, soup: BeautifulSoup, base_url: str, source_name: str, category: str) -> List[Dict]:
        articles = []
        html = self._fetch_with_selenium(base_url, wait_time=5)
        if not html: return articles
        soup = BeautifulSoup(html, 'html.parser')
        for item in soup.find_all(['div', 'li'], class_=re.compile(r'item|news', re.I))[:MAX_ARTICLES_PER_SOURCE * 2]:
            link = item.find('a', href=True)
            if link:
                title = self._clean_text(link.get_text())
                if len(title) > 15:
                    url = self._fix_relative_url(link['href'], base_url)
                    date_str = self._extract_date_from_text(item.get_text())
                    article = self._create_article(title, url, source_name, category, "", date_str)
                    if article: articles.append(article)
                    if len(articles) >= MAX_ARTICLES_PER_SOURCE: break
        return articles

    def _scrape_cia(self, soup: BeautifulSoup, base_url: str, source_name: str, category: str) -> List[Dict]:
        articles = []
        html = self._fetch_with_selenium(base_url, wait_time=5)
        if not html: return articles
        soup = BeautifulSoup(html, 'html.parser')
        # CIA News structure: h3 or h2 titles
        for h in soup.find_all(['h2', 'h3'])[:MAX_ARTICLES_PER_SOURCE * 2]:
            link = h.find('a', href=True)
            if link:
                title = self._clean_text(h.get_text())
                if len(title) > 15:
                    url = self._fix_relative_url(link['href'], base_url)
                    parent = h.find_parent('div')
                    date_str = self._extract_date_from_text(parent.get_text()) if parent else None
                    article = self._create_article(title, url, source_name, category, "", date_str)
                    if article: articles.append(article)
                    if len(articles) >= MAX_ARTICLES_PER_SOURCE: break
        return articles

    def _scrape_acpm(self, soup: BeautifulSoup, base_url: str, source_name: str, category: str) -> List[Dict]:
        articles = []
        html = self._fetch_with_selenium(base_url, wait_time=5)
        if not html: return articles
        soup = BeautifulSoup(html, 'html.parser')
        for h in soup.find_all(['h2', 'h3'])[:MAX_ARTICLES_PER_SOURCE * 2]:
            link = h.find('a', href=True)
            if link:
                title = self._clean_text(h.get_text())
                if len(title) > 15:
                    url = self._fix_relative_url(link['href'], base_url)
                    article = self._create_article(title, url, source_name, category, "")
                    if article: articles.append(article)
                    if len(articles) >= MAX_ARTICLES_PER_SOURCE: break
        return articles

    def _scrape_alberta(self, soup: BeautifulSoup, base_url: str, source_name: str, category: str) -> List[Dict]:
        articles = []
        content = soup.find('main') or soup.find('article') or soup
        # Alberta's page is static updates
        for h in content.find_all(['h2', 'h3', 'h4']):
            title = self._clean_text(h.get_text())
            if len(title) < 10 or title in ['Search', 'Breadcrumb', 'Quick links']: continue
            parent = h.find_parent(['div', 'section', 'article'])
            if parent:
                link = parent.find('a', href=True)
                if link:
                    url = self._fix_relative_url(link['href'], base_url)
                    date_str = self._extract_date_from_text(parent.get_text())
                    article = self._create_article(title, url, source_name, category, self._clean_text(parent.get_text())[:500], date_str)
                    if article: articles.append(article)
                    if len(articles) >= MAX_ARTICLES_PER_SOURCE: break
        return articles

    def _scrape_generic(self, soup: BeautifulSoup, base_url: str, source_name: str, category: str) -> List[Dict]:
        articles = []
        items = soup.find_all('article', limit=MAX_ARTICLES_PER_SOURCE * 2)
        if not items: items = soup.find_all('div', class_=lambda x: x and any(c in str(x).lower() for c in ['news', 'item', 'teaser']), limit=MAX_ARTICLES_PER_SOURCE * 2)
        
        for item in items:
            link = item.find('a', href=True)
            title_tag = item.find(['h1', 'h2', 'h3', 'h4']) or link
            if link and title_tag:
                url = self._fix_relative_url(link['href'], base_url)
                date_str = self._extract_date_from_text(item.get_text())
                article = self._create_article(self._clean_text(title_tag.get_text()), url, source_name, category, self._clean_text(item.get_text())[:500], date_str)
                if article: articles.append(article)
                if len(articles) >= MAX_ARTICLES_PER_SOURCE: break
        return articles

    def _parse_date(self, entry) -> Optional[datetime]:
        for field in ['published_parsed', 'updated_parsed']:
            if hasattr(entry, field):
                ts = getattr(entry, field)
                if ts: return datetime(*ts[:6])
        return None

    def _clean_text(self, text: str) -> str:
        return ' '.join(text.split()).strip() if text else ""

    def _clean_html(self, html: str) -> str:
        if not html: return ""
        return self._clean_text(BeautifulSoup(html, "html.parser").get_text())

    def _validate_url(self, url: str, source_name: str) -> str:
        if not url or not url.startswith('http') or any(skip in url.lower() for skip in ['mailto:', 'tel:', 'javascript:']):
            return ""
        return url

    def _fix_relative_url(self, url: str, base_url: str) -> str:
        if not url: return ""
        if any(skip in url.lower() for skip in ['mailto:', 'tel:', 'javascript:']): return ""
        if url.startswith('http'): return url
        return urljoin(base_url, url)

    def _get_selenium_driver(self):
        if self.driver is None:
            options = webdriver.ChromeOptions()
            options.add_argument("--headless")
            options.add_argument("--no-sandbox")
            options.add_argument("--disable-dev-shm-usage")
            options.add_argument("--disable-gpu")
            options.add_argument("user-agent=Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")
            try:
                self.driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=options)
            except Exception as e:
                logger.error(f"Failed to initialize Selenium driver: {str(e)}")
                return None
        return self.driver

    def _fetch_with_selenium(self, url: str, wait_time: int = 10) -> str:
        try:
            driver = self._get_selenium_driver()
            if not driver: return ""
            driver.get(url)
            time.sleep(wait_time)
            return driver.page_source
        except Exception as e:
            logger.debug(f"Selenium fetch failed for {url}: {str(e)}")
            return ""

    def _fetch_full_article_content(self, url: str) -> str:
        if not FETCH_FULL_CONTENT or not url.startswith('http'): return ""
        try:
            clean_url = url.split('?')[0]
            response = self.session.get(clean_url, timeout=12)
            response.raise_for_status()
            soup = BeautifulSoup(response.text, 'html.parser')
            for element in soup.find_all(['script', 'style', 'nav', 'header', 'footer', 'aside', 'iframe']):
                element.decompose()
            
            content = soup.find('article') or soup.find('main') or soup.find('div', id=re.compile(r'content|main')),  soup.find('div', class_=re.compile(r'content|article|post|body')) or soup.find('body')
            
            if isinstance(content, tuple): content = content[0] or content[1] # Handle multiple finds

            if content:
                text = ' '.join(content.get_text(separator=' ', strip=True).split())
                # Filter out the date strings if they appear at the end like "Page details YYYY-MM-DD"
                text = re.sub(r'Page details\s+\d{4}-\d{2}-\d{2}.*', '', text)
                return text[:5000]
            return ""
        except: return ""

    def aggregate_all_sources(self) -> List[Dict]:
        all_articles = []
        try:
            with open(self.sources_file, 'r') as f:
                config = json.load(f)
            
            logger.info(f"Processing {len(config['sources'])} sources...")
            
            for source in config['sources']:
                try:
                    s_name, s_cat, s_type = source['name'], source['category'], source['type']
                    if s_type == 'rss':
                        articles = self.fetch_rss_feed(source.get('rss_url', source['url']), s_name, s_cat)
                    else:
                        articles = self.scrape_website(source['source_page'], s_name, s_cat)
                    
                    filtered = [a for a in articles if a and self._matches_keywords(a, source)]
                    all_articles.extend(filtered)
                except Exception as e:
                    logger.error(f"✗ Failed to process {source['name']}: {str(e)}")
        except Exception as e:
            logger.error(f"Error reading sources: {str(e)}")
        return all_articles

    def save_to_json(self, articles: List[Dict], filename: str = OUTPUT_FILE):
        output = {
            "timestamp": datetime.now().isoformat(),
            "generated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "summary": {
                "total_articles": len(articles),
                "categories": sorted(list(set(a['category'] for a in articles)))
            },
            "articles": articles
        }
            
        with open(filename, 'w', encoding='utf-8') as f:
            json.dump(output, f, indent=2, ensure_ascii=False)
        logger.info(f"✓ Saved to {filename}")

    def cleanup(self):
        if self.driver:
            self.driver.quit()

def main():
    aggregator = NewsAggregator()
    try:
        articles = aggregator.aggregate_all_sources()
        if articles:
            aggregator.save_to_json(articles)
            print(f"Complete! Found {len(articles)} articles")
        else:
            print("No articles found")
    finally:
        aggregator.cleanup()

if __name__ == "__main__":
    main()
