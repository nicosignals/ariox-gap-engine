"""
NetSuite SuiteApp Marketplace scraper.
Extracts app listings from the Oracle NetSuite SuiteApp marketplace.
"""

import json
import logging
import os
import re
import sys
import time
from datetime import datetime, timezone
from typing import List, Dict, Any, Optional
from urllib.parse import urlparse, urljoin

import requests
from bs4 import BeautifulSoup

# Add parent directory to path for imports
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from utils.clay_webhook import push_to_clay

# Configuration
BASE_URL = "https://www.suiteapp.com"
SEARCH_URL = "https://www.suiteapp.com/search"
RATE_LIMIT_DELAY = 1.5  # seconds between requests
REQUEST_TIMEOUT = 15  # seconds
LOG_INTERVAL = 50  # Log progress every N listings
MAX_PAGES = 100  # Maximum pages to paginate through

logger = logging.getLogger(__name__)


def discover_app_urls(session: requests.Session, limit: int = 0) -> List[str]:
    """
    Discover all app URLs by paginating through search results.

    Args:
        session: Requests session
        limit: Maximum URLs to collect (0 = unlimited)

    Returns:
        List of app detail URLs
    """
    discovered_urls = set()
    page = 1

    while page <= MAX_PAGES:
        try:
            # Build search URL with pagination
            params = {
                "page": page,
                "sort": "name",  # Sort by name for consistency
            }

            logger.info(f"Fetching page {page}...")
            response = session.get(SEARCH_URL, params=params, timeout=REQUEST_TIMEOUT)
            response.raise_for_status()

            soup = BeautifulSoup(response.text, "lxml")

            # Find app links - look for links to app detail pages
            app_links = soup.find_all("a", href=re.compile(r"^/[^/]+$"))
            new_urls = set()

            for link in app_links:
                href = link.get("href", "")
                # Filter out navigation/static pages
                if href and not any(skip in href.lower() for skip in [
                    "/search", "/login", "/register", "/about", "/contact",
                    "/privacy", "/terms", "/help", "/faq", "/blog",
                    "/partner", "/vendor", "/admin", "/category"
                ]):
                    full_url = urljoin(BASE_URL, href)
                    if full_url not in discovered_urls:
                        new_urls.add(full_url)

            # Also look for app cards/tiles with specific patterns
            app_cards = soup.find_all(class_=re.compile(r"app|product|listing|result", re.I))
            for card in app_cards:
                link = card.find("a", href=True)
                if link:
                    href = link.get("href", "")
                    if href and href.startswith("/") and href.count("/") == 1:
                        full_url = urljoin(BASE_URL, href)
                        if full_url not in discovered_urls:
                            new_urls.add(full_url)

            if not new_urls:
                logger.info(f"No new URLs found on page {page}, stopping pagination")
                break

            discovered_urls.update(new_urls)
            logger.info(f"Page {page}: Found {len(new_urls)} new URLs (total: {len(discovered_urls)})")

            # Check limit
            if limit > 0 and len(discovered_urls) >= limit:
                logger.info(f"Reached limit of {limit} URLs")
                break

            page += 1
            time.sleep(RATE_LIMIT_DELAY)

        except requests.exceptions.RequestException as e:
            logger.error(f"Failed to fetch page {page}: {e}")
            break

    result = list(discovered_urls)
    if limit > 0:
        result = result[:limit]

    logger.info(f"Discovered {len(result)} total app URLs")
    return result


def extract_domain(url: Optional[str]) -> Optional[str]:
    """
    Extract domain from a URL, stripping protocol, www, and path.

    Args:
        url: Full URL string

    Returns:
        Domain only (e.g., 'example.com')
    """
    if not url:
        return None

    try:
        parsed = urlparse(url)
        domain = parsed.netloc or parsed.path.split("/")[0]
        if domain.startswith("www."):
            domain = domain[4:]
        return domain if domain else None
    except Exception:
        return None


def parse_app_page(html: str, url: str) -> Optional[Dict[str, Any]]:
    """
    Parse app details from the detail page HTML.

    Args:
        html: Raw HTML content
        url: Original URL for reference

    Returns:
        Normalized listing record or None
    """
    soup = BeautifulSoup(html, "lxml")

    record = {
        "app_name": None,
        "vendor_name": None,
        "vendor_domain": None,
        "vendor_website": None,
        "vendor_email": None,
        "vendor_location": None,
        "app_url": url,
        "description": None,
        "categories": [],
        "rating": None,
        "review_count": 0,
        "marketplace": "netsuite_suiteapp",
        "scraped_at": datetime.now(timezone.utc).isoformat()
    }

    # Extract app name from h1 or title
    h1 = soup.find("h1")
    if h1:
        record["app_name"] = h1.get_text(strip=True)

    if not record["app_name"]:
        title_tag = soup.find("title")
        if title_tag:
            title = title_tag.get_text(strip=True)
            # Clean up title
            title = re.sub(r'\s*[-–|]\s*SuiteApp.*$', '', title, flags=re.I)
            record["app_name"] = title

    # Extract description from meta or page content
    meta_desc = soup.find("meta", attrs={"name": "description"})
    if meta_desc:
        record["description"] = meta_desc.get("content", "").strip()

    if not record["description"]:
        # Look for description in page content
        desc_elem = soup.find(class_=re.compile(r"description|summary|overview", re.I))
        if desc_elem:
            record["description"] = desc_elem.get_text(strip=True)[:500]

    # Extract vendor information
    vendor_elem = soup.find(class_=re.compile(r"vendor|developer|company|partner|provider", re.I))
    if vendor_elem:
        vendor_link = vendor_elem.find("a")
        if vendor_link:
            record["vendor_name"] = vendor_link.get_text(strip=True)
            vendor_href = vendor_link.get("href", "")
            if vendor_href.startswith("http"):
                record["vendor_website"] = vendor_href
                record["vendor_domain"] = extract_domain(vendor_href)
        else:
            record["vendor_name"] = vendor_elem.get_text(strip=True)

    # Look for "by Vendor" pattern
    if not record["vendor_name"]:
        by_pattern = re.search(r'(?:by|from|developed by)\s+([A-Z][A-Za-z0-9\s&.,]+?)(?:<|$|\n)', html, re.I)
        if by_pattern:
            vendor = by_pattern.group(1).strip()
            if len(vendor) < 100:
                record["vendor_name"] = vendor

    # Extract categories/tags
    category_elems = soup.find_all(class_=re.compile(r"category|tag|badge", re.I))
    for elem in category_elems[:10]:
        text = elem.get_text(strip=True)
        if text and len(text) < 50 and text not in record["categories"]:
            record["categories"].append(text)

    # Also look for category links
    cat_links = soup.find_all("a", href=re.compile(r"/category/|/tag/|category=", re.I))
    for link in cat_links[:5]:
        text = link.get_text(strip=True)
        if text and len(text) < 50 and text not in record["categories"]:
            record["categories"].append(text)

    # Extract rating
    rating_elem = soup.find(class_=re.compile(r"rating|stars|score", re.I))
    if rating_elem:
        rating_text = rating_elem.get_text()
        rating_match = re.search(r'(\d+\.?\d*)', rating_text)
        if rating_match:
            try:
                rating = float(rating_match.group(1))
                if 0 <= rating <= 5:
                    record["rating"] = rating
            except ValueError:
                pass

    # Extract review count
    review_match = re.search(r'(\d+)\s*(?:reviews?|ratings?)', html, re.I)
    if review_match:
        try:
            record["review_count"] = int(review_match.group(1))
        except ValueError:
            pass

    # Only return if we got at least the app name
    if record["app_name"]:
        return record

    return None


def scrape_app(url: str, session: requests.Session) -> Optional[Dict[str, Any]]:
    """
    Scrape a single app detail page.

    Args:
        url: App URL to scrape
        session: Requests session for connection reuse

    Returns:
        Parsed app data or None
    """
    try:
        response = session.get(url, timeout=REQUEST_TIMEOUT)
        response.raise_for_status()

        return parse_app_page(response.text, url)

    except requests.exceptions.RequestException as e:
        logger.error(f"Request failed for {url}: {e}")
        return None


def save_results(records: List[Dict[str, Any]], marketplace: str) -> str:
    """
    Save results to a timestamped JSON file.

    Args:
        records: List of scraped records
        marketplace: Marketplace name for filename

    Returns:
        Output filename
    """
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"{marketplace}_{timestamp}.json"

    with open(filename, "w", encoding="utf-8") as f:
        json.dump(records, f, indent=2, ensure_ascii=False)

    logger.info(f"Saved {len(records)} records to {filename}")
    return filename


def main():
    """Main entry point for the scraper."""
    # Configure logging
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s"
    )

    # Get configuration from environment
    clay_webhook_url = os.environ.get("CLAY_WEBHOOK_URL")
    scrape_limit = int(os.environ.get("SCRAPE_LIMIT", "0"))

    logger.info("Starting NetSuite SuiteApp scraper")
    logger.info(f"Scrape limit: {scrape_limit if scrape_limit > 0 else 'unlimited'}")

    # Create session
    session = requests.Session()
    session.headers.update({
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.5",
    })

    # Discover app URLs
    urls = discover_app_urls(session, limit=scrape_limit)

    if not urls:
        logger.warning("No app URLs discovered, exiting")
        return None

    # Scrape app details
    records = []

    for i, url in enumerate(urls, 1):
        record = scrape_app(url, session)

        if record:
            records.append(record)

        # Log progress
        if i % LOG_INTERVAL == 0:
            logger.info(f"Progress: {i}/{len(urls)} URLs processed, {len(records)} successful")

        # Rate limiting
        if i < len(urls):
            time.sleep(RATE_LIMIT_DELAY)

    # Final stats
    logger.info(f"Scraping complete: {len(records)}/{len(urls)} apps extracted")

    # Save to file
    output_file = save_results(records, "netsuite_suiteapp")

    # Push to Clay if configured
    if clay_webhook_url:
        push_to_clay(records, clay_webhook_url)
    else:
        logger.info("No CLAY_WEBHOOK_URL set, skipping webhook push")

    return output_file


if __name__ == "__main__":
    main()
