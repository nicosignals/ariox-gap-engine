"""
Acumatica Marketplace scraper.
Extracts app/extension listings from the Acumatica Marketplace.
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
BASE_URL = "https://marketplace.acumatica.com"
LISTINGS_URL = "https://marketplace.acumatica.com/listings"
RATE_LIMIT_DELAY = 1.5  # seconds between requests
REQUEST_TIMEOUT = 15  # seconds
LOG_INTERVAL = 50  # Log progress every N listings
MAX_PAGES = 50  # Maximum pages to paginate through

logger = logging.getLogger(__name__)


def discover_app_urls(session: requests.Session, limit: int = 0) -> List[str]:
    """
    Discover all app URLs by paginating through the listings page.

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
            # Build URL with pagination
            if page == 1:
                url = LISTINGS_URL
            else:
                url = f"{LISTINGS_URL}?page={page}"

            logger.info(f"Fetching page {page}...")
            response = session.get(url, timeout=REQUEST_TIMEOUT)
            response.raise_for_status()

            soup = BeautifulSoup(response.text, "lxml")

            # Find app listing links
            new_urls = set()

            # Look for listing cards/links
            listing_links = soup.find_all("a", href=re.compile(r"/listing/", re.I))
            for link in listing_links:
                href = link.get("href", "")
                if href:
                    full_url = urljoin(BASE_URL, href)
                    if full_url not in discovered_urls:
                        new_urls.add(full_url)

            # Also try alternative patterns
            if not new_urls:
                # Look for product/app cards
                cards = soup.find_all(class_=re.compile(r"card|listing|product|app", re.I))
                for card in cards:
                    link = card.find("a", href=True)
                    if link:
                        href = link.get("href", "")
                        if href and "/listing" in href.lower():
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


def extract_json_ld(soup: BeautifulSoup) -> Optional[Dict[str, Any]]:
    """
    Extract JSON-LD structured data from the page.

    Args:
        soup: BeautifulSoup object

    Returns:
        Parsed JSON-LD data or None
    """
    json_ld_scripts = soup.find_all("script", type="application/ld+json")

    for script in json_ld_scripts:
        try:
            data = json.loads(script.string or "")
            # Look for Product or SoftwareApplication type
            if isinstance(data, dict):
                if data.get("@type") in ["Product", "SoftwareApplication", "WebApplication"]:
                    return data
            elif isinstance(data, list):
                for item in data:
                    if isinstance(item, dict) and item.get("@type") in ["Product", "SoftwareApplication", "WebApplication"]:
                        return item
        except json.JSONDecodeError:
            continue

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
        "marketplace": "acumatica_marketplace",
        "scraped_at": datetime.now(timezone.utc).isoformat()
    }

    # Try JSON-LD first
    json_ld = extract_json_ld(soup)
    if json_ld:
        record["app_name"] = json_ld.get("name")
        record["description"] = json_ld.get("description")

        # Rating from aggregateRating
        agg_rating = json_ld.get("aggregateRating", {})
        if agg_rating:
            try:
                record["rating"] = float(agg_rating.get("ratingValue", 0))
                record["review_count"] = int(agg_rating.get("reviewCount", 0))
            except (ValueError, TypeError):
                pass

        # Vendor/brand info
        brand = json_ld.get("brand", json_ld.get("manufacturer", {}))
        if isinstance(brand, dict):
            record["vendor_name"] = brand.get("name")

    # Extract app name from h1 or title
    if not record["app_name"]:
        h1 = soup.find("h1")
        if h1:
            record["app_name"] = h1.get_text(strip=True)

    if not record["app_name"]:
        title_tag = soup.find("title")
        if title_tag:
            title = title_tag.get_text(strip=True)
            # Clean up title
            title = re.sub(r'\s*[-–|]\s*Acumatica.*$', '', title, flags=re.I)
            record["app_name"] = title

    # Extract description from meta or page content
    if not record["description"]:
        meta_desc = soup.find("meta", attrs={"name": "description"})
        if meta_desc:
            record["description"] = meta_desc.get("content", "").strip()

    if not record["description"]:
        # Look for description section
        desc_elem = soup.find(class_=re.compile(r"description|overview|summary|about", re.I))
        if desc_elem:
            record["description"] = desc_elem.get_text(strip=True)[:500]

    # Extract vendor information
    if not record["vendor_name"]:
        # Look for vendor/developer/partner sections
        vendor_patterns = [
            (class_=re.compile(r"vendor|developer|partner|company|publisher", re.I)),
        ]

        for pattern in [
            {"class_": re.compile(r"vendor|developer|partner|company|publisher", re.I)},
            {"class_": re.compile(r"provider|author|created-by", re.I)},
        ]:
            vendor_elem = soup.find(**pattern)
            if vendor_elem:
                vendor_link = vendor_elem.find("a")
                if vendor_link:
                    record["vendor_name"] = vendor_link.get_text(strip=True)
                    vendor_href = vendor_link.get("href", "")
                    if vendor_href.startswith("http") and "acumatica.com" not in vendor_href:
                        record["vendor_website"] = vendor_href
                        record["vendor_domain"] = extract_domain(vendor_href)
                else:
                    record["vendor_name"] = vendor_elem.get_text(strip=True)
                break

    # Look for "by Vendor" pattern
    if not record["vendor_name"]:
        by_pattern = re.search(r'(?:by|from|developed by|published by)\s+([A-Z][A-Za-z0-9\s&.,]+?)(?:<|$|\n)', html, re.I)
        if by_pattern:
            vendor = by_pattern.group(1).strip()
            if len(vendor) < 100:
                record["vendor_name"] = vendor

    # Look for vendor website link
    if not record["vendor_website"]:
        website_link = soup.find("a", href=re.compile(r"^https?://(?!.*acumatica\.com)"), string=re.compile(r"website|visit|home", re.I))
        if website_link:
            record["vendor_website"] = website_link.get("href")
            record["vendor_domain"] = extract_domain(record["vendor_website"])

    # Extract categories/tags
    category_elems = soup.find_all(class_=re.compile(r"category|tag|badge|label", re.I))
    for elem in category_elems[:10]:
        text = elem.get_text(strip=True)
        if text and len(text) < 50 and text not in record["categories"]:
            # Skip common non-category text
            if text.lower() not in ["view", "details", "learn more", "get", "buy"]:
                record["categories"].append(text)

    # Also look for category links
    cat_links = soup.find_all("a", href=re.compile(r"/category/|/tag/|type=|category=", re.I))
    for link in cat_links[:5]:
        text = link.get_text(strip=True)
        if text and len(text) < 50 and text not in record["categories"]:
            record["categories"].append(text)

    # Look for industry/module tags
    industry_section = soup.find(string=re.compile(r"industries?|modules?|features?", re.I))
    if industry_section:
        parent = industry_section.find_parent()
        if parent:
            tags = parent.find_all(["span", "a", "li"])
            for tag in tags[:5]:
                text = tag.get_text(strip=True)
                if text and len(text) < 50 and text not in record["categories"]:
                    record["categories"].append(text)

    # Extract rating if not from JSON-LD
    if not record["rating"]:
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
    if record["review_count"] == 0:
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

    logger.info("Starting Acumatica Marketplace scraper")
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
    output_file = save_results(records, "acumatica_marketplace")

    # Push to Clay if configured
    if clay_webhook_url:
        push_to_clay(records, clay_webhook_url)
    else:
        logger.info("No CLAY_WEBHOOK_URL set, skipping webhook push")

    return output_file


if __name__ == "__main__":
    main()
