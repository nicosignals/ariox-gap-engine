"""
Shopify App Store scraper.
Extracts app listings from the Shopify App Store sitemap and detail pages.
"""

import json
import logging
import os
import re
import sys
import time
from datetime import datetime, timezone
from typing import List, Dict, Any, Optional
from urllib.parse import urlparse
from xml.etree import ElementTree

import requests

# Add parent directory to path for imports
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from utils.clay_webhook import push_to_clay

# Configuration
SITEMAP_INDEX_URL = "https://apps.shopify.com/sitemap.xml"
RATE_LIMIT_DELAY = 1.0  # seconds between requests
REQUEST_TIMEOUT = 10  # seconds
LOG_INTERVAL = 50  # Log progress every N listings

logger = logging.getLogger(__name__)


def fetch_sitemap_index() -> List[str]:
    """
    Fetch the sitemap index and return child sitemap URLs.

    Returns:
        List of sitemap URLs to process
    """
    logger.info(f"Fetching sitemap index from {SITEMAP_INDEX_URL}")

    response = requests.get(SITEMAP_INDEX_URL, timeout=REQUEST_TIMEOUT)
    response.raise_for_status()

    root = ElementTree.fromstring(response.content)

    # Handle XML namespace
    namespace = {"ns": "http://www.sitemaps.org/schemas/sitemap/0.9"}

    sitemap_urls = []
    for sitemap_elem in root.findall(".//ns:sitemap/ns:loc", namespace):
        url = sitemap_elem.text
        if url:
            sitemap_urls.append(url)

    logger.info(f"Found {len(sitemap_urls)} child sitemaps")
    return sitemap_urls


def fetch_app_urls_from_sitemap(sitemap_url: str) -> List[str]:
    """
    Fetch app URLs from a single sitemap.

    Args:
        sitemap_url: URL of the sitemap to parse

    Returns:
        List of app detail URLs
    """
    try:
        response = requests.get(sitemap_url, timeout=REQUEST_TIMEOUT)
        response.raise_for_status()

        root = ElementTree.fromstring(response.content)
        namespace = {"ns": "http://www.sitemaps.org/schemas/sitemap/0.9"}

        urls = []
        for url_elem in root.findall(".//ns:url/ns:loc", namespace):
            url = url_elem.text
            # Filter for app detail pages (exclude categories, collections, etc.)
            if url and "/apps/" in url:
                # Skip non-app pages
                path = urlparse(url).path
                # Valid app URLs are like /apps/app-name
                parts = path.strip("/").split("/")
                if len(parts) == 2 and parts[0] == "apps" and parts[1]:
                    # Skip special pages
                    if parts[1] not in ["collections", "categories", "partners", "browse"]:
                        urls.append(url)

        return urls

    except Exception as e:
        logger.error(f"Failed to fetch sitemap {sitemap_url}: {e}")
        return []


def fetch_all_app_urls() -> List[str]:
    """
    Fetch all app URLs from all sitemaps.

    Returns:
        List of all app detail URLs
    """
    sitemap_urls = fetch_sitemap_index()

    all_urls = []
    for sitemap_url in sitemap_urls:
        urls = fetch_app_urls_from_sitemap(sitemap_url)
        all_urls.extend(urls)
        logger.info(f"Fetched {len(urls)} URLs from {sitemap_url}")
        time.sleep(0.5)  # Brief delay between sitemap fetches

    # Deduplicate
    all_urls = list(set(all_urls))
    logger.info(f"Found {len(all_urls)} total unique app URLs")
    return all_urls


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


def extract_json_ld(html: str) -> Optional[Dict[str, Any]]:
    """
    Extract JSON-LD structured data from HTML.

    Args:
        html: Raw HTML content

    Returns:
        Parsed JSON-LD data or None
    """
    pattern = r'<script[^>]*type=["\']application/ld\+json["\'][^>]*>(.*?)</script>'
    matches = re.findall(pattern, html, re.DOTALL | re.IGNORECASE)

    for match in matches:
        try:
            data = json.loads(match.strip())
            # Look for SoftwareApplication type
            if isinstance(data, dict):
                if data.get("@type") == "SoftwareApplication":
                    return data
                # Check @graph array
                if "@graph" in data:
                    for item in data["@graph"]:
                        if isinstance(item, dict) and item.get("@type") == "SoftwareApplication":
                            return item
            elif isinstance(data, list):
                for item in data:
                    if isinstance(item, dict) and item.get("@type") == "SoftwareApplication":
                        return item
        except json.JSONDecodeError:
            continue

    return None


def parse_listing_html(html: str, url: str) -> Optional[Dict[str, Any]]:
    """
    Parse app listing data from HTML using multiple methods.

    Args:
        html: Raw HTML content
        url: Original URL for reference

    Returns:
        Normalized listing record or None
    """
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
        "marketplace": "shopify_app_store",
        "scraped_at": datetime.now(timezone.utc).isoformat()
    }

    # Try JSON-LD first (most reliable)
    json_ld = extract_json_ld(html)
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

        # Vendor/author info
        author = json_ld.get("author", {})
        if isinstance(author, dict):
            record["vendor_name"] = author.get("name")
            record["vendor_website"] = author.get("url")
            record["vendor_domain"] = extract_domain(author.get("url"))

        # Categories from applicationCategory
        app_category = json_ld.get("applicationCategory")
        if app_category:
            if isinstance(app_category, list):
                record["categories"] = app_category
            else:
                record["categories"] = [app_category]

    # Fallback: Extract from HTML meta tags
    if not record["app_name"]:
        title_match = re.search(r'<title[^>]*>([^<]+)</title>', html, re.IGNORECASE)
        if title_match:
            title = title_match.group(1).strip()
            # Remove common suffixes
            title = re.sub(r'\s*[-–|]\s*Shopify App Store.*$', '', title)
            record["app_name"] = title

    if not record["description"]:
        desc_match = re.search(r'<meta[^>]*name=["\']description["\'][^>]*content=["\']([^"\']+)["\']', html, re.IGNORECASE)
        if not desc_match:
            desc_match = re.search(r'<meta[^>]*content=["\']([^"\']+)["\'][^>]*name=["\']description["\']', html, re.IGNORECASE)
        if desc_match:
            record["description"] = desc_match.group(1).strip()

    # Extract vendor from "by Developer Name" pattern
    if not record["vendor_name"]:
        vendor_match = re.search(r'by\s+<a[^>]*>([^<]+)</a>', html, re.IGNORECASE)
        if vendor_match:
            record["vendor_name"] = vendor_match.group(1).strip()

    # Extract rating from data attributes or star ratings
    if not record["rating"]:
        rating_match = re.search(r'data-rating=["\']?([\d.]+)["\']?', html)
        if not rating_match:
            rating_match = re.search(r'(\d+\.?\d*)\s*(?:out of 5|stars|/5)', html, re.IGNORECASE)
        if rating_match:
            try:
                rating = float(rating_match.group(1))
                if 0 <= rating <= 5:
                    record["rating"] = rating
            except ValueError:
                pass

    # Extract review count
    if record["review_count"] == 0:
        review_match = re.search(r'(\d+(?:,\d+)?)\s*(?:reviews?|ratings?)', html, re.IGNORECASE)
        if review_match:
            try:
                count_str = review_match.group(1).replace(",", "")
                record["review_count"] = int(count_str)
            except ValueError:
                pass

    # Only return if we got at least the app name
    if record["app_name"]:
        return record

    return None


def scrape_listing(url: str, session: requests.Session) -> Optional[Dict[str, Any]]:
    """
    Scrape a single listing page.

    Args:
        url: Listing URL to scrape
        session: Requests session for connection reuse

    Returns:
        Parsed listing data or None
    """
    try:
        response = session.get(url, timeout=REQUEST_TIMEOUT)
        response.raise_for_status()

        return parse_listing_html(response.text, url)

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

    logger.info("Starting Shopify App Store scraper")
    logger.info(f"Scrape limit: {scrape_limit if scrape_limit > 0 else 'unlimited'}")

    # Fetch URLs from sitemaps
    urls = fetch_all_app_urls()

    # Apply limit if set
    if scrape_limit > 0:
        urls = urls[:scrape_limit]
        logger.info(f"Limited to {len(urls)} URLs for testing")

    # Scrape listings
    records = []
    session = requests.Session()
    session.headers.update({
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.5",
    })

    for i, url in enumerate(urls, 1):
        record = scrape_listing(url, session)

        if record:
            records.append(record)

        # Log progress
        if i % LOG_INTERVAL == 0:
            logger.info(f"Progress: {i}/{len(urls)} URLs processed, {len(records)} successful")

        # Rate limiting
        if i < len(urls):
            time.sleep(RATE_LIMIT_DELAY)

    # Final stats
    logger.info(f"Scraping complete: {len(records)}/{len(urls)} listings extracted")

    # Save to file
    output_file = save_results(records, "shopify_app_store")

    # Push to Clay if configured
    if clay_webhook_url:
        push_to_clay(records, clay_webhook_url)
    else:
        logger.info("No CLAY_WEBHOOK_URL set, skipping webhook push")

    return output_file


if __name__ == "__main__":
    main()
