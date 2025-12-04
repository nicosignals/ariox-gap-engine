"""
Salesforce AppExchange scraper.
Extracts app listings from the AppExchange sitemap and detail pages.
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
SITEMAP_URL = "https://appexchange.salesforce.com/sitemap.xml"
LISTING_URL_PATTERN = "appxListingDetail"
RATE_LIMIT_DELAY = 1.5  # seconds
REQUEST_TIMEOUT = 5  # seconds
LOG_INTERVAL = 50  # Log progress every N listings

logger = logging.getLogger(__name__)


def fetch_sitemap_urls() -> List[str]:
    """
    Fetch and parse the sitemap XML to extract listing URLs.

    Returns:
        List of URLs containing appxListingDetail
    """
    logger.info(f"Fetching sitemap from {SITEMAP_URL}")

    response = requests.get(SITEMAP_URL, timeout=REQUEST_TIMEOUT)
    response.raise_for_status()

    # Parse XML
    root = ElementTree.fromstring(response.content)

    # Handle XML namespace
    namespace = {"ns": "http://www.sitemaps.org/schemas/sitemap/0.9"}

    urls = []
    for url_elem in root.findall(".//ns:url/ns:loc", namespace):
        url = url_elem.text
        if url and LISTING_URL_PATTERN in url:
            urls.append(url)

    logger.info(f"Found {len(urls)} listing URLs in sitemap")
    return urls


def extract_window_stores(html: str) -> Optional[Dict[str, Any]]:
    """
    Extract the window.stores JSON from the page HTML.

    Args:
        html: Raw HTML content

    Returns:
        Parsed JSON data or None if not found
    """
    # Regex to find window.stores = {...};
    pattern = r"window\.stores\s*=\s*(\{.*?\});\s*(?:window\.|</script>)"
    match = re.search(pattern, html, re.DOTALL)

    if not match:
        return None

    try:
        return json.loads(match.group(1))
    except json.JSONDecodeError as e:
        logger.warning(f"Failed to parse window.stores JSON: {e}")
        return None


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
        # Remove www. prefix
        if domain.startswith("www."):
            domain = domain[4:]
        return domain if domain else None
    except Exception:
        return None


def parse_listing(stores_data: Dict[str, Any], url: str) -> Optional[Dict[str, Any]]:
    """
    Parse the listing data from the stores JSON structure.

    Args:
        stores_data: Parsed window.stores data
        url: Original URL for reference

    Returns:
        Normalized listing record or None
    """
    try:
        # Navigate to listing data
        listing = stores_data.get("LISTING", {}).get("listing", {})

        if not listing:
            return None

        # Extract publisher/vendor info
        publisher = listing.get("publisher", {})
        vendor_website = publisher.get("website")

        # Extract categories from extensions
        categories = []
        extensions = listing.get("extensions", [])
        for ext in extensions:
            data = ext.get("data", {})
            cats = data.get("listingCategories", [])
            for cat in cats:
                if isinstance(cat, dict):
                    cat_name = cat.get("name")
                    if cat_name:
                        categories.append(cat_name)
                elif isinstance(cat, str):
                    categories.append(cat)

        # Fallback to appType if no categories found
        if not categories:
            app_type = listing.get("appType")
            if app_type:
                categories = [app_type]

        # Extract rating info
        reviews_summary = listing.get("reviewsSummary", {})

        # Build normalized record
        record = {
            "app_name": listing.get("name", ""),
            "vendor_name": publisher.get("name", ""),
            "vendor_domain": extract_domain(vendor_website),
            "vendor_website": vendor_website,
            "vendor_email": publisher.get("email"),
            "vendor_location": publisher.get("hQLocation"),
            "app_url": url,
            "description": listing.get("description", ""),
            "categories": categories,
            "rating": reviews_summary.get("averageRating"),
            "review_count": reviews_summary.get("reviewCount", 0),
            "marketplace": "salesforce_appexchange",
            "scraped_at": datetime.now(timezone.utc).isoformat()
        }

        return record

    except (KeyError, TypeError) as e:
        logger.warning(f"Failed to parse listing structure: {e}")
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

        stores_data = extract_window_stores(response.text)
        if not stores_data:
            logger.warning(f"No window.stores found: {url}")
            return None

        return parse_listing(stores_data, url)

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

    logger.info("Starting Salesforce AppExchange scraper")
    logger.info(f"Scrape limit: {scrape_limit if scrape_limit > 0 else 'unlimited'}")

    # Fetch URLs from sitemap
    urls = fetch_sitemap_urls()

    # Apply limit if set
    if scrape_limit > 0:
        urls = urls[:scrape_limit]
        logger.info(f"Limited to {len(urls)} URLs for testing")

    # Scrape listings
    records = []
    session = requests.Session()
    session.headers.update({
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
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
    output_file = save_results(records, "salesforce_appexchange")

    # Push to Clay if configured
    if clay_webhook_url:
        push_to_clay(records, clay_webhook_url)
    else:
        logger.info("No CLAY_WEBHOOK_URL set, skipping webhook push")

    return output_file


if __name__ == "__main__":
    main()
