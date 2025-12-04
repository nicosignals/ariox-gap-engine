"""
Microsoft AppSource (Dynamics 365) scraper.
Extracts app listings from Microsoft AppSource marketplace for Dynamics 365.
"""

import json
import logging
import os
import re
import sys
import time
from datetime import datetime, timezone
from typing import List, Dict, Any, Optional
from urllib.parse import urlparse, urlencode

import requests

# Add parent directory to path for imports
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from utils.clay_webhook import push_to_clay

# Configuration
BASE_URL = "https://appsource.microsoft.com"
# API endpoint for fetching app listings
API_URL = "https://appsource.microsoft.com/api/search"
# Focus on Dynamics 365 products
DYNAMICS_PRODUCTS = [
    "dynamics-365-business-central",
    "dynamics-365-for-finance-and-operations",
    "dynamics-365-for-sales",
    "dynamics-365-for-customer-service",
    "dynamics-365-for-field-service",
    "dynamics-365-for-marketing",
    "dynamics-365-for-project-service-automation",
    "dynamics-365",
]
RATE_LIMIT_DELAY = 1.0  # seconds between requests
REQUEST_TIMEOUT = 15  # seconds
LOG_INTERVAL = 50  # Log progress every N listings
PAGE_SIZE = 50  # Results per page

logger = logging.getLogger(__name__)


def fetch_apps_page(session: requests.Session, product: str, page: int = 1) -> Dict[str, Any]:
    """
    Fetch a page of apps from the AppSource API.

    Args:
        session: Requests session
        product: Product filter (e.g., dynamics-365)
        page: Page number (1-indexed)

    Returns:
        API response data
    """
    # Build API request payload
    payload = {
        "product": product,
        "page": page,
        "pageSize": PAGE_SIZE,
        "country": "US",
        "language": "en-us",
    }

    headers = {
        "Content-Type": "application/json",
        "Accept": "application/json",
    }

    try:
        response = session.get(
            API_URL,
            params=payload,
            headers=headers,
            timeout=REQUEST_TIMEOUT
        )
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        logger.error(f"API request failed for {product} page {page}: {e}")
        return {}


def fetch_app_detail_api(session: requests.Session, app_id: str) -> Optional[Dict[str, Any]]:
    """
    Fetch detailed app information from the API.

    Args:
        session: Requests session
        app_id: Application ID

    Returns:
        App detail data or None
    """
    detail_url = f"{BASE_URL}/api/products/{app_id}"

    try:
        response = session.get(detail_url, timeout=REQUEST_TIMEOUT)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        logger.debug(f"Detail API failed for {app_id}: {e}")
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
        if domain.startswith("www."):
            domain = domain[4:]
        return domain if domain else None
    except Exception:
        return None


def discover_apps(session: requests.Session, limit: int = 0) -> List[Dict[str, Any]]:
    """
    Discover all Dynamics 365 apps from AppSource.

    Args:
        session: Requests session
        limit: Maximum apps to collect (0 = unlimited)

    Returns:
        List of app data dictionaries
    """
    all_apps = {}  # Use dict to dedupe by app ID

    for product in DYNAMICS_PRODUCTS:
        logger.info(f"Fetching apps for product: {product}")
        page = 1
        product_count = 0

        while True:
            data = fetch_apps_page(session, product, page)

            apps = data.get("apps", data.get("results", data.get("items", [])))
            if not apps:
                # Try alternative response structures
                if isinstance(data, list):
                    apps = data
                else:
                    break

            for app in apps:
                app_id = app.get("id") or app.get("appId") or app.get("productId")
                if app_id and app_id not in all_apps:
                    all_apps[app_id] = app
                    product_count += 1

            logger.info(f"  Page {page}: Found {len(apps)} apps (total for {product}: {product_count})")

            # Check if more pages
            total_count = data.get("totalCount", data.get("total", 0))
            if len(all_apps) >= total_count or len(apps) < PAGE_SIZE:
                break

            # Check limit
            if limit > 0 and len(all_apps) >= limit:
                logger.info(f"Reached limit of {limit} apps")
                break

            page += 1
            time.sleep(RATE_LIMIT_DELAY)

        if limit > 0 and len(all_apps) >= limit:
            break

        time.sleep(RATE_LIMIT_DELAY)

    result = list(all_apps.values())
    if limit > 0:
        result = result[:limit]

    logger.info(f"Discovered {len(result)} total unique apps")
    return result


def parse_app_data(app: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """
    Parse app data from API response into normalized format.

    Args:
        app: Raw app data from API

    Returns:
        Normalized listing record or None
    """
    app_id = app.get("id") or app.get("appId") or app.get("productId")
    if not app_id:
        return None

    # Build app URL
    app_url = f"{BASE_URL}/en-us/product/{app_id}"

    record = {
        "app_name": app.get("title") or app.get("displayName") or app.get("name"),
        "vendor_name": None,
        "vendor_domain": None,
        "vendor_website": None,
        "vendor_email": None,
        "vendor_location": None,
        "app_url": app_url,
        "description": app.get("description") or app.get("shortDescription") or app.get("summary"),
        "categories": [],
        "rating": None,
        "review_count": 0,
        "marketplace": "microsoft_appsource",
        "scraped_at": datetime.now(timezone.utc).isoformat()
    }

    # Extract publisher/vendor info
    publisher = app.get("publisher", {})
    if isinstance(publisher, dict):
        record["vendor_name"] = publisher.get("displayName") or publisher.get("name")
        record["vendor_website"] = publisher.get("website") or publisher.get("websiteUrl")
    elif isinstance(publisher, str):
        record["vendor_name"] = publisher

    # Also check for publisherName at top level
    if not record["vendor_name"]:
        record["vendor_name"] = app.get("publisherName") or app.get("vendorName")

    record["vendor_domain"] = extract_domain(record["vendor_website"])

    # Extract categories
    categories = app.get("categories", [])
    if isinstance(categories, list):
        for cat in categories:
            if isinstance(cat, dict):
                cat_name = cat.get("name") or cat.get("displayName")
                if cat_name:
                    record["categories"].append(cat_name)
            elif isinstance(cat, str):
                record["categories"].append(cat)

    # Also check for product types
    products = app.get("products", [])
    if isinstance(products, list):
        for prod in products:
            if isinstance(prod, dict):
                prod_name = prod.get("displayName") or prod.get("name")
                if prod_name and prod_name not in record["categories"]:
                    record["categories"].append(prod_name)
            elif isinstance(prod, str) and prod not in record["categories"]:
                record["categories"].append(prod)

    # Extract rating
    rating_data = app.get("rating", {})
    if isinstance(rating_data, dict):
        record["rating"] = rating_data.get("average") or rating_data.get("averageRating")
        record["review_count"] = rating_data.get("count") or rating_data.get("totalCount") or 0
    elif isinstance(rating_data, (int, float)):
        record["rating"] = float(rating_data)

    # Also check top-level rating fields
    if record["rating"] is None:
        record["rating"] = app.get("averageRating") or app.get("ratingAverage")

    if record["review_count"] == 0:
        record["review_count"] = app.get("ratingCount") or app.get("reviewCount") or 0

    # Only return if we got at least the app name
    if record["app_name"]:
        return record

    return None


def scrape_via_html(session: requests.Session, limit: int = 0) -> List[Dict[str, Any]]:
    """
    Alternative scraping method via HTML if API doesn't work.

    Args:
        session: Requests session
        limit: Maximum apps to collect

    Returns:
        List of app records
    """
    from bs4 import BeautifulSoup

    records = []

    for product in DYNAMICS_PRODUCTS:
        page = 1
        while True:
            url = f"{BASE_URL}/en-us/marketplace/apps"
            params = {
                "product": product,
                "page": page,
            }

            try:
                response = session.get(url, params=params, timeout=REQUEST_TIMEOUT)
                response.raise_for_status()

                soup = BeautifulSoup(response.text, "lxml")

                # Find app cards
                app_cards = soup.find_all(class_=re.compile(r"product|app|card", re.I))

                if not app_cards:
                    break

                for card in app_cards:
                    link = card.find("a", href=True)
                    if not link:
                        continue

                    href = link.get("href", "")
                    if "/product/" not in href:
                        continue

                    record = {
                        "app_name": None,
                        "vendor_name": None,
                        "vendor_domain": None,
                        "vendor_website": None,
                        "vendor_email": None,
                        "vendor_location": None,
                        "app_url": href if href.startswith("http") else f"{BASE_URL}{href}",
                        "description": None,
                        "categories": [product],
                        "rating": None,
                        "review_count": 0,
                        "marketplace": "microsoft_appsource",
                        "scraped_at": datetime.now(timezone.utc).isoformat()
                    }

                    # Extract title
                    title_elem = card.find(class_=re.compile(r"title|name", re.I))
                    if title_elem:
                        record["app_name"] = title_elem.get_text(strip=True)

                    # Extract vendor
                    vendor_elem = card.find(class_=re.compile(r"publisher|vendor|company", re.I))
                    if vendor_elem:
                        record["vendor_name"] = vendor_elem.get_text(strip=True)

                    if record["app_name"]:
                        records.append(record)

                    if limit > 0 and len(records) >= limit:
                        return records

                page += 1
                time.sleep(RATE_LIMIT_DELAY)

            except Exception as e:
                logger.error(f"HTML scraping failed for {product} page {page}: {e}")
                break

        if limit > 0 and len(records) >= limit:
            break

    return records


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

    logger.info("Starting Microsoft AppSource (Dynamics 365) scraper")
    logger.info(f"Scrape limit: {scrape_limit if scrape_limit > 0 else 'unlimited'}")

    # Create session
    session = requests.Session()
    session.headers.update({
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Accept": "application/json, text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.5",
    })

    # Try API-based discovery first
    logger.info("Attempting API-based discovery...")
    apps = discover_apps(session, limit=scrape_limit)

    # Parse app data into records
    records = []
    for i, app in enumerate(apps, 1):
        record = parse_app_data(app)
        if record:
            records.append(record)

        if i % LOG_INTERVAL == 0:
            logger.info(f"Progress: {i}/{len(apps)} apps parsed")

    # If API didn't work well, try HTML scraping
    if len(records) < 10:
        logger.info("API returned few results, trying HTML scraping...")
        html_records = scrape_via_html(session, limit=scrape_limit)
        if len(html_records) > len(records):
            records = html_records

    # Final stats
    logger.info(f"Scraping complete: {len(records)} apps extracted")

    if not records:
        logger.warning("No apps found, exiting")
        return None

    # Save to file
    output_file = save_results(records, "microsoft_appsource")

    # Push to Clay if configured
    if clay_webhook_url:
        push_to_clay(records, clay_webhook_url)
    else:
        logger.info("No CLAY_WEBHOOK_URL set, skipping webhook push")

    return output_file


if __name__ == "__main__":
    main()
