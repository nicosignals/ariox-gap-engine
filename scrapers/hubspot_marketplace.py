"""
HubSpot Marketplace scraper.
Extracts app listings using Playwright for JavaScript rendering.
"""

import json
import logging
import os
import re
import sys
import time
from datetime import datetime, timezone
from typing import List, Dict, Any, Optional, Set
from urllib.parse import urljoin

from playwright.sync_api import sync_playwright, Page, Browser

# Add parent directory to path for imports
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from utils.clay_webhook import push_to_clay

# Configuration
BASE_URL = "https://ecosystem.hubspot.com"
APPS_BASE_URL = "https://ecosystem.hubspot.com/marketplace/explore?eco_PRODUCT_TYPE=APP"
MAX_PAGES = 35  # Total number of pages to scrape
RATE_LIMIT_DELAY = 1.5  # seconds between page loads
PAGE_LOAD_TIMEOUT = 5000  # milliseconds (increased for slow loads)
LOG_INTERVAL = 50  # Log progress every N listings
DEBUG_MODE = True  # Save screenshots and HTML for debugging

logger = logging.getLogger(__name__)


def create_browser() -> Browser:
    """Create a Playwright browser instance."""
    playwright = sync_playwright().start()
    browser = playwright.chromium.launch(headless=True)
    return browser


def wait_for_app_cards(page: Page) -> None:
    """Wait for app cards to load on the page."""
    try:
        # Wait for common app card selectors
        page.wait_for_selector(
            'a[href*="/marketplace/apps/"]',
            timeout=PAGE_LOAD_TIMEOUT
        )
        # Give extra time for all cards to render
        page.wait_for_timeout(2000)
    except Exception:
        # Page might not have any apps
        pass


def scroll_to_load_all(page: Page, max_scrolls: int = 20) -> None:
    """Scroll down to trigger lazy loading of all apps."""
    for _ in range(max_scrolls):
        # Get current scroll height
        prev_height = page.evaluate("document.body.scrollHeight")

        # Scroll to bottom
        page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
        page.wait_for_timeout(1000)

        # Check if we've loaded more content
        new_height = page.evaluate("document.body.scrollHeight")
        if new_height == prev_height:
            break


def extract_app_urls_from_page(page: Page) -> Set[str]:
    """Extract all app URLs from the current page."""
    urls = set()

    # Method 1: Extract from JSON-LD structured data (most reliable)
    try:
        json_ld_scripts = page.query_selector_all('script[type="application/ld+json"]')
        for script in json_ld_scripts:
            try:
                content = script.inner_text()
                data = json.loads(content)

                # Handle ItemList structure
                if data.get("@type") == "ItemList":
                    items = data.get("itemListElement", [])
                    for item in items:
                        item_data = item.get("item", {})
                        item_id = item_data.get("@id", "")
                        if "/marketplace/listing/" in item_id:
                            urls.add(item_id)
            except (json.JSONDecodeError, TypeError):
                continue
    except Exception as e:
        logger.debug(f"Error extracting JSON-LD: {e}")

    # Method 2: Extract from <a> tags with /marketplace/listing/ pattern
    try:
        links = page.query_selector_all('a[href*="/marketplace/listing/"]')
        for link in links:
            href = link.get_attribute("href")
            if href and "/marketplace/listing/" in href:
                # Skip if contains query params or hash (filter pages)
                if "?" not in href and "#" not in href:
                    full_url = urljoin(BASE_URL, href) if not href.startswith("http") else href
                    urls.add(full_url)
    except Exception as e:
        logger.debug(f"Error extracting from <a> tags: {e}")

    # Method 3: Also check for /marketplace/apps/ links (older URL format)
    try:
        links = page.query_selector_all('a[href*="/marketplace/apps/"]')
        skip_patterns = ["/all-categories", "/popular", "/new", "/free", "/apps-for-",
                        "/apps-built-for-", "/featured", "/cms", "/ecommerce", "/all"]

        for link in links:
            href = link.get_attribute("href")
            if not href or "?" in href or "#" in href:
                continue
            if any(pattern in href for pattern in skip_patterns):
                continue

            # Check it looks like an app detail page (has a slug after /apps/)
            path_parts = href.rstrip("/").split("/")
            if "apps" in path_parts:
                apps_idx = path_parts.index("apps")
                if len(path_parts) > apps_idx + 1 and path_parts[apps_idx + 1]:
                    full_url = urljoin(BASE_URL, href) if not href.startswith("http") else href
                    urls.add(full_url)
    except Exception as e:
        logger.debug(f"Error extracting from /apps/ links: {e}")

    logger.info(f"Extracted {len(urls)} app URLs from page")
    return urls


def save_debug_info(page: Page, name: str) -> None:
    """Save screenshot and HTML for debugging."""
    if not DEBUG_MODE:
        return
    try:
        # Save screenshot
        screenshot_path = f"debug_{name}.png"
        page.screenshot(path=screenshot_path, full_page=True)
        logger.info(f"Saved debug screenshot to {screenshot_path}")

        # Save HTML
        html_path = f"debug_{name}.html"
        with open(html_path, "w", encoding="utf-8") as f:
            f.write(page.content())
        logger.info(f"Saved debug HTML to {html_path}")

        # Log page title and URL
        logger.info(f"Page title: {page.title()}")
        logger.info(f"Page URL: {page.url}")

        # Log all links found on page
        all_links = page.query_selector_all("a[href]")
        logger.info(f"Total links on page: {len(all_links)}")

        # Log first 10 hrefs for inspection
        sample_hrefs = []
        for link in all_links[:10]:
            href = link.get_attribute("href")
            if href:
                sample_hrefs.append(href)
        logger.info(f"Sample hrefs: {sample_hrefs}")

    except Exception as e:
        logger.warning(f"Failed to save debug info: {e}")


def discover_app_urls(browser: Browser, limit: int = 0) -> List[str]:
    """
    Discover app URLs by paginating through all marketplace pages.

    Args:
        browser: Playwright browser instance
        limit: Maximum URLs to collect (0 = unlimited)

    Returns:
        List of app detail URLs
    """
    discovered_urls: Set[str] = set()

    # Create incognito-like context (no cookies, no storage)
    context = browser.new_context(
        user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        viewport={"width": 1920, "height": 1080},
        java_script_enabled=True,
        bypass_csp=True,  # Bypass Content Security Policy
        ignore_https_errors=True,
    )
    page = context.new_page()

    try:
        # Paginate through all pages
        for page_num in range(1, MAX_PAGES + 1):
            # Build URL for current page
            if page_num == 1:
                page_url = APPS_BASE_URL
            else:
                page_url = f"{APPS_BASE_URL}&eco_page={page_num}"

            logger.info(f"Loading page {page_num}/{MAX_PAGES}: {page_url}")

            try:
                page.goto(page_url, wait_until="domcontentloaded", timeout=PAGE_LOAD_TIMEOUT)

                # Brief wait for React to render
                page.wait_for_timeout(1000)

                wait_for_app_cards(page)

                urls = extract_app_urls_from_page(page)
                new_urls = urls - discovered_urls
                discovered_urls.update(urls)

                logger.info(f"Page {page_num}: Found {len(new_urls)} new apps (total: {len(discovered_urls)})")

                # Debug: if no apps found on first page, save debug info
                if len(urls) == 0 and page_num == 1:
                    logger.warning("No apps found on first page - saving debug info")
                    save_debug_info(page, "page_1")

                # Check if we've hit the limit
                if limit > 0 and len(discovered_urls) >= limit:
                    logger.info(f"Reached limit of {limit} URLs")
                    break

                # If no new apps found, we might have reached the end
                if len(new_urls) == 0 and page_num > 1:
                    logger.info(f"No new apps on page {page_num}, stopping pagination")
                    break

                # Rate limiting between pages
                if page_num < MAX_PAGES:
                    time.sleep(RATE_LIMIT_DELAY)

            except Exception as e:
                logger.warning(f"Failed to load page {page_num}: {e}")
                continue

    finally:
        context.close()

    result = list(discovered_urls)
    if limit > 0:
        result = result[:limit]

    return result


def extract_domain(url: Optional[str]) -> Optional[str]:
    """Extract domain from URL."""
    if not url:
        return None
    try:
        from urllib.parse import urlparse
        parsed = urlparse(url)
        domain = parsed.netloc
        if domain.startswith("www."):
            domain = domain[4:]
        return domain if domain else None
    except Exception:
        return None


def scrape_app_detail(page: Page, url: str) -> Optional[Dict[str, Any]]:
    """
    Scrape individual app detail page.

    Args:
        page: Playwright page instance
        url: App detail page URL

    Returns:
        Parsed app record or None
    """
    try:
        page.goto(url, timeout=PAGE_LOAD_TIMEOUT)

        # Wait for content to load
        page.wait_for_timeout(3000)

        # Initialize record with defaults
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
            "marketplace": "hubspot_marketplace",
            "scraped_at": datetime.now(timezone.utc).isoformat()
        }

        # Try to extract app name from page title or h1
        try:
            # Try h1 first
            h1 = page.query_selector("h1")
            if h1:
                record["app_name"] = h1.inner_text().strip()

            # Fallback to page title
            if not record["app_name"]:
                title = page.title()
                if title:
                    # Clean up title (remove " | HubSpot" suffix)
                    record["app_name"] = re.sub(r"\s*[|–-]\s*HubSpot.*$", "", title).strip()
        except Exception:
            pass

        # Try to extract description from meta tags
        try:
            desc_meta = page.query_selector('meta[name="description"]') or page.query_selector('meta[property="og:description"]')
            if desc_meta:
                record["description"] = desc_meta.get_attribute("content")
        except Exception:
            pass

        # Try to find vendor name - look for "by" text pattern or vendor links
        try:
            # Common patterns: "by Vendor Name" or vendor in a specific element
            vendor_patterns = [
                '[class*="vendor"]',
                '[class*="provider"]',
                '[class*="company"]',
                '[class*="author"]',
                '[data-testid*="vendor"]',
                '[data-testid*="provider"]',
            ]

            for pattern in vendor_patterns:
                vendor_elem = page.query_selector(pattern)
                if vendor_elem:
                    text = vendor_elem.inner_text().strip()
                    if text and len(text) < 100:  # Sanity check
                        record["vendor_name"] = text
                        break

            # If still no vendor, try to find "by X" pattern in the page
            if not record["vendor_name"]:
                content = page.content()
                by_match = re.search(r'(?:by|By|BY)\s+([A-Z][A-Za-z0-9\s&.,]+?)(?:<|$|\n)', content)
                if by_match:
                    vendor = by_match.group(1).strip()
                    if len(vendor) < 50:
                        record["vendor_name"] = vendor
        except Exception:
            pass

        # Try to extract rating
        try:
            # Look for rating elements
            rating_patterns = [
                '[class*="rating"]',
                '[class*="stars"]',
                '[aria-label*="rating"]',
            ]

            for pattern in rating_patterns:
                rating_elem = page.query_selector(pattern)
                if rating_elem:
                    text = rating_elem.inner_text().strip()
                    # Try to extract number from text like "4.5 out of 5"
                    rating_match = re.search(r'(\d+\.?\d*)', text)
                    if rating_match:
                        rating = float(rating_match.group(1))
                        if 0 <= rating <= 5:
                            record["rating"] = rating
                            break
        except Exception:
            pass

        # Try to extract review count
        try:
            content = page.content()
            review_match = re.search(r'(\d+)\s*(?:reviews?|ratings?)', content, re.I)
            if review_match:
                record["review_count"] = int(review_match.group(1))
        except Exception:
            pass

        # Try to extract categories
        try:
            # Look for category/tag elements
            cat_patterns = [
                '[class*="category"]',
                '[class*="tag"]',
                'a[href*="/marketplace/apps/"][href*="category"]',
            ]

            for pattern in cat_patterns:
                cat_elems = page.query_selector_all(pattern)
                for elem in cat_elems[:5]:  # Limit to first 5
                    text = elem.inner_text().strip()
                    if text and len(text) < 50 and text not in record["categories"]:
                        record["categories"].append(text)
        except Exception:
            pass

        # Only return if we got at least the app name
        if record["app_name"]:
            return record

        logger.warning(f"Could not extract app name from {url}")
        return None

    except Exception as e:
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

    logger.info("Starting HubSpot Marketplace scraper")
    logger.info(f"Scrape limit: {scrape_limit if scrape_limit > 0 else 'unlimited'}")

    # Initialize Playwright
    logger.info("Initializing Playwright browser...")

    with sync_playwright() as playwright:
        browser = playwright.chromium.launch(headless=True)

        try:
            # Discover app URLs
            urls = discover_app_urls(browser, limit=scrape_limit)
            logger.info(f"Discovered {len(urls)} app URLs")

            if not urls:
                logger.warning("No app URLs discovered, exiting")
                return None

            # Create a new context for scraping details
            context = browser.new_context(
                user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
            )
            page = context.new_page()

            # Scrape app details
            records = []

            for i, url in enumerate(urls, 1):
                record = scrape_app_detail(page, url)

                if record:
                    records.append(record)

                # Log progress
                if i % LOG_INTERVAL == 0:
                    logger.info(f"Progress: {i}/{len(urls)} URLs processed, {len(records)} successful")

                # Rate limiting
                if i < len(urls):
                    time.sleep(RATE_LIMIT_DELAY)

            context.close()

        finally:
            browser.close()

    # Final stats
    logger.info(f"Scraping complete: {len(records)}/{len(urls)} apps extracted")

    # Save to file
    output_file = save_results(records, "hubspot_marketplace")

    # Push to Clay if configured
    if clay_webhook_url:
        push_to_clay(records, clay_webhook_url)
    else:
        logger.info("No CLAY_WEBHOOK_URL set, skipping webhook push")

    return output_file


if __name__ == "__main__":
    main()
