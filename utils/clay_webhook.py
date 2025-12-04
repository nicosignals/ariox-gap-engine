"""
Clay webhook utility for pushing scraped data in batches.
"""

import logging
import time
from typing import List, Dict, Any

import requests

logger = logging.getLogger(__name__)

BATCH_SIZE = 100
BATCH_DELAY = 0.5  # seconds between batches
REQUEST_TIMEOUT = 30  # seconds


def push_to_clay(
    records: List[Dict[str, Any]],
    webhook_url: str,
    batch_size: int = BATCH_SIZE,
    batch_delay: float = BATCH_DELAY
) -> int:
    """
    Push records to Clay webhook in batches.

    Args:
        records: List of records to push
        webhook_url: Clay webhook URL
        batch_size: Number of records per batch (default: 100)
        batch_delay: Delay between batches in seconds (default: 0.5)

    Returns:
        Number of successfully pushed records
    """
    if not records:
        logger.info("No records to push to Clay")
        return 0

    if not webhook_url:
        logger.warning("No webhook URL provided, skipping Clay push")
        return 0

    total_records = len(records)
    successful_count = 0

    # Split into batches
    batches = [
        records[i:i + batch_size]
        for i in range(0, total_records, batch_size)
    ]

    logger.info(f"Pushing {total_records} records in {len(batches)} batches")

    for batch_num, batch in enumerate(batches, 1):
        try:
            response = requests.post(
                webhook_url,
                json=batch,
                timeout=REQUEST_TIMEOUT,
                headers={"Content-Type": "application/json"}
            )
            response.raise_for_status()
            successful_count += len(batch)
            logger.info(f"Batch {batch_num}/{len(batches)}: Pushed {len(batch)} records")

        except requests.exceptions.RequestException as e:
            logger.error(f"Batch {batch_num}/{len(batches)} failed: {e}")
            # Continue with next batch, don't fail entirely

        # Delay between batches (except after last batch)
        if batch_num < len(batches):
            time.sleep(batch_delay)

    logger.info(f"Successfully pushed {successful_count}/{total_records} records to Clay")
    return successful_count
