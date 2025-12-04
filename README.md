# Ariox Gap Engine

Automated marketplace scraping system for extracting SaaS vendor data from app marketplaces. Feeds into Clay tables for enrichment, then syncs to HubSpot to power ICI (Integration Coverage Index) scoring for OEM partnership targeting.

## Supported Marketplaces

| Marketplace | Listings | Method |
|-------------|----------|--------|
| Salesforce AppExchange | ~5,963 | Sitemap-based |
| HubSpot Marketplace | ~1,946 | Pagination-based |

## Setup

### Prerequisites

- Python 3.11+
- GitHub repository with Actions enabled

### Local Development

1. Clone the repository:
   ```bash
   git clone https://github.com/your-org/ariox-gap-engine.git
   cd ariox-gap-engine
   ```

2. Create a virtual environment:
   ```bash
   python -m venv venv
   source venv/bin/activate  # or `venv\Scripts\activate` on Windows
   ```

3. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```

### Environment Variables

| Variable | Required | Description |
|----------|----------|-------------|
| `CLAY_WEBHOOK_URL` | No | Webhook URL for pushing data to Clay |
| `SCRAPE_LIMIT` | No | Limit listings for testing (0 = scrape all) |

### GitHub Secrets

Add `CLAY_WEBHOOK_URL` as a repository secret to push data to Clay automatically:

1. Go to repository Settings → Secrets and variables → Actions
2. Click "New repository secret"
3. Name: `CLAY_WEBHOOK_URL`
4. Value: Your Clay webhook URL

## Usage

### Run Locally

```bash
# Salesforce AppExchange (full run)
python scrapers/salesforce_appexchange.py

# HubSpot Marketplace (full run)
python scrapers/hubspot_marketplace.py

# With limits for testing
SCRAPE_LIMIT=10 python scrapers/salesforce_appexchange.py
SCRAPE_LIMIT=10 python scrapers/hubspot_marketplace.py

# With Clay webhook
CLAY_WEBHOOK_URL=https://api.clay.com/webhook/xxx python scrapers/salesforce_appexchange.py
```

### Run via GitHub Actions

- **Automatic**: Runs weekly on Sunday at 6 AM UTC
- **Manual**: Go to Actions → Select workflow → "Run workflow" button
  - Optionally set `scrape_limit` to test with fewer listings

## Output

### JSON Schema

Each record contains:

```json
{
  "app_name": "Example App",
  "vendor_name": "Example Vendor",
  "vendor_domain": "example.com",
  "vendor_website": "https://example.com",
  "vendor_email": "contact@example.com",
  "vendor_location": "San Francisco, CA",
  "app_url": "https://appexchange.salesforce.com/...",
  "description": "App description text...",
  "categories": ["Sales", "Analytics"],
  "rating": 4.5,
  "review_count": 123,
  "marketplace": "salesforce_appexchange",
  "scraped_at": "2024-01-15T06:00:00+00:00"
}
```

**Note:** `vendor_email` and `vendor_location` are only available from Salesforce AppExchange.

### Output Files

- Saved locally as `{marketplace}_{YYYYMMDD_HHMMSS}.json`
- Uploaded as GitHub Actions artifacts (30-day retention)
- Pushed to Clay webhook if `CLAY_WEBHOOK_URL` is configured

### Expected Clay Table Structure

| Column | Type | Description |
|--------|------|-------------|
| app_name | Text | Name of the app |
| vendor_name | Text | Name of the vendor/publisher |
| vendor_domain | Text | Extracted domain (for enrichment) |
| vendor_website | URL | Full vendor website URL |
| vendor_email | Email | Contact email (Salesforce only) |
| vendor_location | Text | HQ location (Salesforce only) |
| app_url | URL | Link to marketplace listing |
| description | Long Text | App description |
| categories | Multi-select | App categories |
| rating | Number | Average rating |
| review_count | Number | Number of reviews |
| marketplace | Single-select | Source marketplace |
| scraped_at | DateTime | When the data was collected |

## Rate Limits

| Target | Delay |
|--------|-------|
| Salesforce AppExchange | 1.5s between requests |
| HubSpot Marketplace | 1.0s between requests |
| Clay Webhook | 0.5s between batches (100 records/batch) |

## Error Handling

- Individual listing failures are logged and skipped (scraping continues)
- Clay webhook failures don't fail the scraping job
- All data is saved locally regardless of webhook status
- 30-second timeout per HTTP request

## Project Structure

```
ariox-gap-engine/
├── .github/
│   └── workflows/
│       ├── salesforce_appexchange.yml
│       └── hubspot_marketplace.yml
├── scrapers/
│   ├── __init__.py
│   ├── salesforce_appexchange.py
│   └── hubspot_marketplace.py
├── utils/
│   ├── __init__.py
│   └── clay_webhook.py
├── requirements.txt
└── README.md
```

## License

MIT
