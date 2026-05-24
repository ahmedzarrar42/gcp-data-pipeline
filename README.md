# 🚀 GCP Data Pipeline & Scraping Platform

> Production-grade Python platform for web scraping, data pipelines, and automation — deployed on Google Cloud Platform.

[![Python](https://img.shields.io/badge/Python-3.11-blue?logo=python)](https://python.org)
[![GCP](https://img.shields.io/badge/GCP-Cloud%20Run-orange?logo=google-cloud)](https://cloud.google.com)
[![Docker](https://img.shields.io/badge/Docker-Containerized-blue?logo=docker)](https://docker.com)
[![License](https://img.shields.io/badge/License-MIT-green)](LICENSE)
[![CI](https://img.shields.io/badge/CI-GitHub%20Actions-black?logo=github)](https://github.com/features/actions)

---

## 📋 Overview

A scalable, containerized Python platform that handles:

- **Web Scraping** — Async multi-source scrapers with rate limiting, proxy rotation, and retry logic
- **Data Pipelines** — ETL pipelines using Google Cloud Pub/Sub, BigQuery, and Cloud Storage
- **Automation Services** — Scheduled tasks via Cloud Scheduler + Cloud Run Jobs
- **Google API Integration** — Sheets, Drive, and BigQuery APIs with service account auth
- **Observability** — Structured logging, Cloud Monitoring metrics, and alerting

---

## 🏗️ Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                        GCP Platform                          │
│                                                             │
│  ┌──────────────┐    ┌──────────────┐    ┌──────────────┐  │
│  │ Cloud        │    │  Cloud Run   │    │   BigQuery   │  │
│  │ Scheduler    │───▶│  (Scrapers)  │───▶│  (Storage)   │  │
│  └──────────────┘    └──────────────┘    └──────────────┘  │
│                             │                               │
│                             ▼                               │
│                      ┌──────────────┐                       │
│                      │  Pub/Sub     │                       │
│                      │  (Queue)     │                       │
│                      └──────┬───────┘                       │
│                             │                               │
│                             ▼                               │
│                      ┌──────────────┐    ┌──────────────┐  │
│                      │  Cloud Run   │───▶│    Cloud     │  │
│                      │  (Pipeline)  │    │   Storage    │  │
│                      └──────────────┘    └──────────────┘  │
└─────────────────────────────────────────────────────────────┘
```

---

## 🛠️ Tech Stack

| Layer | Technology |
|-------|-----------|
| Language | Python 3.11 |
| Scraping | aiohttp, BeautifulSoup4, Playwright |
| Data Pipeline | Apache Beam, Google Cloud Dataflow |
| Message Queue | Google Cloud Pub/Sub |
| Storage | BigQuery, Cloud Storage (GCS) |
| Containerization | Docker, Cloud Run |
| Scheduling | Cloud Scheduler |
| Google APIs | google-api-python-client, gspread |
| Testing | pytest, pytest-asyncio |
| CI/CD | GitHub Actions |
| Monitoring | Cloud Logging, Cloud Monitoring |

---

## 📁 Project Structure

```
gcp-data-pipeline/
├── scraper/
│   ├── __init__.py
│   ├── base_scraper.py          # Abstract base with retry, rate limiting
│   ├── async_scraper.py         # Async scraper using aiohttp
│   ├── playwright_scraper.py    # JS-rendered pages via Playwright
│   └── proxy_manager.py        # Proxy rotation manager
│
├── pipeline/
│   ├── __init__.py
│   ├── pubsub_publisher.py      # Pub/Sub message publisher
│   ├── pubsub_subscriber.py     # Pub/Sub subscriber/consumer
│   ├── bigquery_loader.py       # BigQuery data loader
│   └── gcs_handler.py          # Cloud Storage handler
│
├── automation/
│   ├── __init__.py
│   ├── scheduler.py             # Cloud Scheduler job manager
│   └── cloud_run_jobs.py        # Cloud Run Jobs trigger
│
├── api/
│   ├── __init__.py
│   ├── sheets_api.py            # Google Sheets integration
│   ├── drive_api.py             # Google Drive integration
│   └── auth.py                 # Service account authentication
│
├── tests/
│   ├── test_scraper.py
│   ├── test_pipeline.py
│   └── test_api.py
│
├── .github/workflows/
│   └── deploy.yml               # CI/CD pipeline
│
├── Dockerfile
├── docker-compose.yml
├── requirements.txt
├── pyproject.toml
└── README.md
```

---

## ⚡ Quick Start

### Prerequisites
- Python 3.11+
- Docker
- GCP Project with enabled APIs (Pub/Sub, BigQuery, Cloud Run, Cloud Storage)
- Service Account JSON key

### 1. Clone & Install

```bash
git clone https://github.com/ahmedzarrar42/gcp-data-pipeline.git
cd gcp-data-pipeline
python -m venv venv
source venv/bin/activate  # Windows: venv\Scripts\activate
pip install -r requirements.txt
```

### 2. Configure Environment

```bash
cp .env.example .env
# Edit .env with your GCP project details
```

```env
GCP_PROJECT_ID=your-project-id
GCP_REGION=europe-west1
PUBSUB_TOPIC=scraper-results
BIGQUERY_DATASET=pipeline_data
GCS_BUCKET=your-bucket-name
GOOGLE_APPLICATION_CREDENTIALS=./service-account.json
```

### 3. Run with Docker

```bash
docker-compose up --build
```

### 4. Run Scraper Locally

```python
from scraper.async_scraper import AsyncScraper

scraper = AsyncScraper(
    rate_limit=2,        # requests per second
    max_retries=3,
    timeout=30
)

results = await scraper.scrape_urls([
    "https://example.com/page1",
    "https://example.com/page2",
])
```

---

## 🔧 Core Components

### Web Scraper
- Async scraping with `aiohttp` for high throughput
- Playwright support for JavaScript-rendered pages
- Built-in rate limiting, retry with exponential backoff
- Proxy rotation support
- Structured output (JSON/CSV)

### Data Pipeline
- Pub/Sub publisher/subscriber for decoupled processing
- BigQuery streaming inserts with schema validation
- Cloud Storage for raw data archiving
- Dead letter queue for failed messages

### Google API Integration
- Google Sheets read/write with batch operations
- Google Drive file management and sharing
- Service account authentication with automatic token refresh

### Automation
- Cloud Scheduler job creation and management
- Cloud Run Jobs for heavy batch processing
- Environment-based configuration for multi-project support

---

## 🚢 Deployment

### Deploy to Cloud Run

```bash
# Build and push Docker image
docker build -t gcr.io/$GCP_PROJECT_ID/gcp-pipeline:latest .
docker push gcr.io/$GCP_PROJECT_ID/gcp-pipeline:latest

# Deploy to Cloud Run
gcloud run deploy gcp-pipeline \
  --image gcr.io/$GCP_PROJECT_ID/gcp-pipeline:latest \
  --region europe-west1 \
  --platform managed \
  --set-env-vars GCP_PROJECT_ID=$GCP_PROJECT_ID
```

### CI/CD via GitHub Actions

Every push to `main` automatically:
1. Runs tests
2. Builds Docker image
3. Pushes to Container Registry
4. Deploys to Cloud Run

---

## 🧪 Testing

```bash
# Run all tests
pytest tests/ -v

# Run with coverage
pytest tests/ --cov=. --cov-report=html

# Run async tests
pytest tests/ -v -p asyncio
```

---

## 📊 Monitoring

The platform emits structured logs to **Cloud Logging**:

```json
{
  "severity": "INFO",
  "message": "Scrape completed",
  "urls_scraped": 150,
  "duration_seconds": 12.4,
  "errors": 0,
  "timestamp": "2026-05-24T10:00:00Z"
}
```

Custom metrics available in **Cloud Monitoring**:
- `scraper/urls_per_second`
- `pipeline/messages_processed`
- `pipeline/bigquery_rows_inserted`

---

## 🔐 Security

- Service account with **least-privilege IAM roles**
- Secrets managed via **Google Secret Manager**
- No credentials in source code or Docker images
- VPC connector for private GCP resource access

---

## 📄 License

MIT License — see [LICENSE](LICENSE)

---

## 👤 Author

**Muhammad Ahmed** — Senior Backend Engineer
- 🐙 GitHub: [@ahmedzarrar42](https://github.com/ahmedzarrar42)
- 💼 LinkedIn: [muhammad-ahmed-504b3a46](https://www.linkedin.com/in/muhammad-ahmed-504b3a46/)
- 📧 ahmedzarrar42@gmail.com
