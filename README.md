# TradingEconomics Total Vehicle Sales → Weekly refresh + GitHub Pages downloads

This repo:
- Scrapes TradingEconomics *Total Vehicle Sales* charts for all countries (monthly, last 10 years)
- Writes:
  - `data/total_vehicle_sales_monthly_last_10y.xlsx`
  - `data/total_vehicle_sales_monthly_last_10y.csv.gz`
  - `data/manifest.json`
- Publishes a GitHub Pages site with **one-button download** for the full dataset
- Auto-refreshes via GitHub Actions weekly (and supports manual runs)

## Quick start (local)

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r scraper/requirements.txt

# Ensure you have Chrome/Chromedriver installed locally
python scraper/scrape_te_total_vehicle_sales.py
```

## Enable GitHub Pages

1. Repo → **Settings → Pages**
   - Source: **GitHub Actions**
2. Repo → **Settings → Actions → General**
   - Workflow permissions: **Read and write**

## Manual refresh
Actions → **Refresh dataset and deploy site** → Run workflow
