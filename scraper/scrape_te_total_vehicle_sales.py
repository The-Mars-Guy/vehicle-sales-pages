# scraper/scrape_te_total_vehicle_sales.py
import os
import json
import time
import gzip
import warnings
from datetime import datetime, timezone
from dateutil.relativedelta import relativedelta

import pandas as pd
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, WebDriverException


warnings.filterwarnings(
    "ignore",
    message="Converting to PeriodArray/Index representation will drop timezone information.",
    category=UserWarning,
)

BASE_URL = "https://tradingeconomics.com"
METRIC_PATH = "total-vehicle-sales"

# EXACT countries to scrape (in your requested order)
TARGET_COUNTRIES = [
    "Australia",
    "Brazil",
    "Chile",
    "China",
    "Colombia",
    "India",
    "Malaysia",
    "Mexico",
    "Philippines",
    "Russia",
    "South Africa",
    "Spain",
    "Thailand",
    "Turkey",
    "United States",
]

# Slug overrides where needed (most are handled by default slugify below)
SLUG_OVERRIDES = {
    "United States": "united-states",
    "South Africa": "south-africa",
}

# Output locations (repo-root relative by default)
REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
DATA_DIR = os.environ.get("DATA_DIR", os.path.join(REPO_ROOT, "data"))
os.makedirs(DATA_DIR, exist_ok=True)

OUTPUT_XLSX = os.path.join(DATA_DIR, "total_vehicle_sales_monthly_last_10y.xlsx")
OUTPUT_CSV_GZ = os.path.join(DATA_DIR, "total_vehicle_sales_monthly_last_10y.csv.gz")
MANIFEST_JSON = os.path.join(DATA_DIR, "manifest.json")

# Past 10 years from the first day of the current month (UTC), store cutoff as naive timestamp for CSV/XLSX friendliness
now_utc = datetime.now(timezone.utc)
cutoff = (
    now_utc.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    - relativedelta(years=10)
).replace(tzinfo=None)


def slugify_country(country: str) -> str:
    if country in SLUG_OVERRIDES:
        return SLUG_OVERRIDES[country]
    # TradingEconomics slugs are typically lowercase with hyphens
    return country.strip().lower().replace(" ", "-")


def country_url(country: str) -> str:
    slug = slugify_country(country)
    return f"{BASE_URL}/{slug}/{METRIC_PATH}"


def build_driver():
    opts = Options()

    # Headless for GitHub Actions
    opts.add_argument("--headless=new")
    opts.add_argument("--window-size=1920,1080")
    opts.add_argument("--disable-blink-features=AutomationControlled")
    opts.add_argument("--no-sandbox")
    opts.add_argument("--disable-dev-shm-usage")
    opts.add_argument("--disable-gpu")
    opts.add_argument("--lang=en-US")
    opts.add_argument("--disable-extensions")
    opts.add_argument(
        "--user-agent=Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36"
    )

    # Force binary via env first (workflow sets this)
    env_bin = os.environ.get("CHROME_BINARY")
    if env_bin and os.path.exists(env_bin):
        opts.binary_location = env_bin
    else:
        for p in ("/usr/bin/google-chrome", "/usr/bin/chromium", "/usr/bin/chromium-browser"):
            if os.path.exists(p):
                opts.binary_location = p
                break
        else:
            raise RuntimeError("No Chrome/Chromium binary found on runner.")

    # Pick chromedriver explicitly if provided, else rely on PATH
    env_driver = os.environ.get("CHROMEDRIVER")
    if env_driver and os.path.exists(env_driver):
        service = Service(env_driver)
    else:
        for d in ("/usr/bin/chromedriver", "/usr/bin/chromium-driver"):
            if os.path.exists(d):
                service = Service(d)
                break
        else:
            service = Service()

    service_path = getattr(service, "_path", None) or getattr(service, "path", None)
    print(f"[driver] binary={opts.binary_location} driver={service_path}", flush=True)
    return webdriver.Chrome(service=service, options=opts)


def _debug_dump(driver, label: str):
    try:
        title = driver.title
    except Exception:
        title = "<no title>"
    try:
        url = driver.current_url
    except Exception:
        url = "<no url>"
    try:
        html = driver.page_source or ""
        snippet = html[:800].replace("\n", " ").replace("\r", " ")
    except Exception:
        snippet = "<no html>"
    print(f"[debug:{label}] title={title!r} url={url!r} html_snippet={snippet!r}", flush=True)


def wait_for_highcharts(driver, timeout=45):
    WebDriverWait(driver, timeout).until(
        lambda d: d.execute_script(
            "return typeof Highcharts !== 'undefined' && Highcharts.charts && Highcharts.charts.length > 0;"
        )
    )


def click_te_10y_button(driver):
    sel = "a.hawk-chartOptions-datePicker-cnt-btn[data-span_str='10Y']"
    try:
        btn = WebDriverWait(driver, 8).until(EC.element_to_be_clickable((By.CSS_SELECTOR, sel)))
        driver.execute_script("arguments[0].click();", btn)
        return True
    except TimeoutException:
        return False


def set_range_to_max_or_10y(driver):
    js = r"""
    function clickRange(label) {
      if (typeof Highcharts === 'undefined' || !Highcharts.charts) return false;
      for (const ch of Highcharts.charts) {
        if (!ch) continue;
        const rs = ch.rangeSelector;
        if (!rs || !rs.buttons) continue;

        for (let i = 0; i < rs.buttons.length; i++) {
          const btn = rs.buttons[i];
          const txt = (btn && btn.textStr) ? btn.textStr.toUpperCase().replace(/\s/g,'') : '';
          if (txt === label) {
            rs.clickButton(i, true);
            return true;
          }
        }
      }
      return false;
    }

    if (clickRange('MAX')) return 'MAX';
    if (clickRange('10Y')) return '10Y';
    if (clickRange('ALL')) return 'ALL';
    return null;
    """
    return driver.execute_script(js)


def extract_highcharts_series(driver):
    js = r"""
    const results = [];
    if (typeof Highcharts === 'undefined' || !Highcharts.charts) return results;

    for (const ch of Highcharts.charts) {
      if (!ch || !ch.series) continue;

      for (const s of ch.series) {
        if (!s || !s.points || s.points.length === 0) continue;

        if (s.options && (s.options.isInternal || s.options.id === 'navigator')) continue;

        for (const p of s.points) {
          if (p && typeof p.x === 'number' && typeof p.y === 'number') {
            results.push([p.x, p.y]);
          }
        }
        if (results.length > 10) return results;
      }
    }
    return results;
    """
    pts = driver.execute_script(js)
    if not pts:
        return None

    df = pd.DataFrame(pts, columns=["ts", "value"])
    df["date"] = pd.to_datetime(df["ts"], unit="ms", utc=True)
    df = df.drop(columns=["ts"]).dropna().drop_duplicates().sort_values("date")
    return df


def scrape_country(driver, country, url, retry=2):
    last_err = None
    for attempt in range(retry + 1):
        try:
            driver.get(url)
            WebDriverWait(driver, 45).until(EC.presence_of_element_located((By.TAG_NAME, "body")))
            time.sleep(2)

            # Basic block/challenge detection early
            lower = (driver.page_source or "").lower()
            if any(s in lower for s in ["captcha", "verify you are human", "attention required", "cloudflare"]):
                _debug_dump(driver, f"blocked_{slugify_country(country)}")
                raise RuntimeError("Blocked by anti-bot/challenge page.")

            wait_for_highcharts(driver, timeout=45)

            clicked = click_te_10y_button(driver)
            if clicked:
                time.sleep(2)

            if not clicked:
                chosen = set_range_to_max_or_10y(driver)
                if chosen:
                    time.sleep(2)

            df = extract_highcharts_series(driver)
            if df is None or df.empty:
                _debug_dump(driver, f"no_series_{slugify_country(country)}")
                return None

            # Normalize to month start as naive timestamps (best for CSV/XLSX)
            df["date"] = df["date"].dt.to_period("M").dt.to_timestamp()

            # Apply cutoff (naive)
            df = df[df["date"] >= cutoff]

            df["country"] = country
            df = df.drop_duplicates(subset=["country", "date"])
            return df[["country", "date", "value"]]

        except (TimeoutException, WebDriverException, Exception) as e:
            last_err = e
            time.sleep(2 + attempt)

    print(f"  [fail] {country}: {last_err}", flush=True)
    return None


def write_outputs(panel: pd.DataFrame):
    panel = panel.sort_values(["country", "date"]).reset_index(drop=True)

    with pd.ExcelWriter(OUTPUT_XLSX, engine="openpyxl") as writer:
        panel.to_excel(writer, sheet_name="panel", index=False)

    csv_bytes = panel.to_csv(index=False).encode("utf-8")
    with gzip.open(OUTPUT_CSV_GZ, "wb") as f:
        f.write(csv_bytes)

    manifest = {
        "dataset": "Total Vehicle Sales (Monthly, last 10y)",
        "source": f"{BASE_URL}/",
        "metric_path": METRIC_PATH,
        "generated_utc": datetime.now(timezone.utc).isoformat(),
        "cutoff_utc": cutoff.isoformat(),
        "row_count": int(panel.shape[0]),
        "country_count": int(panel["country"].nunique()),
        "files": {
            "xlsx": "data/total_vehicle_sales_monthly_last_10y.xlsx",
            "csv_gz": "data/total_vehicle_sales_monthly_last_10y.csv.gz",
        },
        "countries": TARGET_COUNTRIES,
    }
    with open(MANIFEST_JSON, "w", encoding="utf-8") as f:
        json.dump(manifest, f, indent=2)


def main():
    driver = build_driver()
    try:
        items = [(c, country_url(c)) for c in TARGET_COUNTRIES]
        print(f"[info] will_process={len(items)} countries", flush=True)

        all_rows = []
        start = time.time()

        for i, (country, url) in enumerate(items, 1):
            print(f"[{i}/{len(items)}] {country} -> {url}", flush=True)

            df = scrape_country(driver, country, url, retry=2)
            if df is not None and not df.empty:
                all_rows.append(df)
                print(f"  [ok] rows={len(df)}", flush=True)
            else:
                print("  [warn] no data extracted", flush=True)

            if i % 5 == 0:
                elapsed = int(time.time() - start)
                print(f"[progress] {i}/{len(items)} processed in {elapsed}s", flush=True)

            time.sleep(1.0)  # be polite

        if not all_rows:
            raise RuntimeError("No data extracted for any target country (blocked or chart not accessible).")

        panel = pd.concat(all_rows, ignore_index=True)
        write_outputs(panel)
        print(f"\nSaved:\n- {OUTPUT_XLSX}\n- {OUTPUT_CSV_GZ}\n- {MANIFEST_JSON}", flush=True)

    finally:
        driver.quit()


if __name__ == "__main__":
    main()
