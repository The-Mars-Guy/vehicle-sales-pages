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

SLUG_OVERRIDES = {
    "United States": "united-states",
    "South Africa": "south-africa",
}

# Repo paths
REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
DATA_DIR = os.environ.get("DATA_DIR", os.path.join(REPO_ROOT, "data"))
os.makedirs(DATA_DIR, exist_ok=True)

DEBUG_DIR = os.path.join(DATA_DIR, "debug")
os.makedirs(DEBUG_DIR, exist_ok=True)

LATEST_DIR = os.path.join(DATA_DIR, "latest")
os.makedirs(LATEST_DIR, exist_ok=True)

# Master (append/upsert) outputs
MASTER_CSV_GZ = os.path.join(LATEST_DIR, "master_total_vehicle_sales.csv.gz")
MASTER_XLSX = os.path.join(LATEST_DIR, "master_total_vehicle_sales.xlsx")
MASTER_MANIFEST = os.path.join(LATEST_DIR, "manifest.json")

# Optional: also publish "latest 10y" view for convenience
LATEST10Y_XLSX = os.path.join(LATEST_DIR, "total_vehicle_sales_monthly_last_10y.xlsx")
LATEST10Y_CSV_GZ = os.path.join(LATEST_DIR, "total_vehicle_sales_monthly_last_10y.csv.gz")

# Cutoff used ONLY for the "latest 10y" view. Master keeps everything ever seen.
now_utc = datetime.now(timezone.utc)
cutoff_10y = (
    now_utc.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    - relativedelta(years=10)
).replace(tzinfo=None)


def slugify_country(country: str) -> str:
    if country in SLUG_OVERRIDES:
        return SLUG_OVERRIDES[country]
    return country.strip().lower().replace(" ", "-")


def country_url(country: str) -> str:
    return f"{BASE_URL}/{slugify_country(country)}/{METRIC_PATH}"


def build_driver():
    opts = Options()
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

    # Chrome binary set by workflow
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

    # Chromedriver set by workflow
    env_driver = os.environ.get("CHROMEDRIVER")
    if env_driver and os.path.exists(env_driver):
        service = Service(env_driver)
    else:
        service = Service()

    service_path = getattr(service, "_path", None) or getattr(service, "path", None)
    print(f"[driver] binary={opts.binary_location} driver={service_path}", flush=True)
    return webdriver.Chrome(service=service, options=opts)


def _dump_artifacts(driver, slug: str, label: str):
    try:
        html_path = os.path.join(DEBUG_DIR, f"{slug}__{label}.html")
        with open(html_path, "w", encoding="utf-8") as f:
            f.write(driver.page_source or "")
        print(f"[debug] wrote {html_path}", flush=True)
    except Exception as e:
        print(f"[debug] failed to write html: {e}", flush=True)

    try:
        png_path = os.path.join(DEBUG_DIR, f"{slug}__{label}.png")
        driver.save_screenshot(png_path)
        print(f"[debug] wrote {png_path}", flush=True)
    except Exception as e:
        print(f"[debug] failed to write screenshot: {e}", flush=True)


def wait_for_highcharts(driver, timeout=60):
    WebDriverWait(driver, timeout).until(
        lambda d: d.execute_script(
            "return typeof Highcharts !== 'undefined' && Highcharts.charts && Highcharts.charts.length > 0;"
        )
    )


def click_te_10y_button(driver):
    sel = "a.hawk-chartOptions-datePicker-cnt-btn[data-span_str='10Y']"
    try:
        btn = WebDriverWait(driver, 10).until(EC.element_to_be_clickable((By.CSS_SELECTOR, sel)))
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
          if (txt === label) { rs.clickButton(i, true); return true; }
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
    slug = slugify_country(country)
    last_err = None

    for attempt in range(retry + 1):
        try:
            driver.get(url)
            WebDriverWait(driver, 60).until(EC.presence_of_element_located((By.TAG_NAME, "body")))
            time.sleep(2)

            try:
                wait_for_highcharts(driver, timeout=60)
            except TimeoutException as e:
                _dump_artifacts(driver, slug, f"no_highcharts_attempt{attempt}")
                raise e

            clicked = click_te_10y_button(driver)
            if clicked:
                time.sleep(2)
            else:
                chosen = set_range_to_max_or_10y(driver)
                if chosen:
                    time.sleep(2)

            df = extract_highcharts_series(driver)
            if df is None or df.empty:
                _dump_artifacts(driver, slug, f"no_series_attempt{attempt}")
                return None

            # Normalize to month start; store as naive timestamp
            df["date"] = df["date"].dt.to_period("M").dt.to_timestamp()

            df["country"] = country
            df = df.drop_duplicates(subset=["country", "date"])
            return df[["country", "date", "value"]]

        except (TimeoutException, WebDriverException, Exception) as e:
            last_err = e
            time.sleep(2 + attempt)

    print(f"  [fail] {country}: {last_err}", flush=True)
    return None


def merge_with_existing(master_csv_gz: str, new_panel: pd.DataFrame) -> pd.DataFrame:
    """
    Append + upsert into a master dataset.
    Key is (country, date). New values win.
    """
    new_panel = new_panel.copy()
    new_panel["date"] = pd.to_datetime(new_panel["date"])  # ensure datetime64[ns]

    if os.path.exists(master_csv_gz):
        old = pd.read_csv(master_csv_gz, compression="gzip", parse_dates=["date"])
        old = old[["country", "date", "value"]]
        combined = pd.concat([old, new_panel], ignore_index=True)
        # new wins -> keep last after sort+concat ordering (so we drop dupes keeping last)
        combined = combined.sort_values(["country", "date"]).drop_duplicates(
            subset=["country", "date"], keep="last"
        )
        return combined.sort_values(["country", "date"]).reset_index(drop=True)

    return new_panel.sort_values(["country", "date"]).reset_index(drop=True)


def write_manifest(master: pd.DataFrame, latest10y: pd.DataFrame):
    manifest = {
        "dataset": "Total Vehicle Sales (Monthly)",
        "source": f"{BASE_URL}/",
        "metric_path": METRIC_PATH,
        "generated_utc": datetime.now(timezone.utc).isoformat(),
        "row_count_master": int(master.shape[0]),
        "row_count_latest10y": int(latest10y.shape[0]),
        "country_count": int(master["country"].nunique()),
        "files": {
            "master_xlsx": "data/latest/master_total_vehicle_sales.xlsx",
            "master_csv_gz": "data/latest/master_total_vehicle_sales.csv.gz",
            "latest10y_xlsx": "data/latest/total_vehicle_sales_monthly_last_10y.xlsx",
            "latest10y_csv_gz": "data/latest/total_vehicle_sales_monthly_last_10y.csv.gz",
            "manifest": "data/latest/manifest.json",
        },
        "countries": TARGET_COUNTRIES,
        "latest10y_cutoff_utc": cutoff_10y.isoformat(),
    }
    with open(MASTER_MANIFEST, "w", encoding="utf-8") as f:
        json.dump(manifest, f, indent=2)


def write_outputs(new_panel: pd.DataFrame):
    # Merge into master (append + upsert)
    master = merge_with_existing(MASTER_CSV_GZ, new_panel)

    # Latest 10y view from master (convenience)
    latest10y = master[master["date"] >= cutoff_10y].copy()

    # Write master CSV.GZ (best canonical store)
    csv_bytes = master.to_csv(index=False).encode("utf-8")
    with gzip.open(MASTER_CSV_GZ, "wb") as f:
        f.write(csv_bytes)

    # Write master XLSX (optional; can get big over time)
    with pd.ExcelWriter(MASTER_XLSX, engine="openpyxl") as writer:
        master.to_excel(writer, sheet_name="panel", index=False)

    # Write latest10y outputs
    csv10_bytes = latest10y.to_csv(index=False).encode("utf-8")
    with gzip.open(LATEST10Y_CSV_GZ, "wb") as f:
        f.write(csv10_bytes)

    with pd.ExcelWriter(LATEST10Y_XLSX, engine="openpyxl") as writer:
        latest10y.to_excel(writer, sheet_name="panel", index=False)

    write_manifest(master, latest10y)

    print(f"[out] wrote:\n- {MASTER_CSV_GZ}\n- {MASTER_XLSX}\n- {LATEST10Y_CSV_GZ}\n- {LATEST10Y_XLSX}\n- {MASTER_MANIFEST}", flush=True)


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

            time.sleep(1.0)

        if not all_rows:
            raise RuntimeError("No data extracted for any target country.")

        new_panel = pd.concat(all_rows, ignore_index=True).sort_values(["country", "date"]).reset_index(drop=True)
        write_outputs(new_panel)

    finally:
        driver.quit()


if __name__ == "__main__":
    main()
