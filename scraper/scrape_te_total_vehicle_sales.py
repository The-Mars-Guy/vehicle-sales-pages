import os
import json
import time
import gzip
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


LIST_URL = "https://tradingeconomics.com/country-list/total-vehicle-sales"

# Output locations (repo-root relative by default)
REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
DATA_DIR = os.environ.get("DATA_DIR", os.path.join(REPO_ROOT, "data"))
os.makedirs(DATA_DIR, exist_ok=True)

OUTPUT_XLSX = os.path.join(DATA_DIR, "total_vehicle_sales_monthly_last_10y.xlsx")
OUTPUT_CSV_GZ = os.path.join(DATA_DIR, "total_vehicle_sales_monthly_last_10y.csv.gz")
MANIFEST_JSON = os.path.join(DATA_DIR, "manifest.json")

# Past 10 years from the first day of the current month (UTC)
now_utc = datetime.now(timezone.utc)
cutoff = (now_utc.replace(day=1, hour=0, minute=0, second=0, microsecond=0) - relativedelta(years=10))


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

    # Use system chromedriver/chromium on ubuntu-latest
    service = Service()
    return webdriver.Chrome(service=service, options=opts)


def wait_for_highcharts(driver, timeout=25):
    WebDriverWait(driver, timeout).until(
        lambda d: d.execute_script(
            "return typeof Highcharts !== 'undefined' && Highcharts.charts && Highcharts.charts.length > 0;"
        )
    )


def get_country_links(driver):
    driver.get(LIST_URL)
    WebDriverWait(driver, 25).until(EC.presence_of_element_located((By.TAG_NAME, "table")))
    time.sleep(1)

    anchors = driver.find_elements(By.CSS_SELECTOR, "table a[href*='/total-vehicle-sales']")
    out = {}
    for a in anchors:
        name = (a.text or "").strip()
        href = a.get_attribute("href")
        if name and href:
            out[name] = href
    return out


def click_te_10y_button(driver):
    sel = "a.hawk-chartOptions-datePicker-cnt-btn[data-span_str='10Y']"
    try:
        btn = WebDriverWait(driver, 6).until(EC.element_to_be_clickable((By.CSS_SELECTOR, sel)))
        driver.execute_script("arguments[0].click();", btn)
        return True
    except TimeoutException:
        return False


def set_range_to_max_or_10y(driver):
    js = r'''
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
    '''
    return driver.execute_script(js)


def extract_highcharts_series(driver):
    js = r'''
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
    '''
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
            WebDriverWait(driver, 25).until(EC.presence_of_element_located((By.TAG_NAME, "body")))
            time.sleep(2)

            wait_for_highcharts(driver, timeout=25)

            clicked = click_te_10y_button(driver)
            if clicked:
                time.sleep(2)

            if not clicked:
                chosen = set_range_to_max_or_10y(driver)
                if chosen:
                    time.sleep(2)

            df = extract_highcharts_series(driver)
            if df is None or df.empty:
                return None

            df = df[df["date"] >= cutoff]
            df["country"] = country
            # normalize date to month start (many series are monthly)
            df["date"] = df["date"].dt.to_period("M").dt.to_timestamp().dt.tz_localize("UTC")
            df = df.drop_duplicates(subset=["country", "date"])
            return df[["country", "date", "value"]]

        except (TimeoutException, WebDriverException, Exception) as e:
            last_err = e
            time.sleep(2 + attempt)

    print(f"  !! Failed after retries for {country}: {last_err}")
    return None


def write_outputs(panel: pd.DataFrame):
    panel = panel.sort_values(["country", "date"]).reset_index(drop=True)

    with pd.ExcelWriter(OUTPUT_XLSX, engine="openpyxl") as writer:
        panel.to_excel(writer, sheet_name="panel", index=False)

    # compact gzip csv for web download
    csv_bytes = panel.to_csv(index=False).encode("utf-8")
    with gzip.open(OUTPUT_CSV_GZ, "wb") as f:
        f.write(csv_bytes)

    manifest = {
        "dataset": "Total Vehicle Sales (Monthly, last 10y)",
        "source": LIST_URL,
        "generated_utc": datetime.now(timezone.utc).isoformat(),
        "cutoff_utc": cutoff.isoformat(),
        "row_count": int(panel.shape[0]),
        "country_count": int(panel["country"].nunique()),
        "files": {
            "xlsx": "data/total_vehicle_sales_monthly_last_10y.xlsx",
            "csv_gz": "data/total_vehicle_sales_monthly_last_10y.csv.gz",
        },
        "countries": sorted(panel["country"].unique().tolist()),
    }
    with open(MANIFEST_JSON, "w", encoding="utf-8") as f:
        json.dump(manifest, f, indent=2)


def main():
    driver = build_driver()
    try:
        countries = get_country_links(driver)
        if not countries:
            raise RuntimeError("Could not find country links on the list page (site may have changed).")

        all_rows = []
        items = list(countries.items())
        for i, (country, url) in enumerate(items, 1):
            print(f"[{i}/{len(items)}] {country} -> {url}")
            df = scrape_country(driver, country, url, retry=2)
            if df is not None and not df.empty:
                all_rows.append(df)
            else:
                print(f"  !! No chart data extracted for {country}")

            time.sleep(1.0)  # be polite

        if not all_rows:
            raise RuntimeError("No data extracted. TE may be blocking or charts not accessible in your session.")

        panel = pd.concat(all_rows, ignore_index=True)
        write_outputs(panel)
        print(f"\nSaved:\n- {OUTPUT_XLSX}\n- {OUTPUT_CSV_GZ}\n- {MANIFEST_JSON}")

    finally:
        driver.quit()


if __name__ == "__main__":
    main()
