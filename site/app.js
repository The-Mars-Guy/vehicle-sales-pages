async function loadManifest() {
  const res = await fetch("./data/latest/manifest.json", { cache: "no-store" });
  if (!res.ok) throw new Error("Failed to load data/latest/manifest.json");
  return await res.json();
}

function fmtInt(n) {
  try {
    return new Intl.NumberFormat().format(n);
  } catch {
    return String(n);
  }
}

function safeText(v, fallback = "—") {
  return v == null || v === "" ? fallback : String(v);
}

function setLink(el, href, filename) {
  if (!el) return;
  el.href = href;
  if (filename) el.setAttribute("download", filename);
}

function render(manifest) {
  const statusText = document.querySelector("#status .status-text");

  // Existing buttons (repurpose as "latest 10y")
  const dlLatest10yXlsx = document.getElementById("dl-xlsx");
  const dlLatest10yCsv = document.getElementById("dl-csv");

  // New buttons (master)
  const dlMasterXlsx = document.getElementById("dl-master-xlsx");
  const dlMasterCsv = document.getElementById("dl-master-csv");

  const countriesEl = document.getElementById("countries");
  const filterEl = document.getElementById("filter");
  const clearBtn = document.getElementById("clear");
  const sourceEl = document.getElementById("source");

  // Existing stat slots (we’ll map them to MASTER by default)
  const statCountries = document.getElementById("stat-countries");
  const statRows = document.getElementById("stat-rows");
  const statGenerated = document.getElementById("stat-generated");
  const cutoffEl = document.getElementById("cutoff");
  const countryCountEl = document.getElementById("country-count");
  const buildIdEl = document.getElementById("build-id");

  // If you add an optional extra element, we’ll fill it:
  // <span id="stat-rows-10y"></span>
  const statRows10y = document.getElementById("stat-rows-10y");

  // Source
  if (sourceEl && manifest.source) sourceEl.href = manifest.source;

  // Download links (from manifest.files)
  const f = manifest.files || {};

  // Master
  setLink(dlMasterXlsx, f.master_xlsx ? `./${f.master_xlsx}` : "#", "master_total_vehicle_sales.xlsx");
  setLink(dlMasterCsv, f.master_csv_gz ? `./${f.master_csv_gz}` : "#", "master_total_vehicle_sales.csv.gz");

  // Latest 10y (existing IDs)
  setLink(
    dlLatest10yXlsx,
    f.latest10y_xlsx ? `./${f.latest10y_xlsx}` : "#",
    "total_vehicle_sales_monthly_last_10y.xlsx"
  );
  setLink(
    dlLatest10yCsv,
    f.latest10y_csv_gz ? `./${f.latest10y_csv_gz}` : "#",
    "total_vehicle_sales_monthly_last_10y.csv.gz"
  );

  // Stats
  statCountries.textContent = fmtInt(manifest.country_count ?? 0);
  statRows.textContent = fmtInt(manifest.row_count_master ?? manifest.row_count ?? 0);
  if (statRows10y) statRows10y.textContent = fmtInt(manifest.row_count_latest10y ?? 0);

  statGenerated.textContent = safeText(manifest.generated_utc);

  // Cutoff is now specifically for the latest-10y view
  const cutoff = manifest.latest10y_cutoff_utc || manifest.cutoff_utc;
  cutoffEl.textContent = `10y cutoff: ${safeText(cutoff)}`;

  statusText.textContent = `Updated ${safeText(manifest.generated_utc)} (UTC)`;

  // Build ID
  buildIdEl.textContent = `manifest: ${safeText(manifest.generated_utc)}`;

  // Countries list (same behavior)
  const countries = manifest.countries || [];

  function drawList(q) {
    countriesEl.innerHTML = "";
    const query = (q || "").trim().toLowerCase();

    const filtered = query
      ? countries.filter((c) => c.toLowerCase().includes(query))
      : countries;

    countryCountEl.textContent = `${filtered.length} shown / ${countries.length} total`;

    for (const c of filtered) {
      const li = document.createElement("li");
      li.textContent = c;
      countriesEl.appendChild(li);
    }
  }

  // Avoid stacking duplicate listeners if render() is ever called twice
  filterEl.oninput = (e) => drawList(e.target.value);
  clearBtn.onclick = () => {
    filterEl.value = "";
    drawList("");
    filterEl.focus();
  };

  drawList("");
}

(async () => {
  try {
    const manifest = await loadManifest();
    render(manifest);
  } catch (e) {
    const statusText = document.querySelector("#status .status-text");
    statusText.textContent = `Error: ${e.message}`;
    statusText.style.color = "rgba(255,255,255,0.85)";
  }
})();
