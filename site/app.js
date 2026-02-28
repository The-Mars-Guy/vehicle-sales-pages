async function loadManifest() {
  const res = await fetch("./data/manifest.json", { cache: "no-store" });
  if (!res.ok) throw new Error("Failed to load data/manifest.json");
  return await res.json();
}

function fmtInt(n) {
  try { return new Intl.NumberFormat().format(n); } catch { return String(n); }
}

function render(manifest) {
  const statusText = document.querySelector("#status .status-text");
  const dlXlsx = document.getElementById("dl-xlsx");
  const dlCsv = document.getElementById("dl-csv");
  const countriesEl = document.getElementById("countries");
  const filterEl = document.getElementById("filter");
  const clearBtn = document.getElementById("clear");
  const sourceEl = document.getElementById("source");

  const statCountries = document.getElementById("stat-countries");
  const statRows = document.getElementById("stat-rows");
  const statGenerated = document.getElementById("stat-generated");
  const cutoffEl = document.getElementById("cutoff");
  const countryCountEl = document.getElementById("country-count");
  const buildIdEl = document.getElementById("build-id");

  // Links
  dlXlsx.href = `./${manifest.files.xlsx}`;
  dlCsv.href = `./${manifest.files.csv_gz}`;
  sourceEl.href = manifest.source;

  // Stats
  statCountries.textContent = fmtInt(manifest.country_count ?? 0);
  statRows.textContent = fmtInt(manifest.row_count ?? 0);
  statGenerated.textContent = manifest.generated_utc ?? "—";
  cutoffEl.textContent = `Cutoff: ${manifest.cutoff_utc ?? "—"}`;

  statusText.textContent = `Updated ${manifest.generated_utc ?? "—"} (UTC)`;

  // Build ID (simple)
  buildIdEl.textContent = `manifest: ${manifest.generated_utc ?? "—"}`;

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

  filterEl.addEventListener("input", (e) => drawList(e.target.value));
  clearBtn.addEventListener("click", () => {
    filterEl.value = "";
    drawList("");
    filterEl.focus();
  });

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
