async function loadManifest() {
  // Served from repo root via Pages (we copy data/ into published site)
  const res = await fetch("./data/manifest.json", { cache: "no-store" });
  if (!res.ok) throw new Error("Failed to load manifest.json");
  return await res.json();
}

function render(manifest) {
  const meta = document.getElementById("meta");
  const dlXlsx = document.getElementById("dl-xlsx");
  const dlCsv = document.getElementById("dl-csv");
  const countriesEl = document.getElementById("countries");
  const filterEl = document.getElementById("filter");
  const sourceEl = document.getElementById("source");

  meta.textContent =
    `Last generated (UTC): ${manifest.generated_utc} · Countries: ${manifest.country_count} · Rows: ${manifest.row_count}`;

  dlXlsx.href = `./${manifest.files.xlsx}`;
  dlCsv.href = `./${manifest.files.csv_gz}`;
  sourceEl.href = manifest.source;

  const countries = manifest.countries || [];
  function drawList(q) {
    countriesEl.innerHTML = "";
    const query = (q || "").toLowerCase();
    const filtered = countries.filter((c) => c.toLowerCase().includes(query));
    for (const c of filtered) {
      const li = document.createElement("li");
      li.textContent = c;
      countriesEl.appendChild(li);
    }
  }

  filterEl.addEventListener("input", (e) => drawList(e.target.value));
  drawList("");
}

(async () => {
  try {
    const manifest = await loadManifest();
    render(manifest);
  } catch (e) {
    document.getElementById("meta").textContent = `Error: ${e.message}`;
  }
})();
