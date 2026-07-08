const DAYS = ["2026-07-01", "2026-07-02", "2026-07-03", "2026-07-04", "2026-07-05"];
const START = DAYS[0], END = DAYS[DAYS.length - 1];

async function j(url) { const r = await fetch(url); return r.json(); }

async function initDims() {
  const dims = await j("/api/dimensions");
  const cSel = document.getElementById("campaign");
  const sSel = document.getElementById("segment");
  (dims.campaigns || ["camp_finals"]).forEach(c => cSel.add(new Option(c, c)));
  sSel.add(new Option("(all segments)", ""));
  (dims.segments || ["sports"]).forEach(s => sSel.add(new Option(s, s)));
  sSel.value = (dims.segments || []).includes("sports") ? "sports" : "";
}

function renderBars(labels, values, cumIndex) {
  const max = Math.max(1, ...values);
  const bars = document.getElementById("bars");
  bars.innerHTML = "";
  values.forEach((v, i) => {
    const wrap = document.createElement("div"); wrap.className = "bar-wrap";
    const val = document.createElement("div"); val.className = "bar-val";
    val.textContent = v.toLocaleString();
    const bar = document.createElement("div");
    bar.className = "bar" + (i === cumIndex ? " cum" : "");
    bar.style.height = (100 * v / max) + "%";
    const lbl = document.createElement("div"); lbl.className = "bar-lbl";
    lbl.textContent = labels[i];
    wrap.append(val, bar, lbl); bars.append(wrap);
  });
}

async function load() {
  const campaign = document.getElementById("campaign").value || "camp_finals";
  const segment = document.getElementById("segment").value;
  const segQ = segment ? `&segment=${segment}` : "";
  const daily = [];
  for (const d of DAYS) {
    const r = await j(`/api/reach/daily?campaign=${campaign}${segQ}&day=${d}`);
    daily.push(Number(r.reach) || 0);
  }
  const cum = await j(`/api/reach/cumulative?campaign=${campaign}${segQ}&start=${START}&end=${END}`);
  const cumVal = Number(cum.reach) || 0;
  const sumDaily = daily.reduce((a, b) => a + b, 0);
  renderBars([...DAYS.map(d => d.slice(5)), "CUMULATIVE"], [...daily, cumVal], DAYS.length);
  document.getElementById("stats").innerHTML =
    `<span class="stat">Cumulative reach<b>${cumVal.toLocaleString()}</b></span>` +
    `<span class="stat">Sum of daily<b>${sumDaily.toLocaleString()}</b></span>` +
    `<span class="stat">Overlap (dedup)<b>${(sumDaily - cumVal).toLocaleString()}</b></span>`;
  document.getElementById("note").textContent =
    "Cumulative < sum-of-daily because the same individuals are reached on multiple days — HLL sketches dedup them without rescanning raw impressions.";
  await loadConvergence(campaign, segment, segQ);
}

async function loadConvergence(campaign, segment, segQ) {
  const c = await j(`/api/reach/convergence?campaign=${campaign}${segQ}&start=${START}&end=${END}`);
  const cells = [
    ["digital", "Digital (ad server)", c.digital],
    ["linear", "Linear (national TV)", c.linear],
    ["combined", "Combined — deduped", c.combined],
  ];
  document.getElementById("conv").innerHTML = cells.map(([cls, k, v]) =>
    `<div class="cell ${cls}"><div class="k">${k}</div><div class="v">${(Number(v) || 0).toLocaleString()}</div></div>`
  ).join("");
  const seg = segment || "all segments";
  document.getElementById("conv-note").textContent =
    `Digital + linear reach ${(c.digital + c.linear).toLocaleString()} people, but ${(c.overlap || 0).toLocaleString()} were reached on both — so the true unified reach across delivery methods for ${campaign} / ${seg} is ${(Number(c.combined) || 0).toLocaleString()}.`;
}

async function ask() {
  const ans = document.getElementById("ans");
  ans.textContent = "…";
  const r = await fetch("/api/chat", {
    method: "POST", headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ prompt: document.getElementById("q").value }),
  }).then(x => x.json());
  ans.textContent = r.reply;
}

(async () => { await initDims(); await load(); })();
