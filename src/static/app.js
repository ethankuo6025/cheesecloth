// static/app.js

const API = "/api";

// ── State ──

const state = {
    tickers:       [],
    metrics:       {},
    primaryTicker: null,
    compareTicker: null,
    activeMetric:  null,
    mode:          "annual",
    normalize:     false,
    primaryFacts:  [],
    compareFacts:  [],
};

let chart = null;

// Active background scrape polls: jobId → { ticker, intervalId }
const activeJobs = new Map();

// ── Formatting ──

function fmtValue(num, { isPerShare = false, isCount = false } = {}) {
    if (num == null || isNaN(num)) return "—";
    const a = Math.abs(num);
    if (isPerShare) return `$${num.toFixed(2)}`;
    if (isCount) {
        if (a >= 1e9) return `${(num / 1e9).toFixed(2)}B`;
        if (a >= 1e6) return `${(num / 1e6).toFixed(2)}M`;
        if (a >= 1e3) return `${(num / 1e3).toFixed(2)}K`;
        return num.toLocaleString("en-US", { maximumFractionDigits: 0 });
    }
    if (a >= 1e9) return `$${(num / 1e9).toFixed(2)}B`;
    if (a >= 1e6) return `$${(num / 1e6).toFixed(2)}M`;
    if (a >= 1e3) return `$${(num / 1e3).toFixed(2)}K`;
    return `$${num.toLocaleString("en-US", { minimumFractionDigits: 2, maximumFractionDigits: 2 })}`;
}

function fmtPct(num) {
    if (num == null || isNaN(num)) return "—";
    return `${num >= 0 ? "+" : ""}${(num * 100).toFixed(2)}%`;
}

function factDate(f)  { return f.instant_date || f.end_date || f.start_date || null; }

function factLabel(f) {
    if (f.period_type === "instant" && f.instant_date) return f.instant_date;
    if (f.start_date && f.end_date) return `${f.start_date} → ${f.end_date}`;
    return f.instant_date || f.end_date || "—";
}

function meta()    { return (state.activeMetric && state.metrics[state.activeMetric]) || {}; }
function fmtOpts() { const m = meta(); return { isPerShare: m.is_per_share, isCount: m.is_count }; }

function periodBucket(isoDate) {
    const d = new Date(isoDate + "T00:00:00Z");
    const y = d.getUTCFullYear();
    const m = d.getUTCMonth() + 1;
    if (state.mode === "annual") return String(y);
    return `${y}-Q${Math.ceil(m / 3)}`;
}

function emptyMsg(ticker, label, mode) {
    return {
        title: `No ${label} data for ${ticker}`,
        sub:   `There are no ${label.toLowerCase()} records in ${mode} mode. ` +
               `Try a different mode, or the company may not report this line item.`,
    };
}

// ── API ──

async function api(path, opts = {}) {
    const res  = await fetch(`${API}${path}`, opts);
    const data = await res.json();
    if (!res.ok) throw new Error(data.error || `HTTP ${res.status}`);
    return data;
}

async function fetchTickers()             { return api("/tickers"); }
async function fetchMetrics()             { return api("/metrics"); }
async function fetchFacts(ticker, metric) {
    return api(`/facts/${encodeURIComponent(ticker)}/${encodeURIComponent(metric)}?mode=${state.mode}`);
}

// ── Selects ──

function renderSelects() {
    for (const id of ["ticker-primary", "ticker-compare"]) {
        const sel  = document.getElementById(id);
        const prev = sel.value;
        const lbl  = id === "ticker-primary" ? "— select —" : "— none —";
        sel.innerHTML = `<option value="">${lbl}</option>` +
            state.tickers.map(t => `<option value="${t.ticker}">${t.ticker}</option>`).join("");
        if (prev && state.tickers.some(t => t.ticker === prev)) sel.value = prev;
    }
}

// ── Metric tabs ──

function renderMetricTabs() {
    const wrap = document.getElementById("metric-tabs");
    wrap.innerHTML = "";
    for (const [key, m] of Object.entries(state.metrics)) {
        const btn = document.createElement("button");
        btn.className = "metric-tab" + (key === state.activeMetric ? " active" : "");
        btn.textContent = m.label;
        btn.dataset.metric = key;
        btn.addEventListener("click", () => selectMetric(key));
        wrap.appendChild(btn);
    }
}

// ── Chart ──

function toPoints(facts) {
    return facts
        .map(f => ({ x: factDate(f), y: parseFloat(f.value) }))
        .filter(p => p.x && !isNaN(p.y))
        .sort((a, b) => a.x.localeCompare(b.x));
}

function normalizePoints(pPts, cPts) {
    const bucketMap = pts => {
        const m = new Map();
        for (const p of pts) { const k = periodBucket(p.x); if (!m.has(k)) m.set(k, p.y); }
        return m;
    };
    const pMap = bucketMap(pPts), cMap = bucketMap(cPts);
    const common = [...pMap.keys()].filter(k => cMap.has(k)).sort();
    if (!common.length) return { primary: pPts, compare: cPts };
    const bk = common[0], pb = pMap.get(bk), cb = cMap.get(bk);
    if (!pb || !cb) return { primary: pPts, compare: cPts };
    const norm = (pts, base) =>
        pts.filter(p => periodBucket(p.x) >= bk).map(p => ({ x: p.x, y: (p.y / base) * 100 }));
    return { primary: norm(pPts, pb), compare: norm(cPts, cb) };
}

function buildDatasets() {
    let pPts = toPoints(state.primaryFacts);
    let cPts = toPoints(state.compareFacts);
    const isNorm = state.normalize && pPts.length && cPts.length;
    if (isNorm) { const n = normalizePoints(pPts, cPts); pPts = n.primary; cPts = n.compare; }
    const ds = [];
    if (pPts.length) ds.push({
        label: state.primaryTicker, data: pPts,
        borderColor: "#6c8cff", backgroundColor: "rgba(108,140,255,.12)",
        fill: true, tension: .25, pointRadius: 3, pointHoverRadius: 5,
    });
    if (cPts.length) ds.push({
        label: state.compareTicker, data: cPts,
        borderColor: "#fb923c", backgroundColor: "rgba(251,146,60,.10)",
        fill: true, tension: .25, pointRadius: 3, pointHoverRadius: 5,
    });
    return { datasets: ds, normalized: isNorm };
}

function showChartEmpty(title, sub) {
    document.getElementById("chart-empty-title").textContent = title;
    document.getElementById("chart-empty-sub").textContent   = sub || "";
    document.getElementById("chart-empty").style.display = "";
    document.getElementById("main-chart").style.display  = "none";
}
function hideChartEmpty() {
    document.getElementById("chart-empty").style.display = "none";
    document.getElementById("main-chart").style.display  = "";
}

function renderChart() {
    const ctx = document.getElementById("main-chart").getContext("2d");
    if (chart) chart.destroy();
    const { datasets, normalized } = buildDatasets();
    const m = meta();

    if (!state.primaryTicker) { showChartEmpty("No ticker selected", "Choose a ticker above to get started."); return; }
    if (!state.activeMetric)  { showChartEmpty("No metric selected", "Pick a metric from the tabs above."); return; }

    const pEmpty = !state.primaryFacts.length, cEmpty = !state.compareFacts.length;
    if (pEmpty && (!state.compareTicker || cEmpty)) {
        const msg = emptyMsg(state.primaryTicker, m.label, state.mode);
        showChartEmpty(msg.title, msg.sub); return;
    }
    if (pEmpty && state.compareTicker && !cEmpty) {
        showChartEmpty(`No ${m.label} data for ${state.primaryTicker}`,
            `Only ${state.compareTicker} has data. Try a different metric or mode.`); return;
    }
    hideChartEmpty();

    const yLabel = normalized ? "Indexed (base = 100)" : (m.label || "Value");
    chart = new Chart(ctx, {
        type: "line", data: { datasets },
        options: {
            responsive: true, maintainAspectRatio: false,
            interaction: { mode: "index", intersect: false },
            scales: {
                x: {
                    type: "time",
                    time: { unit: state.mode === "quarterly" ? "quarter" : "year", tooltipFormat: "MMM yyyy" },
                    grid: { color: "rgba(255,255,255,.04)" },
                    ticks: { color: "#8b8d98", font: { size: 11 } },
                },
                y: {
                    title: { display: true, text: yLabel, color: "#8b8d98", font: { size: 11 } },
                    grid: { color: "rgba(255,255,255,.04)" },
                    ticks: {
                        color: "#8b8d98", font: { size: 11 },
                        callback: v => normalized ? v.toFixed(0) : fmtValue(v, fmtOpts()),
                    },
                },
            },
            plugins: {
                legend: { display: datasets.length > 1, labels: { color: "#e1e2e8", font: { size: 12 } } },
                tooltip: { callbacks: {
                    label: tip => {
                        const v = tip.parsed.y;
                        return `${tip.dataset.label}: ${normalized ? v.toFixed(1) : fmtValue(v, fmtOpts())}`;
                    },
                } },
            },
        },
    });
}

// ── Table ──

function renderTable(tableId, facts, ticker) {
    const table = document.getElementById(tableId);
    const thead = table.querySelector("thead tr");
    const tbody = table.querySelector("tbody");
    thead.innerHTML = ""; tbody.innerHTML = "";
    const m = meta();

    if (!facts.length) {
        const tr = document.createElement("tr"), td = document.createElement("td");
        td.setAttribute("colspan", "5"); td.className = "table-empty";
        if (!ticker) {
            td.innerHTML = '<div class="table-empty-title">No ticker selected</div>';
        } else {
            const msg = emptyMsg(ticker, m.label || "metric", state.mode);
            td.innerHTML = `<div class="table-empty-title">${msg.title}</div><div class="table-empty-sub">${msg.sub}</div>`;
        }
        tr.appendChild(td); tbody.appendChild(tr); return;
    }

    ["Period", m.label || "Value", "Change", "Unit", "Accession"].forEach(h => {
        const th = document.createElement("th"); th.textContent = h; thead.appendChild(th);
    });
    const sorted = [...facts].sort((a, b) => (factDate(b) || "").localeCompare(factDate(a) || ""));
    for (let i = 0; i < sorted.length; i++) {
        const f = sorted[i], val = parseFloat(f.value), tr = document.createElement("tr");
        let chgStr = "—", chgCls = "";
        if (i < sorted.length - 1) {
            const prev = parseFloat(sorted[i + 1].value);
            if (prev && prev !== 0) {
                const chg = (val - prev) / Math.abs(prev);
                chgStr = fmtPct(chg); chgCls = chg >= 0 ? "val-positive" : "val-negative";
            }
        }
        [
            { t: factLabel(f) },
            { t: fmtValue(val, fmtOpts()), c: val < 0 ? "val-negative" : "" },
            { t: chgStr, c: chgCls },
            { t: f.unit || "—" },
            { t: f.accession ? f.accession.slice(-10) : "—" },
        ].forEach(({ t, c }) => {
            const td = document.createElement("td"); td.textContent = t;
            if (c) td.className = c; tr.appendChild(td);
        });
        tbody.appendChild(tr);
    }
}

// ── Stats ──

function computeStats(facts) {
    const pts = facts
        .filter(f => factDate(f) && f.value != null && !isNaN(parseFloat(f.value)))
        .sort((a, b) => (factDate(a) || "").localeCompare(factDate(b) || ""));
    if (!pts.length) return null;
    const vals = pts.map(f => parseFloat(f.value)), n = vals.length;
    const latest = vals[n - 1], oldest = vals[0];
    const min = Math.min(...vals), max = Math.max(...vals);
    const mean = vals.reduce((s, v) => s + v, 0) / n;
    const growths = [];
    for (let i = 1; i < n; i++) if (vals[i - 1] !== 0) growths.push((vals[i] - vals[i - 1]) / Math.abs(vals[i - 1]));
    const avgGrowth = growths.length ? growths.reduce((s, g) => s + g, 0) / growths.length : null;
    const medianGrowth = growths.length ? (() => {
        const s = [...growths].sort((a, b) => a - b), m = Math.floor(s.length / 2);
        return s.length % 2 ? s[m] : (s[m - 1] + s[m]) / 2;
    })() : null;
    let cagr = null;
    if (n >= 2 && oldest !== 0 && (latest / oldest) > 0) cagr = Math.pow(latest / oldest, 1 / (n - 1)) - 1;
    let stddev = null;
    if (growths.length >= 2 && avgGrowth !== null) {
        stddev = Math.sqrt(growths.reduce((s, g) => s + (g - avgGrowth) ** 2, 0) / (growths.length - 1));
    }
    return { latest, oldest, min, max, mean, periods: n, avgGrowth, medianGrowth, cagr, stddev };
}

function renderStatsCards(grid, pStats, cStats, pTicker, cTicker) {
    const fo = fmtOpts(), compare = !!cStats;
    const signCls = v => v == null ? "stat-neutral" : v >= 0 ? "stat-positive" : "stat-negative";
    const cards = [
        { label: "Latest",         p: fmtValue(pStats.latest, fo),  c: cStats ? fmtValue(cStats.latest, fo) : null },
        { label: "Mean",           p: fmtValue(pStats.mean, fo),    c: cStats ? fmtValue(cStats.mean, fo) : null },
        { label: "Min / Max",
          p: `${fmtValue(pStats.min, fo)} / ${fmtValue(pStats.max, fo)}`,
          c: cStats ? `${fmtValue(cStats.min, fo)} / ${fmtValue(cStats.max, fo)}` : null },
        { label: "Avg Growth",     p: fmtPct(pStats.avgGrowth),    pc: signCls(pStats.avgGrowth),
          c: cStats ? fmtPct(cStats.avgGrowth) : null,    cc: cStats ? signCls(cStats.avgGrowth) : "" },
        { label: "Median Growth",  p: fmtPct(pStats.medianGrowth), pc: signCls(pStats.medianGrowth),
          c: cStats ? fmtPct(cStats.medianGrowth) : null, cc: cStats ? signCls(cStats.medianGrowth) : "" },
        { label: "CAGR (approx)",  p: fmtPct(pStats.cagr),        pc: signCls(pStats.cagr),
          c: cStats ? fmtPct(cStats.cagr) : null,        cc: cStats ? signCls(cStats.cagr) : "" },
        { label: "Growth Std Dev", p: pStats.stddev != null ? fmtPct(pStats.stddev) : "—", pc: "stat-neutral",
          c: cStats ? (cStats.stddev != null ? fmtPct(cStats.stddev) : "—") : null, cc: "stat-neutral" },
        { label: "Periods",        p: String(pStats.periods), c: cStats ? String(cStats.periods) : null },
    ];
    for (const card of cards) {
        const div = document.createElement("div");
        div.className = "stat-card";
        if (compare && card.c != null) {
            div.classList.add("stat-card-compare");
            div.innerHTML = `
                <div class="stat-col">
                    <div class="stat-ticker-label stat-ticker-primary">${pTicker}</div>
                    <div class="stat-label">${card.label}</div>
                    <div class="stat-value ${card.pc || "stat-neutral"}">${card.p}</div>
                </div>
                <div class="stat-col-divider"></div>
                <div class="stat-col">
                    <div class="stat-ticker-label stat-ticker-compare">${cTicker}</div>
                    <div class="stat-label">${card.label}</div>
                    <div class="stat-value ${card.cc || "stat-neutral"}">${card.c}</div>
                </div>`;
        } else {
            div.innerHTML = `
                <div class="stat-label">${card.label}</div>
                <div class="stat-value ${card.pc || "stat-neutral"}">${card.p}</div>`;
        }
        grid.appendChild(div);
    }
}

function renderStats() {
    const grid = document.getElementById("stats-grid");
    grid.innerHTML = "";
    const m = meta();
    if (!state.primaryTicker || !state.activeMetric) {
        grid.innerHTML = '<p class="stats-placeholder">Select a ticker and metric to view statistics.</p>'; return;
    }
    const pStats = state.primaryFacts.length ? computeStats(state.primaryFacts) : null;
    const cStats = state.compareFacts.length ? computeStats(state.compareFacts) : null;
    if (!pStats && !cStats) {
        const msg = emptyMsg(state.primaryTicker, m.label, state.mode);
        grid.innerHTML = `<p class="stats-placeholder">${msg.title}. ${msg.sub}</p>`; return;
    }
    if (!pStats && cStats) {
        grid.innerHTML = `<p class="stats-placeholder">No ${m.label} data for ${state.primaryTicker} — showing ${state.compareTicker} only.</p>`;
        renderStatsCards(grid, cStats, null, state.compareTicker, null); return;
    }
    if (state.compareTicker && !cStats) {
        const note = document.createElement("p"); note.className = "stats-placeholder";
        note.textContent = `No ${m.label} data for ${state.compareTicker} in ${state.mode} mode.`;
        grid.appendChild(note);
    }
    renderStatsCards(grid, pStats, (state.compareTicker && cStats) ? cStats : null, state.primaryTicker, state.compareTicker);
}

// ── Refresh ──

async function refresh() {
    const m = meta();
    if (!state.primaryTicker || !state.activeMetric) {
        state.primaryFacts = []; state.compareFacts = [];
        renderChart(); renderTable("table-primary", [], null);
        renderTable("table-compare", [], null); renderStats(); return;
    }
    document.getElementById("chart-title").textContent =
        `${m.label} — ${state.primaryTicker}` +
        (state.compareTicker ? ` vs ${state.compareTicker}` : "") +
        ` (${state.mode.toUpperCase()})`;
    try { state.primaryFacts = (await fetchFacts(state.primaryTicker, state.activeMetric)).facts || []; }
    catch (e) { console.error(e); state.primaryFacts = []; }
    if (state.compareTicker) {
        try { state.compareFacts = (await fetchFacts(state.compareTicker, state.activeMetric)).facts || []; }
        catch (e) { console.error(e); state.compareFacts = []; }
    } else { state.compareFacts = []; }

    const cmp = !!state.compareTicker;
    document.getElementById("normalize-wrap").style.display = cmp ? "" : "none";
    document.getElementById("tab-compare").style.display    = cmp ? "" : "none";
    document.getElementById("tab-primary").textContent       = state.primaryTicker || "Data";
    document.getElementById("tab-compare").textContent       = state.compareTicker || "Compare";
    renderChart();
    renderTable("table-primary", state.primaryFacts, state.primaryTicker);
    renderTable("table-compare", state.compareFacts, state.compareTicker);
    renderStats();
}

function selectMetric(key) {
    state.activeMetric = key;
    document.querySelectorAll(".metric-tab").forEach(b => b.classList.toggle("active", b.dataset.metric === key));
    refresh();
}

// ────────────────────────────────────────────────────
//  Background scrape: toast + polling
// ────────────────────────────────────────────────────

function createToast(jobId, ticker) {
    const container = document.getElementById("toast-container");
    const el = document.createElement("div");
    el.className = "scrape-toast";
    el.dataset.jobId = jobId;
    el.innerHTML = `
        <div class="toast-icon" data-role="icon">⏳</div>
        <div class="toast-body">
            <div class="toast-ticker">${ticker}</div>
            <div class="toast-msg" data-role="msg">Starting…</div>
            <div class="toast-bar-track">
                <div class="toast-bar-fill active" data-role="bar" style="width:0%"></div>
            </div>
            <div class="toast-pct">
                <span data-role="step">Queued</span>
                <span data-role="pct">0%</span>
            </div>
        </div>
        <button class="toast-close" data-role="close" style="display:none">✕</button>`;
    el.querySelector('[data-role="close"]').addEventListener("click", () => removeToast(el));
    container.appendChild(el);
    return el;
}

function updateToast(jobId, info) {
    const el = document.querySelector(`.scrape-toast[data-job-id="${jobId}"]`);
    if (!el) return;

    const bar   = el.querySelector('[data-role="bar"]');
    const msg   = el.querySelector('[data-role="msg"]');
    const icon  = el.querySelector('[data-role="icon"]');
    const step  = el.querySelector('[data-role="step"]');
    const pct   = el.querySelector('[data-role="pct"]');
    const close = el.querySelector('[data-role="close"]');

    const pctVal = Math.round(info.progress * 100);
    bar.style.width = `${pctVal}%`;
    pct.textContent  = `${pctVal}%`;
    msg.textContent  = info.message;

    const stepLabels = {
        queued: "Queued", init: "Initializing",
        "10-K": "10-K filings", "10-Q": "10-Q filings",
        finalize: "Finalizing", done: "Done",
    };
    step.textContent = stepLabels[info.step] || info.step;

    if (info.status === "complete") {
        el.classList.add("is-complete");
        bar.classList.remove("active");
        bar.classList.add("success");
        icon.textContent = "✅";
        close.style.display = "";
    } else if (info.status === "error") {
        el.classList.add("is-error");
        bar.classList.remove("active");
        bar.classList.add("error");
        icon.textContent = "❌";
        close.style.display = "";
    }
}

function removeToast(el) {
    el.classList.add("removing");
    el.addEventListener("animationend", () => el.remove());
}

async function pollJob(jobId) {
    try {
        const info = await api(`/scrape/${jobId}`);
        updateToast(jobId, info);

        if (info.status === "complete" || info.status === "error") {
            const entry = activeJobs.get(jobId);
            if (entry) { clearInterval(entry.intervalId); activeJobs.delete(jobId); }

            if (info.status === "complete") {
                // Refresh ticker list
                state.tickers = await fetchTickers();
                renderSelects();

                // Auto-select if nothing selected or if this is the selected ticker
                const ticker = info.ticker;
                if (!state.primaryTicker || state.primaryTicker === ticker) {
                    document.getElementById("ticker-primary").value = ticker;
                    state.primaryTicker = ticker;
                    refresh();
                }

                // Auto-dismiss toast after 6 seconds
                const el = document.querySelector(`.scrape-toast[data-job-id="${jobId}"]`);
                if (el) setTimeout(() => { if (el.parentNode) removeToast(el); }, 6000);
            }
        }
    } catch (e) {
        console.error("Poll error:", e);
    }
}

function startPolling(jobId, ticker) {
    const toast = createToast(jobId, ticker);
    const intervalId = setInterval(() => pollJob(jobId), 1500);
    activeJobs.set(jobId, { ticker, intervalId });
}

async function resumeActiveJobs() {
    try {
        const running = await api("/scrape");
        for (const job of running) {
            if (!activeJobs.has(job.job_id)) {
                startPolling(job.job_id, job.ticker);
                updateToast(job.job_id, job);
            }
        }
    } catch (e) {
        console.error("Resume poll error:", e);
    }
}

// ── Events ──

document.addEventListener("DOMContentLoaded", () => {

    document.getElementById("ticker-primary").addEventListener("change", e => {
        state.primaryTicker = e.target.value || null; refresh();
    });
    document.getElementById("ticker-compare").addEventListener("change", e => {
        state.compareTicker = e.target.value || null; refresh();
    });

    document.querySelectorAll(".mode-btn").forEach(btn =>
        btn.addEventListener("click", () => {
            document.querySelectorAll(".mode-btn").forEach(b => b.classList.remove("active"));
            btn.classList.add("active"); state.mode = btn.dataset.mode; refresh();
        }));

    document.getElementById("normalize-toggle").addEventListener("change", e => {
        state.normalize = e.target.checked; renderChart();
    });

    document.querySelectorAll(".table-tab").forEach(tab =>
        tab.addEventListener("click", () => {
            document.querySelectorAll(".table-tab").forEach(t => t.classList.remove("active"));
            tab.classList.add("active");
            document.querySelectorAll(".data-table").forEach(t => t.style.display = "none");
            document.getElementById(tab.dataset.target).style.display = "";
        }));

    // ── Modal ──
    const modal   = document.getElementById("modal-add");
    const input   = document.getElementById("input-new-ticker");
    const status  = document.getElementById("modal-status");
    const confirm = document.getElementById("btn-confirm-add");

    const openModal  = () => {
        modal.style.display = ""; input.value = "";
        status.textContent = ""; status.className = "modal-status";
        setTimeout(() => input.focus(), 50);
    };
    const closeModal = () => { modal.style.display = "none"; };

    document.getElementById("btn-add-ticker").addEventListener("click", openModal);
    document.getElementById("btn-cancel-add").addEventListener("click", closeModal);
    modal.addEventListener("click", e => { if (e.target === modal) closeModal(); });

    const doScrape = async () => {
        const ticker = input.value.trim().toUpperCase();
        if (!ticker) {
            status.textContent = "Enter a ticker symbol.";
            status.className = "modal-status error"; return;
        }
        confirm.disabled = true;
        status.textContent = ""; status.className = "modal-status";

        try {
            const res = await api("/scrape", {
                method: "POST",
                headers: { "Content-Type": "application/json" },
                body: JSON.stringify({ ticker }),
            });

            // Job accepted — close modal, hand off to toast
            closeModal();
            startPolling(res.job_id, res.ticker);

        } catch (err) {
            status.textContent = err.message;
            status.className = "modal-status error";
        } finally {
            confirm.disabled = false;
        }
    };

    confirm.addEventListener("click", doScrape);
    input.addEventListener("keydown", e => {
        if (e.key === "Enter")  doScrape();
        if (e.key === "Escape") closeModal();
    });

    // ── Boot ──
    Promise.all([
        fetchTickers().then(t => { state.tickers = t; renderSelects(); }),
        fetchMetrics().then(m => { state.metrics = m; renderMetricTabs(); }),
    ]).then(() => {
        const keys = Object.keys(state.metrics);
        if (keys.length) selectMetric(keys[0]);
        // Resume any scrapes that were running before page load/refresh
        resumeActiveJobs();
    });
});