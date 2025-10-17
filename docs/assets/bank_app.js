(() => {
  const usd = new Intl.NumberFormat('en-US', { style: 'currency', currency: 'USD', maximumFractionDigits: 0 });
  const fmt = v => usd.format(v);
  const qs = s => document.querySelector(s);
  const qsa = s => Array.from(document.querySelectorAll(s));

  function setTheme(next) { document.documentElement.setAttribute('data-theme', next); localStorage.setItem('theme', next); qs('#themeToggle').textContent = next === 'dark' ? '☀️' : '🌙'; }
  function initTheme() { const saved = localStorage.getItem('theme'); if (saved) return setTheme(saved); setTheme(window.matchMedia('(prefers-color-scheme: dark)').matches ? 'dark' : 'light'); }

  async function loadData() {
    const errEl = qs('#error');
    try {
      const res = await fetch('assets/bank_data.json', { cache: 'no-cache' });
      if (!res.ok) throw new Error(`Fetch failed: ${res.status}`);
      return await res.json();
    } catch (e) {
      errEl.style.display = '';
      errEl.textContent = 'Failed to load bank_data.json. Run: spark-submit scripts/bank_fraud_etl.py';
      throw e;
    }
  }

  function withZoom(options) { options.plugins = options.plugins || {}; options.plugins.zoom = { pan: { enabled: true, mode: 'xy', modifierKey: 'ctrl' }, zoom: { wheel: { enabled: true }, pinch: { enabled: true }, mode: 'xy' } }; return options; }

  function sliceRange(payload, range) {
    if (range === 'all') return payload;
    const n = payload.dates.length;
    const days = range === '30d' ? 30 : range === '14d' ? 14 : 7;
    const start = Math.max(0, n - days);
    return { ...payload, dates: payload.dates.slice(start), tx_count: payload.tx_count.slice(start), fraud_count: payload.fraud_count.slice(start), fraud_rate: payload.fraud_rate.slice(start), total_amount: payload.total_amount.slice(start), fraud_amount: payload.fraud_amount.slice(start) };
  }

  function buildCharts(data) {
    const tsCfg = withZoom({
      type: 'bar',
      data: { labels: data.dates, datasets: [
        { type: 'bar', label: 'Transactions', data: data.tx_count, backgroundColor: 'rgba(59,130,246,.75)', yAxisID: 'y' },
        { type: 'line', label: 'Fraud rate %', data: data.fraud_rate, borderColor: 'rgba(239,68,68,.95)', backgroundColor: 'rgba(239,68,68,.1)', tension: .25, yAxisID: 'y1', pointRadius: 3 }
      ]},
      options: { responsive: true, maintainAspectRatio: false, interaction: { mode: 'index', intersect: false }, scales: { y: { title: { display: true, text: 'Transactions' } }, y1: { position:'right', grid:{ drawOnChartArea:false }, title:{ display:true, text:'Fraud rate (%)' }, ticks:{ callback: v => v + '%' } } }, plugins: { tooltip: { callbacks: { label: c => c.dataset.type === 'line' ? `${c.dataset.label}: ${c.parsed.y}%` : `${c.dataset.label}: ${c.parsed.y}` } } } }
    });

    const acct = data.top_accounts.slice().sort((a,b)=>b.fraud_amount-a.fraud_amount);
    const acctCfg = withZoom({ type: 'bar', data: { labels: acct.map(a=>a.account_id), datasets: [{ label: 'Fraud amount', data: acct.map(a=>a.fraud_amount), backgroundColor: 'rgba(234,88,12,.85)' }] }, options: { indexAxis: 'y', responsive: true, maintainAspectRatio: false, plugins: { tooltip: { callbacks: { label: c => `${c.dataset.label}: ${fmt(c.parsed.x)}` } } }, scales: { x: { ticks: { callback: v => fmt(v) } } } } });

    const chanCfg = withZoom({ type: 'bar', data: { labels: data.channels.map(c=>`${c.channel} (${c.fraud_rate}%)`), datasets: [ { label:'Legit amount', data: data.channels.map(c=>c.legit_amount), backgroundColor:'rgba(16,185,129,.85)', stack:'amt' }, { label:'Fraud amount', data: data.channels.map(c=>c.fraud_amount), backgroundColor:'rgba(239,68,68,.85)', stack:'amt' } ] }, options: { responsive:true, maintainAspectRatio:false, scales:{ y:{ stacked:true, ticks:{ callback: v => fmt(v) } }, x:{ stacked:true } }, plugins:{ tooltip:{ callbacks:{ label:c=>`${c.dataset.label}: ${fmt(c.parsed.y)}` } } } } });

    const tsChart = new Chart(qs('#tsChart'), tsCfg);
    const acctChart = new Chart(qs('#acctChart'), acctCfg);
    const chanChart = new Chart(qs('#chanChart'), chanCfg);
    return { tsChart, acctChart, chanChart };
  }

  function initMap(points) {
    const map = L.map('map').setView([39.5, -98.5], 4);
    L.tileLayer('https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png', { maxZoom: 18, attribution: '&copy; OpenStreetMap' }).addTo(map);
    points.forEach(p => {
      const m = L.circleMarker([p.lat, p.lon], { radius: 6, color: '#ef4444', fillColor:'#ef4444', fillOpacity:.85 });
      m.bindPopup(`<strong>${p.account_id}</strong><br/>${p.channel}<br/>${fmt(p.amount)}<br/><small>${p.ts}</small>`);
      m.addTo(map);
    });
    return map;
  }

  function hookControls(charts, payload, map) {
    qsa('[data-range]').forEach(btn => {
      btn.addEventListener('click', () => {
        qsa('[data-range]').forEach(b=>b.classList.remove('active')); btn.classList.add('active');
        const sliced = sliceRange(payload, btn.dataset.range);
        charts.tsChart.data.labels = sliced.dates;
        charts.tsChart.data.datasets[0].data = sliced.tx_count;
        charts.tsChart.data.datasets[1].data = sliced.fraud_rate;
        charts.tsChart.update();
        qs('#tsSummary').textContent = `Tx: ${sliced.tx_count.reduce((a,b)=>a+b,0)} • Avg fraud rate: ${ (sliced.fraud_rate.reduce((a,b)=>a+b,0) / Math.max(1,sliced.fraud_rate.length)).toFixed(2) }%`;
      });
    });
    qs('#resetZoom').addEventListener('click', () => { charts.tsChart.resetZoom?.(); charts.acctChart.resetZoom?.(); charts.chanChart.resetZoom?.(); });
    qs('#exportPng').addEventListener('click', () => { const url = charts.tsChart.toBase64Image('image/png',1); const a = document.createElement('a'); a.href=url; a.download='bank-fraud-timeseries.png'; a.click(); });
  }

  async function init() {
    initTheme();
    qs('#themeToggle').addEventListener('click', () => { const next = document.documentElement.getAttribute('data-theme') === 'dark' ? 'light' : 'dark'; setTheme(next); });

    const payload = await loadData();
    const charts = buildCharts(payload);
    const map = initMap(payload.geo_points || []);
    hookControls(charts, payload, map);
    const defaultBtn = qs('[data-range="all"]'); if (defaultBtn) defaultBtn.classList.add('active');
    qs('#tsSummary').textContent = `Tx: ${payload.tx_count.reduce((a,b)=>a+b,0)} • Avg fraud rate: ${(payload.fraud_rate.reduce((a,b)=>a+b,0)/Math.max(1,payload.fraud_rate.length)).toFixed(2)}%`;
  }

  window.addEventListener('DOMContentLoaded', init);
})();

