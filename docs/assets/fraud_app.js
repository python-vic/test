(() => {
  const usd = new Intl.NumberFormat('en-US', { style: 'currency', currency: 'USD', maximumFractionDigits: 0 });
  const pct = new Intl.NumberFormat('en-US', { style: 'percent', maximumFractionDigits: 1 });
  const fmtUsd = v => usd.format(v);

  const qs = s => document.querySelector(s);
  const qsa = s => Array.from(document.querySelectorAll(s));

  function setTheme(next) {
    document.documentElement.setAttribute('data-theme', next);
    localStorage.setItem('theme', next);
    qs('#themeToggle').textContent = next === 'dark' ? '☀️' : '🌙';
  }
  function initTheme() {
    const saved = localStorage.getItem('theme');
    if (saved) return setTheme(saved);
    const prefersDark = window.matchMedia('(prefers-color-scheme: dark)').matches;
    setTheme(prefersDark ? 'dark' : 'light');
  }

  async function loadData() {
    const errEl = qs('#error');
    try {
      const res = await fetch('assets/fraud_data.json', { cache: 'no-cache' });
      if (!res.ok) throw new Error(`Fetch failed: ${res.status}`);
      return await res.json();
    } catch (e) {
      errEl.style.display = '';
      errEl.textContent = `Failed to load fraud_data.json. Run: spark-submit scripts/fraud_etl.py`;
      throw e;
    }
  }

  function withZoom(options) {
    options.plugins = options.plugins || {};
    options.plugins.zoom = {
      pan: { enabled: true, mode: 'xy', modifierKey: 'ctrl' },
      zoom: { wheel: { enabled: true }, pinch: { enabled: true }, mode: 'xy' }
    };
    return options;
  }

  function sliceRange(payload, range) {
    if (range === 'all') return payload;
    const n = payload.dates.length;
    const days = range === '30d' ? 30 : range === '14d' ? 14 : 7;
    const start = Math.max(0, n - days);
    return {
      dates: payload.dates.slice(start),
      tx_count: payload.tx_count.slice(start),
      fraud_count: payload.fraud_count.slice(start),
      fraud_rate: payload.fraud_rate.slice(start),
      total_amount: payload.total_amount.slice(start),
      fraud_amount: payload.fraud_amount.slice(start),
      top_merchants: payload.top_merchants,
      channels: payload.channels,
      hours: payload.hours,
    };
  }

  function buildCharts(data) {
    // Time series: tx_count (left), fraud_rate% (right)
    const tsCfg = withZoom({
      type: 'bar',
      data: {
        labels: data.dates,
        datasets: [
          { type: 'bar', label: 'Transactions', data: data.tx_count, backgroundColor: 'rgba(99, 102, 241, 0.7)', yAxisID: 'y' },
          { type: 'line', label: 'Fraud rate %', data: data.fraud_rate, borderColor: 'rgba(239, 68, 68, 0.95)', backgroundColor: 'rgba(239,68,68,.1)', tension: 0.25, yAxisID: 'y1', pointRadius: 3 }
        ]
      },
      options: {
        responsive: true,
        maintainAspectRatio: false,
        interaction: { mode: 'index', intersect: false },
        scales: {
          y: { title: { display: true, text: 'Transactions' } },
          y1: { position: 'right', grid: { drawOnChartArea: false }, title: { display: true, text: 'Fraud rate (%)' }, ticks: { callback: v => v + '%' } }
        },
        plugins: {
          tooltip: { callbacks: { label: c => c.dataset.label.includes('Fraud') ? `${c.dataset.label}: ${c.parsed.y}%` : `${c.dataset.label}: ${c.parsed.y}` } }
        }
      }
    });

    // Top merchants: horizontal bar by fraud_amount
    const merchants = data.top_merchants.slice().sort((a,b) => b.fraud_amount - a.fraud_amount);
    const merchCfg = withZoom({
      type: 'bar',
      data: {
        labels: merchants.map(m => m.merchant),
        datasets: [
          { label: 'Fraud amount', data: merchants.map(m => m.fraud_amount), backgroundColor: 'rgba(234, 88, 12, 0.85)' },
        ]
      },
      options: {
        indexAxis: 'y',
        responsive: true,
        maintainAspectRatio: false,
        plugins: { tooltip: { callbacks: { label: c => `${c.dataset.label}: ${fmtUsd(c.parsed.x)}` } } },
        scales: { x: { ticks: { callback: v => fmtUsd(v) } } }
      }
    });

    // Channel breakdown: stacked fraud vs legit amounts
    const channels = data.channels;
    const chanCfg = withZoom({
      type: 'bar',
      data: {
        labels: channels.map(c => `${c.channel} (${c.fraud_rate}%)`),
        datasets: [
          { label: 'Legit amount', data: channels.map(c => c.legit_amount), backgroundColor: 'rgba(16, 185, 129, 0.85)', stack: 'amt' },
          { label: 'Fraud amount', data: channels.map(c => c.fraud_amount), backgroundColor: 'rgba(239, 68, 68, 0.85)', stack: 'amt' },
        ]
      },
      options: {
        responsive: true,
        maintainAspectRatio: false,
        scales: { y: { stacked: true, ticks: { callback: v => fmtUsd(v) } }, x: { stacked: true } },
        plugins: { tooltip: { callbacks: { label: c => `${c.dataset.label}: ${fmtUsd(c.parsed.y)}` } } }
      }
    });

    const tsChart = new Chart(qs('#tsChart'), tsCfg);
    const merchantChart = new Chart(qs('#merchantChart'), merchCfg);
    const channelChart = new Chart(qs('#channelChart'), chanCfg);
    return { tsChart, merchantChart, channelChart };
  }

  function hookControls(charts, payload) {
    qsa('[data-range]').forEach(btn => {
      btn.addEventListener('click', () => {
        qsa('[data-range]').forEach(b => b.classList.remove('active'));
        btn.classList.add('active');
        const sliced = sliceRange(payload, btn.dataset.range);
        charts.tsChart.data.labels = sliced.dates;
        charts.tsChart.data.datasets[0].data = sliced.tx_count;
        charts.tsChart.data.datasets[1].data = sliced.fraud_rate;
        charts.tsChart.update();
      });
    });
    qs('#resetZoom').addEventListener('click', () => {
      charts.tsChart.resetZoom?.();
      charts.merchantChart.resetZoom?.();
      charts.channelChart.resetZoom?.();
    });
    qs('#exportPng').addEventListener('click', () => {
      const url = charts.tsChart.toBase64Image('image/png', 1);
      const a = document.createElement('a');
      a.href = url; a.download = 'fraud-dashboard.png'; a.click();
    });
  }

  async function init() {
    initTheme();
    qs('#themeToggle').addEventListener('click', () => {
      const next = document.documentElement.getAttribute('data-theme') === 'dark' ? 'light' : 'dark';
      setTheme(next);
    });

    const payload = await loadData();
    const charts = buildCharts(payload);
    hookControls(charts, payload);
    const defaultBtn = qs('[data-range="all"]');
    if (defaultBtn) defaultBtn.classList.add('active');
  }

  window.addEventListener('DOMContentLoaded', init);
})();

