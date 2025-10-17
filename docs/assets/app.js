(() => {
  const usd = new Intl.NumberFormat('en-US', { style: 'currency', currency: 'USD', maximumFractionDigits: 0 });
  const fmt = v => usd.format(v);

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

  function movingAverage(arr, window = 3) {
    const out = [];
    for (let i = 0; i < arr.length; i++) {
      const s = Math.max(0, i - window + 1);
      const win = arr.slice(s, i + 1);
      out.push(win.reduce((a, b) => a + b, 0) / win.length);
    }
    return out;
  }

  function sliceRange(labels, payload, range) {
    const n = labels.length;
    let start = 0;
    if (range === '6m') start = Math.max(0, n - 6);
    else if (range === '3m') start = Math.max(0, n - 3);
    return {
      labels: labels.slice(start),
      total_sales: payload.total_sales.slice(start),
      online_sales: payload.online_sales.slice(start),
      in_store_sales: payload.in_store_sales.slice(start),
      scatter_points: payload.scatter_points.slice(start),
      regression_points: payload.regression_points.slice(start)
    };
  }

  async function loadData() {
    const errEl = qs('#error');
    try {
      const res = await fetch('assets/data.json', { cache: 'no-cache' });
      if (!res.ok) throw new Error(`Fetch failed: ${res.status}`);
      return await res.json();
    } catch (e) {
      errEl.style.display = '';
      errEl.textContent = `Failed to load data.json. Did you run scripts/spark_etl.py? (${e.message})`;
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

  function buildCharts(labels, data, state) {
    const totalCfg = withZoom({
      type: 'line',
      data: {
        labels,
        datasets: [
          {
            label: 'Total sales',
            data: data.total_sales,
            tension: 0.25,
            borderColor: 'rgb(16, 124, 165)',
            backgroundColor: 'rgba(16, 124, 165, 0.1)',
            pointRadius: 4,
            pointHoverRadius: 6,
          }
        ]
      },
      options: {
        responsive: true,
        maintainAspectRatio: false,
        interaction: { mode: 'index', intersect: false },
        scales: { y: { ticks: { callback: v => fmt(v) } } },
        plugins: {
          tooltip: { callbacks: { label: c => `${c.dataset.label}: ${fmt(c.parsed.y)}` } },
          legend: { display: false }
        }
      }
    });

    const barCfg = withZoom({
      type: 'bar',
      data: {
        labels,
        datasets: [
          { label: 'In-store sales', data: data.in_store_sales, backgroundColor: 'rgba(234, 88, 12, 0.85)', stack: 'sales', hidden: !state.showInstore },
          { label: 'Online sales', data: data.online_sales, backgroundColor: 'rgba(34, 197, 94, 0.85)', stack: 'sales', hidden: !state.showOnline }
        ]
      },
      options: {
        responsive: true,
        maintainAspectRatio: false,
        interaction: { mode: 'index', intersect: false },
        scales: { y: { stacked: true, ticks: { callback: v => fmt(v) } }, x: { stacked: true } },
        plugins: { tooltip: { callbacks: { label: c => `${c.dataset.label}: ${fmt(c.parsed.y)}` } } }
      }
    });

    const scatterCfg = withZoom({
      type: 'scatter',
      data: {
        labels,
        datasets: [
          { label: 'Monthly totals', data: data.scatter_points, backgroundColor: 'rgba(59, 130, 246, 0.85)', pointRadius: 6 },
          { type: 'line', label: 'Regression line', data: data.regression_points, borderColor: 'rgba(239, 68, 68, 0.9)', borderWidth: 2, pointRadius: 0, fill: false, tension: 0 }
        ]
      },
      options: {
        responsive: true,
        maintainAspectRatio: false,
        interaction: { mode: 'nearest', intersect: true },
        scales: {
          x: { title: { display: true, text: 'Marketing spend (USD)' }, ticks: { callback: v => fmt(v) } },
          y: { title: { display: true, text: 'Total sales (USD)' }, ticks: { callback: v => fmt(v) } }
        },
        plugins: { tooltip: { callbacks: { label: c => `${fmt(c.parsed.x)} → ${fmt(c.parsed.y)}` } } }
      }
    });

    const totalChart = new Chart(qs('#totalSalesChart'), totalCfg);
    const barChart = new Chart(qs('#channelBreakdownChart'), barCfg);
    const scatterChart = new Chart(qs('#marketingScatterChart'), scatterCfg);

    return { totalChart, barChart, scatterChart };
  }

  function syncHover(charts) {
    function handler(srcChart, evt) {
      const actives = srcChart.getElementsAtEventForMode(evt, 'index', { intersect: false }, false);
      const index = actives?.[0]?.index;
      if (index == null) return;
      Object.values(charts).forEach(ch => {
        if (ch === srcChart) return;
        const meta = ch.getDatasetMeta(0);
        const el = meta?.data?.[index];
        if (!el) return;
        ch.setActiveElements([{ datasetIndex: 0, index }]);
        ch.tooltip.setActiveElements([{ datasetIndex: 0, index }], { x: el.x, y: el.y });
        ch.update('none');
      });
    }
    Object.values(charts).forEach(ch => {
      ch.canvas.addEventListener('mousemove', e => handler(ch, e));
      ch.canvas.addEventListener('mouseleave', () => {
        Object.values(charts).forEach(c => { c.setActiveElements([]); c.tooltip.setActiveElements([], {x:0,y:0}); c.update('none'); });
      });
    });
  }

  function hookControls(state, charts, baseLabels, payload) {
    // Range chips
    qsa('[data-range]').forEach(btn => {
      btn.addEventListener('click', () => {
        qsa('[data-range]').forEach(b => b.classList.remove('active'));
        btn.classList.add('active');
        state.range = btn.dataset.range;
        render();
      });
    });
    // Toggles
    qs('#toggleMA').addEventListener('change', e => { state.showMA = e.target.checked; render(); });
    qs('#toggleOnline').addEventListener('change', e => { state.showOnline = e.target.checked; render(); });
    qs('#toggleInstore').addEventListener('change', e => { state.showInstore = e.target.checked; render(); });

    // Zoom reset
    qs('#resetZoom').addEventListener('click', () => {
      charts.totalChart.resetZoom?.();
      charts.barChart.resetZoom?.();
      charts.scatterChart.resetZoom?.();
    });
    // Export PNG (from total chart)
    qs('#exportPng').addEventListener('click', () => {
      const url = charts.totalChart.toBase64Image('image/png', 1);
      const a = document.createElement('a');
      a.href = url; a.download = 'coffee-sales.png'; a.click();
    });

    function render() {
      const sliced = sliceRange(baseLabels, payload, state.range || 'all');
      // Labels
      charts.totalChart.data.labels = sliced.labels;
      charts.barChart.data.labels = sliced.labels;
      // Total chart datasets
      const baseTotal = sliced.total_sales;
      const ma = movingAverage(baseTotal, 3);
      charts.totalChart.data.datasets = [
        {
          label: 'Total sales', data: baseTotal, tension: .25,
          borderColor: 'rgb(16,124,165)', backgroundColor: 'rgba(16,124,165,.1)', pointRadius: 4, pointHoverRadius: 6
        },
      ];
      if (state.showMA) {
        charts.totalChart.data.datasets.push({ label: 'Moving Avg (3)', data: ma, borderDash: [6,4], borderColor: '#10b981', pointRadius: 0, tension: .2 });
      }
      // Bar chart toggle datasets
      charts.barChart.data.datasets = [
        { label: 'In-store sales', data: sliced.in_store_sales, backgroundColor: 'rgba(234,88,12,.85)', stack: 'sales', hidden: !state.showInstore },
        { label: 'Online sales', data: sliced.online_sales, backgroundColor: 'rgba(34,197,94,.85)', stack: 'sales', hidden: !state.showOnline }
      ];

      charts.totalChart.update();
      charts.barChart.update();
      charts.scatterChart.update();
    }

    return render;
  }

  async function init() {
    initTheme();
    qs('#themeToggle').addEventListener('click', () => {
      const next = document.documentElement.getAttribute('data-theme') === 'dark' ? 'light' : 'dark';
      setTheme(next);
    });

    const payload = await loadData();
    const rawLabels = payload.months.map(String);
    const labels = rawLabels.map(m => m.slice(0, 7)); // YYYY-MM

    const state = { showMA:false, showOnline:true, showInstore:true, range:'all' };
    const charts = buildCharts(labels, payload, state);
    const render = hookControls(state, charts, labels, payload);
    syncHover(charts);

    // Activate default range button
    const defaultBtn = qs('[data-range="all"]');
    if (defaultBtn) defaultBtn.classList.add('active');

    // Initial render to apply any control state
    render();
  }

  window.addEventListener('DOMContentLoaded', init);
})();

