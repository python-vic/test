---
title: "Coffee Sales Analysis"
layout: default
---
# Coffee Chain Sales Analysis

## Key Metrics
- Average monthly marketing spend: $16,666.67
- Average monthly online sales: $40,416.67
- Average monthly in-store sales: $32,250.00
- Average monthly total sales: $72,666.67
- Median monthly total sales: $74,250.00
- Standard deviation of total sales: $9,230.22

## Relationship Between Marketing and Sales
- Pearson correlation between marketing spend and total sales: 0.977. Values close to 1 indicate a strong positive relationship.
- Simple linear regression of total sales on marketing spend yields a slope of 2.60 and intercept of $29,366.83. Every additional dollar of marketing spend is associated with the slope amount in sales.

## Month-over-Month Growth
- 2023-01 to 2023-02: -5.0% change in total sales
- 2023-02 to 2023-03: 15.8% change in total sales
- 2023-03 to 2023-04: -2.3% change in total sales
- 2023-04 to 2023-05: 16.3% change in total sales
- 2023-05 to 2023-06: 5.3% change in total sales
- 2023-06 to 2023-07: 5.1% change in total sales
- 2023-07 to 2023-08: -4.8% change in total sales
- 2023-08 to 2023-09: -2.5% change in total sales
- 2023-09 to 2023-10: -4.5% change in total sales
- 2023-10 to 2023-11: -3.4% change in total sales
- 2023-11 to 2023-12: 22.5% change in total sales

- Highest growth occurred in 2023-12 with a 22.5% increase over the prior month.

## Online Sales Share
- Average online sales share of total revenue: 55.5%
- Highest online share observed in 2023-06 at 57.0%

## Visualizations
![Monthly total sales trend](assets/coffee_total_sales_trend.png)
![Channel sales breakdown](assets/coffee_channel_sales_breakdown.png)
![Marketing vs sales scatter](assets/coffee_marketing_vs_sales.png)
## Interactive Visualizations

<style>
.chart-grid {
  display: grid;
  gap: 1.75rem;
  margin-top: 1.5rem;
}
@media (min-width: 900px) {
  .chart-grid {
    grid-template-columns: repeat(2, minmax(0, 1fr));
  }
  .chart-grid .chart-card:nth-child(3) {
    grid-column: span 2;
  }
}
.chart-card {
  background: #ffffff;
  border: 1px solid #e5e7eb;
  border-radius: 12px;
  padding: 1.5rem;
  box-shadow: 0 10px 25px rgba(15, 23, 42, 0.08);
}
.chart-card h3 {
  margin-top: 0;
  margin-bottom: 1rem;
  font-size: 1.1rem;
}
.chart-card canvas {
  max-height: 420px;
}
</style>
<div class="chart-grid">
  <section class="chart-card">
    <h3>Monthly Total Sales</h3>
    <canvas id="totalSalesChart"></canvas>
  </section>
  <section class="chart-card">
    <h3>Sales Breakdown by Channel</h3>
    <canvas id="channelBreakdownChart"></canvas>
  </section>
  <section class="chart-card">
    <h3>Marketing Spend vs. Total Sales</h3>
    <canvas id="marketingScatterChart"></canvas>
  </section>
</div>
<script id="coffee-chart-data" type="application/json">
{"months": ["2023-01", "2023-02", "2023-03", "2023-04", "2023-05", "2023-06", "2023-07", "2023-08", "2023-09", "2023-10", "2023-11", "2023-12"], "total_sales": [60000.0, 57000.0, 66000.0, 64500.0, 75000.0, 79000.0, 83000.0, 79000.0, 77000.0, 73500.0, 71000.0, 87000.0], "online_sales": [32000.0, 31000.0, 36000.0, 35000.0, 42000.0, 45000.0, 47000.0, 44000.0, 43000.0, 41000.0, 40000.0, 49000.0], "in_store_sales": [28000.0, 26000.0, 30000.0, 29500.0, 33000.0, 34000.0, 36000.0, 35000.0, 34000.0, 32500.0, 31000.0, 38000.0], "marketing_spend": [12000.0, 11000.0, 15000.0, 14000.0, 18000.0, 20000.0, 21000.0, 19000.0, 17000.0, 16000.0, 15000.0, 22000.0], "scatter_points": [{"x": 12000.0, "y": 60000.0}, {"x": 11000.0, "y": 57000.0}, {"x": 15000.0, "y": 66000.0}, {"x": 14000.0, "y": 64500.0}, {"x": 18000.0, "y": 75000.0}, {"x": 20000.0, "y": 79000.0}, {"x": 21000.0, "y": 83000.0}, {"x": 19000.0, "y": 79000.0}, {"x": 17000.0, "y": 77000.0}, {"x": 16000.0, "y": 73500.0}, {"x": 15000.0, "y": 71000.0}, {"x": 22000.0, "y": 87000.0}], "regression_points": [{"x": 12000.0, "y": 60542.71356783919}, {"x": 11000.0, "y": 57944.72361809046}, {"x": 15000.0, "y": 68336.68341708543}, {"x": 14000.0, "y": 65738.69346733668}, {"x": 18000.0, "y": 76130.65326633166}, {"x": 20000.0, "y": 81326.63316582915}, {"x": 21000.0, "y": 83924.62311557788}, {"x": 19000.0, "y": 78728.64321608041}, {"x": 17000.0, "y": 73532.66331658291}, {"x": 16000.0, "y": 70934.67336683418}, {"x": 15000.0, "y": 68336.68341708543}, {"x": 22000.0, "y": 86522.61306532664}]}
</script>
<script src="https://cdn.jsdelivr.net/npm/chart.js@4.4.0/dist/chart.umd.min.js"></script>
<script>
const chartPayload = JSON.parse(document.getElementById('coffee-chart-data').textContent);
const usdFormatter = new Intl.NumberFormat('en-US', { style: 'currency', currency: 'USD', maximumFractionDigits: 0 });
const numberFormatter = value => usdFormatter.format(value);

new Chart(document.getElementById('totalSalesChart'), {
  type: 'line',
  data: {
    labels: chartPayload.months,
    datasets: [{
      label: 'Total sales',
      data: chartPayload.total_sales,
      fill: false,
      tension: 0.25,
      borderColor: 'rgb(16, 124, 165)',
      backgroundColor: 'rgba(16, 124, 165, 0.1)',
      pointRadius: 5,
      pointHoverRadius: 7,
    }]
  },
  options: {
    responsive: true,
    maintainAspectRatio: false,
    scales: {
      y: {
        ticks: { callback: value => numberFormatter(value) },
        beginAtZero: false,
      }
    },
    plugins: {
      tooltip: {
        callbacks: {
          label: context => `${context.dataset.label}: ${numberFormatter(context.parsed.y)}`
        }
      },
      legend: { display: false }
    }
  }
});

new Chart(document.getElementById('channelBreakdownChart'), {
  type: 'bar',
  data: {
    labels: chartPayload.months,
    datasets: [
      {
        label: 'In-store sales',
        data: chartPayload.in_store_sales,
        backgroundColor: 'rgba(234, 88, 12, 0.85)',
        stack: 'sales',
      },
      {
        label: 'Online sales',
        data: chartPayload.online_sales,
        backgroundColor: 'rgba(34, 197, 94, 0.85)',
        stack: 'sales',
      }
    ]
  },
  options: {
    responsive: true,
    maintainAspectRatio: false,
    scales: {
      y: {
        stacked: true,
        ticks: { callback: value => numberFormatter(value) },
        beginAtZero: true,
      },
      x: { stacked: true }
    },
    plugins: {
      tooltip: {
        callbacks: {
          label: context => `${context.dataset.label}: ${numberFormatter(context.parsed.y)}`
        }
      }
    }
  }
});

new Chart(document.getElementById('marketingScatterChart'), {
  type: 'scatter',
  data: {
    datasets: [
      {
        label: 'Monthly totals',
        data: chartPayload.scatter_points,
        backgroundColor: 'rgba(59, 130, 246, 0.85)',
        pointRadius: 6,
      },
      {
        type: 'line',
        label: 'Regression line',
        data: chartPayload.regression_points,
        borderColor: 'rgba(239, 68, 68, 0.9)',
        borderWidth: 2,
        pointRadius: 0,
        fill: false,
        tension: 0,
      }
    ]
  },
  options: {
    responsive: true,
    maintainAspectRatio: false,
    scales: {
      x: {
        title: { display: true, text: 'Marketing spend (USD)' },
        ticks: { callback: value => numberFormatter(value) },
      },
      y: {
        title: { display: true, text: 'Total sales (USD)' },
        ticks: { callback: value => numberFormatter(value) },
      }
    },
    plugins: {
      tooltip: {
        callbacks: {
          label: context => `${context.dataset.label}: (${numberFormatter(context.parsed.x)}, ${numberFormatter(context.parsed.y)})`
        }
      }
    }
  }
});
</script>
