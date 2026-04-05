# Global Monitor — Architecture Decision Log

## Project Structure
```
project/
  app/                         ← Web application (served by server.py)
    static/
      data/
        correlations/          ← Pre-computed windowed correlation JSON (by country + year range)
        forecasts/             ← Pre-computed ARIMA forecasts per country
        geo/                   ← GeoJSON topology files
      js/                      ← Future: split JS bundles
      css/                     ← Future: split stylesheets
    templates/
      index.html               ← Main dashboard
  etl/                         ← Data pipeline scripts
  db/                          ← SQLite database
  data/                        ← Raw + staged CSVs
  server.py                    ← Local Python server (entry point)
  build_data.py                ← Regenerates all static data files

## Decision Log

| ID   | Date       | Decision                  | Choice                              | Rationale                                      | Revisit when                        |
|------|------------|---------------------------|-------------------------------------|------------------------------------------------|-------------------------------------|
| AD-1 | 2026-04-05 | Payload strategy          | Python local server + data files    | Clean separation, no size ceiling              | Moving to cloud                     |
| AD-2 | 2026-04-05 | Globe fidelity            | CDN GeoJSON + bump map              | Professional quality, Natural Earth license    | Offline requirement                 |
| AD-3 | 2026-04-05 | Correlation computation   | Pre-computed JSON cache             | Scalable, JS fetches not computes              | >1000 indicator pairs               |
| AD-4 | 2026-04-05 | Node selection behaviour  | Click → auto-update impact table    | Better UX                                      | Never                               |
| AD-5 | 2026-04-05 | Database                  | SQLite → Postgres migration path    | SQLite fine to ~20M rows, then Supabase        | Adding 50+ countries                |

## Scaling Thresholds

| Component           | Current    | Warning threshold | Action required                          |
|---------------------|------------|-------------------|------------------------------------------|
| SQLite rows         | 3,419      | 10M rows          | Migrate to Supabase Postgres             |
| Correlation payload | ~100KB     | 5MB               | Paginate by country, lazy-load           |
| Forecast series     | 78         | 2000              | Move to on-demand API endpoint           |
| HTML file size      | 366KB      | 1MB               | Split into JS bundle + template          |
| Countries           | 5          | 50                | Add country-level lazy loading           |
| Cities              | 12         | 500               | Spatial index on dim_city lat/lon        |

## Data Source Registry

| Source  | Coverage          | Refresh  | Last pulled | Notes                          |
|---------|-------------------|----------|-------------|--------------------------------|
| WB      | 200+ countries    | Annual   | 2026-04-05  | API via wbgapi                 |
| IMF WEO | 190 countries     | Biannual | 2026-04-05  | DataMapper API                 |
| UNDP    | 190 countries     | Annual   | 2026-04-05  | CSV download                   |
| NASA    | Global cities     | Daily    | 2026-04-05  | Point API, aggregated annual   |
| SIPRI   | Not yet pulled    | Annual   | —           | Defense/geopolitical theme     |
| IHME    | Not yet pulled    | Annual   | —           | Health burden theme            |
| UN Comtrade | Not yet pulled | Annual  | —           | Trade/supply chain theme       |

## Pending Decisions

| ID   | Question                                          | Options                              | Blocking                        |
|------|---------------------------------------------------|--------------------------------------|---------------------------------|
| PD-1 | Cloud hosting when ready                          | Supabase + Vercel / AWS / self-host  | Client access feature           |
| PD-2 | Scenario shock engine algorithm                  | VAR model / agent-based / hybrid     | Scenario dashboard feature      |
| PD-3 | Mineral/supply chain data source                 | UN Comtrade / USGS / proprietary     | Minerals theme                  |
| PD-4 | Authentication for client access                 | Supabase auth / OAuth / simple token | Client-facing deployment        |
