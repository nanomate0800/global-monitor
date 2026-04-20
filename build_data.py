"""
build_data.py
Pre-computes all static data files for the Global Monitor app.
Run this after any database update: python3 build_data.py
Outputs land in app/static/data/
"""
import sys, json, sqlite3, numpy as np, pandas as pd, warnings, os, math
from itertools import combinations, product as iproduct
from collections import defaultdict
from pathlib import Path
if hasattr(sys.stdout, 'reconfigure'):
    try:
        sys.stdout.reconfigure(encoding='utf-8')
    except Exception:
        pass
sys.stdout.flush()
warnings.filterwarnings('ignore')

def _f(v, decimals=4):
    """Round float to decimals; return None for NaN/Inf (serialises as JSON null)."""
    try:
        if v is None or (isinstance(v, float) and not math.isfinite(v)):
            return None
        return round(float(v), decimals)
    except Exception:
        return None

class _SafeEncoder(json.JSONEncoder):
    """Converts NaN/Inf floats to null so output is always valid JSON."""
    def iterencode(self, o, _one_shot=False):
        # Walk the structure and sanitise floats before encoding
        return super().iterencode(self._sanitise(o), _one_shot)
    def _sanitise(self, obj):
        if isinstance(obj, float):
            return None if not math.isfinite(obj) else obj
        if isinstance(obj, dict):
            return {k: self._sanitise(v) for k, v in obj.items()}
        if isinstance(obj, (list, tuple)):
            return [self._sanitise(v) for v in obj]
        return obj
from statsmodels.tsa.arima.model import ARIMA
from statsmodels.tsa.stattools import adfuller

BASE     = Path(__file__).parent
DB_PATH  = BASE / 'db' / 'database.db'
OUT_BASE = BASE / 'app' / 'static' / 'data'
CORR_DIR = OUT_BASE / 'correlations'
FC_DIR   = OUT_BASE / 'forecasts'
GEO_DIR  = OUT_BASE / 'geo'
for d in [OUT_BASE, CORR_DIR, FC_DIR, GEO_DIR]:
    d.mkdir(parents=True, exist_ok=True)

COUNTRIES   = ['USA','KEN','BRA','DEU','CHN','RUS','SGP','IND','JPN','IDN']
HIST_START  = 2000
HIST_END    = 2025
FC_HORIZON  = 10
FC_YEARS    = list(range(HIST_END+1, HIST_END+FC_HORIZON+1))
YEAR_WINDOWS = [
    (2000,2010),(2005,2015),(2010,2020),(2015,2025),
    (2000,2023),(2010,2023),(2018,2023),
    (2000,2038),(2015,2038),(2020,2038),  # forecast windows
]

print("="*55)
print("Global Monitor — Data Build")
print("="*55)

# ── LOAD DATA ──
conn = sqlite3.connect(DB_PATH)
df = pd.read_sql('''
    SELECT f.iso3, c.country_name, f.year,
           i.source_indicator_code as indicator,
           i.indicator_name, i.category, i.unit, f.value
    FROM fact_indicators f
    JOIN dim_indicator i ON f.indicator_id=i.indicator_id
    JOIN dim_country c   ON f.country_id=c.country_id
    WHERE f.city_id IS NULL AND f.value IS NOT NULL
      AND f.year BETWEEN 2000 AND 2023
    ORDER BY f.iso3, f.year
''', conn)
df = df.drop_duplicates(subset=['iso3','year','indicator'])

# Build indicator_name → source lookup from source_indicator_code prefix
def _infer_source(code):
    code = str(code or '')
    if code.startswith('NASA'):  return 'NASA'
    if code.startswith('FAO'):   return 'FAOSTAT'
    if code.startswith('IEA'):   return 'IEA'
    if code.startswith('UNDP'):  return 'UNDP'
    if code.startswith('IMF'):   return 'IMF'
    if code.startswith('CT'):    return 'Comtrade'
    if code.startswith('FT'):    return 'FastTrack'
    if code.startswith('WHO'):   return 'WHO'
    if code.startswith('ILO'):   return 'ILO'
    if code.startswith('WTO'):   return 'WTO'
    if code.startswith('UNPOP'):  return 'UN Population'
    if code.startswith('SCHAIN'): return 'Supply Chain'
    if code.startswith('UNCTAD'): return 'UNCTAD'
    if code.startswith('DAMO'):   return 'Damodaran'
    if code.startswith('FRASER'): return 'Fraser'
    if code.startswith('WBINNOV'): return 'WBInnov'
    if code.startswith('DB_'):    return 'DoingBusiness'
    if code.startswith('USGS'):   return 'USGS'
    return 'WB'

# Pull unique indicator→source map from db
_ind_src_df = pd.read_sql('SELECT source_indicator_code, indicator_name FROM dim_indicator', conn)
IND_SOURCE = {row['indicator_name']: _infer_source(row['source_indicator_code'])
              for _, row in _ind_src_df.iterrows()}

# Add derived metrics to source map
IND_SOURCE['Heating Degree Days (HDD)'] = 'NASA'
IND_SOURCE['Cooling Degree Days (CDD)'] = 'NASA'
IND_SOURCE['Temperature Anomaly (from 2000-2005 baseline)'] = 'NASA'

city_df = pd.read_sql('''
    SELECT f.iso3, ci.city_name, ci.lat, ci.lon, f.year,
           i.source_indicator_code as param,
           i.indicator_name, f.value
    FROM fact_indicators f
    JOIN dim_indicator i ON f.indicator_id=i.indicator_id
    JOIN dim_city ci     ON f.city_id=ci.city_id
    WHERE f.value IS NOT NULL
    ORDER BY f.iso3, ci.city_name, f.year
''', conn)
conn.close()
print(f"Loaded {len(df)} country rows, {len(city_df)} city rows")

# ── COMPUTE DERIVED CLIMATE METRICS ──────────────────────────────────────
# HDD (Heating Degree Days), CDD (Cooling Degree Days), Temperature Anomaly
# These are computed from NASA T2M (2m temperature in Celsius) to correlate with energy/economics

def compute_hdd_cdd(temps_celsius, baseline=65):
    """
    Compute annual Heating/Cooling Degree Days from annual mean temperature.
    Uses 65°F (18.3°C) as baseline per energy industry standard.
    - HDD: Sum of max(0, 65 - temp_F) for each day; approximated as max(0, 65 - annual_mean_F)
    - CDD: Sum of max(0, temp_F - 65) for each day; approximated as max(0, annual_mean_F - 65)

    For annual data, we scale by 365 to approximate daily sum behavior.
    Returns (hdd, cdd) tuple.
    """
    if not temps_celsius or pd.isna(temps_celsius):
        return None, None
    temp_f = float(temps_celsius) * 9/5 + 32
    hdd = max(0, baseline - temp_f) * 365
    cdd = max(0, temp_f - baseline) * 365
    return round(hdd, 1), round(cdd, 1)

def compute_temp_anomaly(temps_celsius, baseline_temps, baseline_period_std):
    """
    Compute temperature anomaly as z-score: (current - baseline_mean) / baseline_std.
    baseline_temps is a list of temperatures from baseline period (e.g., 2000-2005).
    Returns anomaly in standard deviations.
    """
    if not temps_celsius or pd.isna(temps_celsius) or not baseline_temps or baseline_period_std <= 0:
        return None
    baseline_mean = np.mean(baseline_temps)
    anomaly = (float(temps_celsius) - baseline_mean) / baseline_period_std
    return round(anomaly, 3)

# Extract baseline temperatures (2000-2005 for all countries)
baseline_period = (2000, 2005)
baseline_temps_by_country = {}
# Find the actual temperature indicator name (avoid encoding issues with °)
t2m_indicator = None
for ind in df['indicator_name'].unique():
    if 'surface temperature' in ind.lower():
        t2m_indicator = ind
        break
if t2m_indicator is None:
    t2m_indicator = 'Mean surface temperature'  # fallback
for iso3 in COUNTRIES:
    sub = df[(df['iso3']==iso3) & (df['indicator_name']==t2m_indicator)
             & (df['year']>=baseline_period[0]) & (df['year']<=baseline_period[1])]
    temps = sub['value'].dropna().tolist()
    if temps:
        baseline_temps_by_country[iso3] = {
            'temps': temps,
            'mean': np.mean(temps),
            'std': np.std(temps) if len(temps) > 1 else 1.0
        }

# Compute HDD, CDD, and Temperature Anomaly for all countries and years
derived_rows = []
for iso3 in COUNTRIES:
    sub = df[df['iso3']==iso3]
    t2m_rows = sub[sub['indicator_name']==t2m_indicator].copy()

    for _, row in t2m_rows.iterrows():
        temp_c = row['value']
        year = row['year']

        # Compute HDD and CDD
        hdd, cdd = compute_hdd_cdd(temp_c)
        if hdd is not None:
            derived_rows.append({
                'iso3': iso3, 'indicator': 'NASA_HDD',
                'indicator_name': 'Heating Degree Days (HDD)',
                'category': 'Climate', 'unit': 'HDD', 'year': year, 'value': hdd
            })
            derived_rows.append({
                'iso3': iso3, 'indicator': 'NASA_CDD',
                'indicator_name': 'Cooling Degree Days (CDD)',
                'category': 'Climate', 'unit': 'CDD', 'year': year, 'value': cdd
            })

        # Compute Temperature Anomaly
        if iso3 in baseline_temps_by_country:
            baseline_info = baseline_temps_by_country[iso3]
            anom = compute_temp_anomaly(temp_c, baseline_info['temps'], baseline_info['std'])
            if anom is not None:
                derived_rows.append({
                    'iso3': iso3, 'indicator': 'NASA_TANOM',
                    'indicator_name': 'Temperature Anomaly (from 2000-2005 baseline)',
                    'category': 'Climate', 'unit': 'std dev', 'year': year, 'value': anom
                })

# Add derived metrics to main df
if derived_rows:
    derived_df = pd.DataFrame(derived_rows)
    df = pd.concat([df, derived_df], ignore_index=True)
    print(f"  Added {len(derived_rows)} derived climate metrics (HDD, CDD, Temperature Anomaly)")
else:
    print("  Warning: Could not compute derived climate metrics — no NASA T2M data found")

# ── STEP 1: ARIMA/ARIMAX FORECASTS (ADF stationarity + AIC model selection) ──
print("\n[1/5] ARIMA/ARIMAX forecasts...")

def _adf_d(vals):
    """ADF test to select integration order d (0 or 1).
    Uses constant-only regression; p < 0.10 → series is stationary → d=0.
    Falls back to d=1 if the test fails (safe default for economic series).
    """
    if len(vals) < 8:
        return 1
    try:
        maxlag = min(3, (len(vals) - 1) // 3)
        pval = adfuller(vals, maxlag=maxlag, regression='c', autolag=None)[1]
        return 0 if pval < 0.10 else 1
    except Exception:
        return 1

def _fit_arima_aic(vals, d, exog=None):
    """AIC grid search over p∈{0,1,2} × q∈{0,1,2} for a given d.
    Skips (0,d,0) as it is equivalent to a random walk with no structure.
    Returns (best_model, best_order) or (None, (1,d,1)) on total failure.
    """
    best_aic, best_m, best_order = np.inf, None, (1, d, 1)
    for p, q in iproduct(range(3), range(3)):
        if p == 0 and q == 0:
            continue
        try:
            with warnings.catch_warnings():
                warnings.simplefilter('ignore')
                m = (ARIMA(vals, order=(p, d, q), exog=exog).fit()
                     if exog is not None
                     else ARIMA(vals, order=(p, d, q)).fit())
                if m.aic < best_aic:
                    best_aic, best_m, best_order = m.aic, m, (p, d, q)
        except Exception:
            continue
    return best_m, best_order

def _fd_corr(xs, ys):
    """Pearson r on first-differences — measures co-movement after detrending."""
    if len(xs) < 4:
        return 0.0
    dx = [xs[i] - xs[i-1] for i in range(1, len(xs))]
    dy = [ys[i] - ys[i-1] for i in range(1, len(ys))]
    n = len(dx)
    mx, my = sum(dx) / n, sum(dy) / n
    num = sum((a - mx) * (b - my) for a, b in zip(dx, dy))
    den = (sum((a - mx)**2 for a in dx) * sum((b - my)**2 for b in dy)) ** 0.5
    return num / den if den > 0 else 0.0

all_forecasts = {}
for iso3 in COUNTRIES:
    all_forecasts[iso3] = {}
    sub = df[df['iso3'] == iso3]

    # Pre-collect all series keyed by indicator name
    country_series = {}
    for ind_name in sub['indicator_name'].unique():
        rows_s = sub[sub['indicator_name'] == ind_name].sort_values('year')
        if len(rows_s) < 8:
            continue
        country_series[ind_name] = dict(zip(rows_s['year'].tolist(),
                                            rows_s['value'].tolist()))

    n_arimax = 0
    for ind_name, year_val in country_series.items():
        years = sorted(year_val.keys())
        vals  = np.array([year_val[y] for y in years], dtype=float)

        # Stage 1 — ADF test determines d
        d = _adf_d(vals)

        # Stage 2 — search for an ARIMAX exogenous candidate:
        #   · first-difference correlation |r| > 0.65 (genuine co-movement, not spurious)
        #   · ≥ 15 overlapping years (enough data for a stable β estimate)
        #   · fully covers target series years (required for aligned training exog)
        exog_train  = None
        exog_future = None
        arimax_name = None
        best_r      = 0.65  # minimum threshold

        for other_name, other_yv in country_series.items():
            if other_name == ind_name:
                continue
            common = sorted(set(years) & set(other_yv.keys()))
            if len(common) < 15:
                continue
            if not all(y in other_yv for y in years):
                continue  # exog must align exactly with training window
            xs_c = [year_val[y]  for y in common]
            ys_c = [other_yv[y]  for y in common]
            r = abs(_fd_corr(xs_c, ys_c))
            if r <= best_r:
                continue
            # Forecast the exog series with its own plain ARIMA so we have
            # future values to pass to the main ARIMAX forecast.
            exog_vals = np.array([other_yv[y] for y in years], dtype=float)
            d_x = _adf_d(exog_vals)
            exog_m, _ = _fit_arima_aic(exog_vals, d_x)
            if exog_m is None:
                continue
            try:
                with warnings.catch_warnings():
                    warnings.simplefilter('ignore')
                    exog_fc_vals = exog_m.get_forecast(FC_HORIZON).predicted_mean.values
                best_r      = r
                exog_train  = exog_vals
                exog_future = exog_fc_vals
                arimax_name = other_name
            except Exception:
                continue

        # Stage 3 — AIC-selected ARIMA (or ARIMAX if a candidate was found)
        m, order = _fit_arima_aic(vals, d, exog=exog_train)
        if m is None:
            try:
                with warnings.catch_warnings():
                    warnings.simplefilter('ignore')
                    m = ARIMA(vals, order=(1, 1, 1)).fit()
                    order = (1, 1, 1)
            except Exception:
                continue

        # Stage 4 — generate 15-year forecast with 80% CI
        try:
            with warnings.catch_warnings():
                warnings.simplefilter('ignore')
                fc_kw  = {'exog': exog_future} if exog_future is not None else {}
                fco    = m.get_forecast(FC_HORIZON, **fc_kw)
                fc     = fco.predicted_mean.tolist()
                ci     = fco.conf_int(alpha=0.2)
                lo     = (ci[:, 0] if hasattr(ci, 'shape') else ci.iloc[:, 0]).tolist()
                hi     = (ci[:, 1] if hasattr(ci, 'shape') else ci.iloc[:, 1]).tolist()
        except Exception:
            continue

        if arimax_name:
            n_arimax += 1
        all_forecasts[iso3][ind_name] = {
            'years':       years,
            'values':      [_f(v) for v in vals.tolist()],
            'fc_years':    FC_YEARS,
            'fc_values':   [_f(v) for v in fc],
            'fc_lo':       [_f(v) for v in lo],
            'fc_hi':       [_f(v) for v in hi],
            'arima_order': list(order),
            'arimax':      arimax_name,   # None → plain ARIMA
        }

    n_total = len(all_forecasts[iso3])
    print(f"  {iso3}: {n_total} series "
          f"({n_arimax} ARIMAX, {n_total - n_arimax} ARIMA)")

# Save per-country forecast files
for iso3, fc_data in all_forecasts.items():
    path = FC_DIR / f'{iso3}.json'
    with open(path, 'w') as f:
        json.dump(fc_data, f, separators=(',', ':'), cls=_SafeEncoder)
total = sum(len(v) for v in all_forecasts.values())
print(f"  {total} total series → app/static/data/forecasts/")

# ── STEP 2: WINDOWED CORRELATIONS ──
print("\n[2/5] Windowed correlations...")

def first_diff(vals):
    """Year-over-year first differences — removes trend-based spurious correlations."""
    return [vals[i]-vals[i-1] for i in range(1,len(vals))]

def _quick_corr(xs, ys):
    """Pearson r between two equal-length arrays."""
    n = len(xs)
    if n < 3: return 0.0
    mx, my = sum(xs)/n, sum(ys)/n
    num = sum((x-mx)*(y-my) for x,y in zip(xs,ys))
    den = (sum((x-mx)**2 for x in xs)*sum((y-my)**2 for y in ys))**0.5
    return max(-1.0, min(1.0, num/den)) if den > 0 else 0.0

def _compute_stability(dict_a, dict_b, win=None):
    """Rolling-window stability score [0–1].

    Slides a window across the first-differenced residuals for both
    indicators and computes Pearson r in each window.  The score is:

        stability = max(0, 1 - range_of_rolling_r / 2)

    where range_of_rolling_r ∈ [0, 2]:
    • 1.0  → r is identical in every window (structural, reliable)
    • 0.5  → r range ≈ 1 (moderate variability)
    • 0.0  → r flips from −1 to +1 across periods (spurious / regime-dependent)

    Window size adapts: 8 for long series, 5 for shorter ones.
    Returns None only if data is too short for even the smallest window.
    """
    common = sorted(set(dict_a) & set(dict_b))
    n = len(common)
    if win is None:
        win = 5 if n < 12 else 8
    if n < win + 2:
        return None
    xs = [dict_a[y] for y in common]
    ys = [dict_b[y] for y in common]
    rs = [_quick_corr(xs[i - win + 1:i + 1], ys[i - win + 1:i + 1])
          for i in range(win - 1, n)]
    if len(rs) < 3:
        return None
    return round(max(0.0, 1.0 - (max(rs) - min(rs)) / 2.0), 3)

# ── DOMAIN WHITELIST ──
# Only allow correlations between category pairs with a plausible mechanism.
# Format: frozenset({cat_a, cat_b}) — symmetric, same-category pairs always allowed.
_ALLOWED_CAT_PAIRS = {
    # Economy
    frozenset({'Economy','Economy'}),
    frozenset({'Economy','Trade'}),
    frozenset({'Economy','SovereignDebt'}),
    frozenset({'Economy','Energy'}),
    frozenset({'Economy','Governance'}),
    frozenset({'Economy','Demographics'}),
    frozenset({'Economy','Demographic'}),
    frozenset({'Economy','Health'}),
    frozenset({'Economy','Social'}),
    frozenset({'Economy','Education'}),
    frozenset({'Economy','Climate'}),       # temperature → labour productivity, ag output
    frozenset({'Economy','Agriculture'}),
    # SovereignDebt
    frozenset({'SovereignDebt','SovereignDebt'}),
    frozenset({'SovereignDebt','Governance'}),
    frozenset({'SovereignDebt','Economy'}),
    frozenset({'SovereignDebt','Trade'}),   # trade deficits → debt accumulation
    # Energy
    frozenset({'Energy','Economy'}),
    frozenset({'Energy','Environment'}),
    frozenset({'Energy','Climate'}),
    frozenset({'Energy','Energy'}),
    frozenset({'Energy','Trade'}),          # energy is a major traded commodity
    frozenset({'Energy','Social'}),         # energy access → human development
    frozenset({'Energy','Health'}),         # air quality, energy poverty → health
    frozenset({'Energy','Agriculture'}),
    frozenset({'Energy','Governance'}),     # energy policy, resource curse
    # Environment
    frozenset({'Environment','Climate'}),
    frozenset({'Environment','Health'}),
    frozenset({'Environment','Environment'}),
    frozenset({'Environment','Agriculture'}),
    # Climate (NASA T2M + CO2)
    frozenset({'Climate','Climate'}),
    frozenset({'Climate','Health'}),
    frozenset({'Climate','Energy'}),
    frozenset({'Climate','Agriculture'}),
    frozenset({'Climate','Economy'}),       # temperature → GDP, labour productivity
    frozenset({'Climate','Trade'}),         # climate → ag exports, commodity prices
    frozenset({'Climate','Social'}),        # extreme weather → displacement
    frozenset({'Climate','Demographic'}),   # climate migration
    frozenset({'Climate','Demographics'}),
    # Health
    frozenset({'Health','Health'}),
    frozenset({'Health','Social'}),
    frozenset({'Health','Education'}),
    frozenset({'Health','Demographics'}),
    frozenset({'Health','Demographic'}),
    frozenset({'Health','Governance'}),     # health systems quality
    frozenset({'Health','Trade'}),          # pharmaceutical access
    # Social
    frozenset({'Social','Social'}),
    frozenset({'Social','Education'}),
    frozenset({'Social','Demographics'}),
    frozenset({'Social','Demographic'}),
    frozenset({'Social','Governance'}),
    frozenset({'Social','Trade'}),          # trade openness → labour markets
    # Education
    frozenset({'Education','Education'}),
    frozenset({'Education','Demographics'}),
    frozenset({'Education','Demographic'}),
    frozenset({'Education','Governance'}),
    # Trade
    frozenset({'Trade','Trade'}),
    frozenset({'Trade','Economy'}),
    frozenset({'Trade','Health'}),
    frozenset({'Trade','Social'}),
    frozenset({'Trade','Climate'}),
    frozenset({'Trade','Agriculture'}),
    # Governance (FastTrack — democracy, corruption, press freedom)
    frozenset({'Governance','Governance'}),
    frozenset({'Governance','Economy'}),
    frozenset({'Governance','Health'}),
    frozenset({'Governance','Social'}),
    frozenset({'Governance','Education'}),
    frozenset({'Governance','Trade'}),
    frozenset({'Governance','Demographic'}),
    frozenset({'Governance','Demographics'}),
    frozenset({'Governance','SovereignDebt'}),
    frozenset({'Governance','Agriculture'}),
    # Demographics
    frozenset({'Demographics','Demographics'}),
    frozenset({'Demographic','Demographic'}),
    # Agriculture
    frozenset({'Agriculture','Agriculture'}),
    frozenset({'Agriculture','Economy'}),
    frozenset({'Agriculture','Climate'}),
    frozenset({'Agriculture','Environment'}),
    frozenset({'Agriculture','Trade'}),
    frozenset({'Agriculture','Health'}),
    frozenset({'Agriculture','Social'}),
    frozenset({'Agriculture','Demographics'}),
    frozenset({'Agriculture','Demographic'}),
    frozenset({'Agriculture','Energy'}),
    frozenset({'Agriculture','Governance'}),
}

# 3-tier system: 1=always, 2=stricter threshold, 3=blocked
_CAT_PAIR_TIER = {}
for p in _ALLOWED_CAT_PAIRS:
    _CAT_PAIR_TIER[p] = 1  # default all existing to tier 1

# Elevate weak-mechanism pairs to tier 2 (stricter threshold required)
_TIER2_PAIRS = [
    frozenset({'Economy','Education'}),
    frozenset({'Economy','Climate'}),
    frozenset({'Energy','Social'}),
    frozenset({'Energy','Health'}),
    frozenset({'Energy','Governance'}),
    frozenset({'Climate','Social'}),
    frozenset({'Climate','Trade'}),
    frozenset({'Climate','Demographic'}),
    frozenset({'Climate','Demographics'}),
    frozenset({'Governance','Health'}),
    frozenset({'Governance','Agriculture'}),
    frozenset({'Governance','Demographic'}),
    frozenset({'Governance','Demographics'}),
    frozenset({'Health','Trade'}),
    frozenset({'Social','Trade'}),
    frozenset({'Agriculture','Social'}),
    frozenset({'Agriculture','Health'}),
    frozenset({'Agriculture','Demographic'}),
    frozenset({'Agriculture','Demographics'}),
]
for p in _TIER2_PAIRS:
    if p in _CAT_PAIR_TIER:
        _CAT_PAIR_TIER[p] = 2

def _domain_allowed(cat_a, cat_b):
    """Return tier: 1=always, 2=stricter, 0=blocked."""
    if cat_a == cat_b:
        return 1
    pair = frozenset({cat_a, cat_b})
    return _CAT_PAIR_TIER.get(pair, 0)

def _granger_score(xs, ys, lag=1):
    """
    Simple Granger-causality check: does xs[t-lag] improve prediction of ys[t]
    beyond ys[t-lag] alone?
    Returns the F-like improvement ratio (>1 means xs Granger-causes ys).
    Uses OLS residuals: RSS_restricted (AR only) vs RSS_unrestricted (AR + lagged X).
    """
    n = len(ys)
    if n < lag + 4:
        return 1.0
    # Restricted: predict ys[t] from ys[t-1]
    y_t  = np.array(ys[lag:])
    y_l  = np.array(ys[:-lag])
    x_l  = np.array(xs[:-lag])
    # OLS: y_t = a + b*y_l  (restricted)
    def ols_resid(Y, *Xs):
        A = np.column_stack([np.ones(len(Y))] + list(Xs))
        try:
            b = np.linalg.lstsq(A, Y, rcond=None)[0]
            return Y - A @ b
        except Exception:
            return Y
    rss_r = np.sum(ols_resid(y_t, y_l)**2)
    rss_u = np.sum(ols_resid(y_t, y_l, x_l)**2)
    if rss_u < 1e-12:
        return 1.0
    return float(rss_r / rss_u)   # >1 means improvement; we want >1.15

def _partial_corr_ab_given_others(residuals, key_a, key_b, all_keys):
    """
    Partial correlation of A and B after removing shared variance from all other series
    that are correlated with both A and B.
    We regress out the top-3 most correlated 'confounders' from both A and B first.
    """
    common_ab = sorted(set(residuals[key_a]) & set(residuals[key_b]))
    if len(common_ab) < 5:
        return _quick_corr(
            [residuals[key_a][y] for y in common_ab],
            [residuals[key_b][y] for y in common_ab]
        )
    xa = np.array([residuals[key_a][y] for y in common_ab])
    xb = np.array([residuals[key_b][y] for y in common_ab])

    # Find confounders: other series with high correlation to BOTH a and b
    confounders = []
    for k in all_keys:
        if k == key_a or k == key_b: continue
        common_k = sorted(set(residuals.get(k,{}).keys()) & set(common_ab))
        if len(common_k) < 5: continue
        xk_full = {y: residuals[k][y] for y in common_k}
        xk_a = [xk_full[y] for y in common_ab if y in xk_full]
        xk_b = [xk_full[y] for y in common_ab if y in xk_full]
        if len(xk_a) < 5: continue
        r_ka = abs(_quick_corr(xk_a, [xa[i] for i,y in enumerate(common_ab) if y in xk_full]))
        r_kb = abs(_quick_corr(xk_b, [xb[i] for i,y in enumerate(common_ab) if y in xk_full]))
        if r_ka > 0.35 and r_kb > 0.35:
            confounders.append((r_ka + r_kb, k, common_k))

    if not confounders:
        return _quick_corr(xa.tolist(), xb.tolist())

    # Regress out top-3 confounders from xa and xb
    confounders.sort(reverse=True)
    for _, k, cy in confounders[:3]:
        xk = np.array([residuals[k][y] for y in cy])
        # only use years present in both
        idx_a = [i for i, y in enumerate(common_ab) if y in set(cy)]
        if len(idx_a) < 4: continue
        xa_sub = xa[idx_a]; xk_sub = xk[[list(cy).index(common_ab[i]) for i in idx_a] if len(cy)==len(xk) else range(len(xk_sub))]
        # simple version: just subtract projection
        try:
            xk_arr = np.array([residuals[k][y] for y in common_ab if y in residuals[k]])
            if len(xk_arr) == len(xa):
                b = float(np.cov(xk_arr, xa)[0,1] / (np.var(xk_arr) + 1e-12))
                xa = xa - b * xk_arr
                b2 = float(np.cov(xk_arr, xb)[0,1] / (np.var(xk_arr) + 1e-12))
                xb = xb - b2 * xk_arr
        except Exception:
            pass

    return _quick_corr(xa.tolist(), xb.tolist())


# Near-duplicate indicator detection — block correlations between
# indicators that measure essentially the same thing from different sources
_DUP_STEMS = [
    'life expectancy',
    'co2 emissions per capita',
    'carbon footprint per capita',
    'unemployment rate',
    'child mortality',
    'under-5 mortality',
    'population ages',
    'tariff rate',
]

def _sim_indicator(a_low, b_low):
    """Return True if a and b are near-duplicate indicators."""
    for stem in _DUP_STEMS:
        if stem in a_low and stem in b_low:
            return True
    return False


def compute_corr(data_dict, y1, y2, min_obs=5, cat_map=None):
    """
    6-stage robust correlation pipeline:
      1. First-differencing  — removes shared linear trend
      2. Common-factor residual — removes shared global cycle
      3. Fisher-z significance test — removes noise (p < 0.15)
      4. Consistency check — same sign in both halves of the period
      5. Domain whitelist — only category pairs with a plausible mechanism
      6. Granger causality — at least one direction must show predictive power
         (for within-country correlations when cat_map is provided)
    Partial correlation check is applied in the cross-country pipeline separately.
    Each edge also receives a `stability` score [0–1] from _compute_stability().
    """
    # ── Stage 1: first-difference all series ──
    diff_map = {}
    for key, pairs in data_dict.items():
        da = {yr: v for yr, v in pairs if y1 <= yr <= y2}
        yrs = sorted(da.keys())
        if len(yrs) < max(4, min_obs): continue
        diffs = first_diff([da[y] for y in yrs])
        diff_map[key] = dict(zip(yrs[1:], diffs))

    if len(diff_map) < 2:
        return []

    # ── Stage 2: common factor (median across all diffs per year) ──
    all_years_set = set()
    for d in diff_map.values(): all_years_set |= set(d.keys())
    factor = {}
    for yr in sorted(all_years_set):
        vals = [diff_map[k][yr] for k in diff_map if yr in diff_map[k]]
        if len(vals) >= 3:
            factor[yr] = float(np.median(vals))

    # ── Stage 3: subtract common factor → residuals ──
    residuals = {}
    for key, diffs in diff_map.items():
        cy = sorted(set(diffs.keys()) & set(factor.keys()))
        if len(cy) < max(3, min_obs - 2): continue
        xf = np.array([factor[y] for y in cy])
        yv = np.array([diffs[y] for y in cy])
        std_xf = float(np.std(xf))
        if std_xf > 1e-9:
            beta = float(np.cov(xf, yv)[0, 1] / np.var(xf))
            resid = yv - beta * xf
        else:
            resid = yv
        residuals[key] = dict(zip(cy, resid.tolist()))

    keys = list(residuals.keys())
    edges = []
    eid = 0
    for i in range(len(keys)):
        for j in range(i + 1, len(keys)):
            a, b = keys[i], keys[j]

            # ── Stage 5 (early): domain whitelist (3-tier) ──
            if cat_map is not None:
                cat_a = cat_map.get(a, 'Other')
                cat_b = cat_map.get(b, 'Other')
                tier = _domain_allowed(cat_a, cat_b)
                if tier == 0:
                    continue
                # Flag same-source + same-category pairs (except WB which is diverse)
                # These often produce spurious correlations from shared development
                # trends (e.g. WHO mortality vs WHO alcohol, ILO NEET vs ILO unemployment)
                # ── Quality severity bitfield (q):
                #   bit 0 (q & 1) = near-duplicate name stems    → hidden by Balanced+Strict
                #   bit 1 (q & 2) = same-source same-category    → hidden by Strict only
                # Final q ∈ {0,1,2,3}; 0 = clean.
                #
                # "Tight-domain" sources are those whose indicators are
                # mathematically derived from common underlying data (e.g.
                # Damodaran's CRP ↔ ERP ↔ Default Spread; Heritage's freedom
                # sub-scores all co-vary by construction). Same-source pairs
                # in these sources are treated as near-duplicates (bit 0) so
                # Balanced mode hides them by default.
                _TIGHT_DOMAIN = {'Damodaran', 'Fraser', 'DoingBusiness'}
                src_a = IND_SOURCE.get(a, 'WB')
                src_b = IND_SOURCE.get(b, 'WB')
                _q_bits = 0
                if cat_a == cat_b and src_a == src_b and src_a not in ('WB',):
                    if src_a in _TIGHT_DOMAIN:
                        _q_bits |= 1  # bit 0 — Balanced filter
                    else:
                        _q_bits |= 2  # bit 1 — Strict only
                # Near-duplicate indicators (same metric, often cross-source)
                al, bl = a.lower(), b.lower()
                if _sim_indicator(al, bl):
                    _q_bits |= 1  # bit 0

            common = sorted(set(residuals[a]) & set(residuals[b]))
            n = len(common)
            if n < max(4, min_obs - 1): continue
            xs = [residuals[a][y] for y in common]
            ys = [residuals[b][y] for y in common]
            r = _quick_corr(xs, ys)

            # ── Stage 3b: Fisher-z significance ──
            ar = abs(r)
            if ar >= 1.0 - 1e-9: continue
            # Tier 2 pairs need |r| >= 0.45 (vs default ~0.30)
            if cat_map is not None and tier == 2 and ar < 0.45: continue
            z = 0.5 * np.log((1 + ar) / (1 - ar))
            is_cross = (cat_map is not None and cat_a != cat_b)
            z_crit = 1.04 if is_cross else 1.44
            z_thr = z_crit / max(1, (n - 3)) ** 0.5
            if z < z_thr: continue

            # ── Stage 4: consistency — same sign in both halves ──
            mid = n // 2
            if mid >= 3 and n - mid >= 3:
                r1 = _quick_corr(xs[:mid], ys[:mid])
                r2 = _quick_corr(xs[mid:], ys[mid:])
                if r1 * r2 < 0 and min(abs(r1), abs(r2)) > 0.25:
                    continue

            # ── Stage 6: Granger — at least one direction predictive ──
            if cat_map is not None and n >= 8:
                raw_xs = [diff_map[a].get(y, xs[i]) for i, y in enumerate(common)]
                raw_ys = [diff_map[b].get(y, ys[i]) for i, y in enumerate(common)]
                g_ab = _granger_score(raw_xs, raw_ys, lag=1)
                g_ba = _granger_score(raw_ys, raw_xs, lag=1)
                # Tier 2: stricter Granger threshold (1.15 vs 1.05)
                g_thr = 1.15 if tier == 2 else 1.05
                if g_ab < g_thr and g_ba < g_thr:
                    continue

            # ── Stage 7: Partial correlation — remove development confounders ──
            if cat_map is not None and is_cross and n >= 6:
                pr = _partial_corr_ab_given_others(residuals, a, b, keys)
                if pr is not None and abs(pr) < 0.25:
                    continue  # only correlated through shared confounders
                if pr is not None:
                    r = pr  # use partial r as the reported correlation

            # Stability: rolling 8-yr window on first-differenced data
            stab = _compute_stability(diff_map[a], diff_map[b])
            edge = {'id': eid, 'source': a, 'target': b,
                    'correlation': round(r, 3), 'strength': round(ar, 3),
                    'sign': 'pos' if r > 0 else 'neg',
                    'stability': stab}
            if _q_bits:
                edge['q'] = _q_bits  # severity bitfield — see Quality block above
            edges.append(edge)
            eid += 1
    return edges

corr_count = 0
for iso3 in COUNTRIES:
    sub = df[df['iso3']==iso3]
    cat_map_c = dict(zip(sub['indicator_name'], sub['category']))
    # Build data dict with hist + forecast merged
    data_dict = {}
    for ind_name in sub['indicator_name'].unique():
        rows = sub[sub['indicator_name']==ind_name].sort_values('year')
        hist_pairs = list(zip(rows['year'].tolist(), rows['value'].tolist()))
        fc = all_forecasts[iso3].get(ind_name)
        fc_pairs  = list(zip(fc['fc_years'], fc['fc_values'])) if fc else []
        data_dict[ind_name] = hist_pairs + fc_pairs

    for (y1,y2) in YEAR_WINDOWS:
        is_proj = y2 > HIST_END
        edges = compute_corr(data_dict, y1, y2, cat_map=cat_map_c)
        for e in edges: e['projected'] = is_proj
        out = {
            'iso3': iso3, 'year_start': y1, 'year_end': y2,
            'is_forecast': is_proj, 'edges': edges
        }
        fname = CORR_DIR / f'{iso3}_{y1}_{y2}.json'
        with open(fname,'w') as f: json.dump(out, f, separators=(',',':'), cls=_SafeEncoder)
        corr_count += len(edges)

# Global correlations
data_dict_g = {}
cat_map = dict(zip(df['indicator_name'], df['category']))
for ind_name in df['indicator_name'].unique():
    rows = df[df['indicator_name']==ind_name].sort_values('year')
    data_dict_g[ind_name] = list(zip(rows['year'].tolist(), rows['value'].tolist()))
for (y1,y2) in YEAR_WINDOWS:
    edges = compute_corr(data_dict_g, y1, y2, cat_map=cat_map)
    out = {'iso3':'GLOBAL','year_start':y1,'year_end':y2,'is_forecast':y2>HIST_END,'edges':edges}
    with open(CORR_DIR/f'GLOBAL_{y1}_{y2}.json','w') as f: json.dump(out, f, separators=(',',':'), cls=_SafeEncoder)

print(f"  {corr_count} correlation entries → app/static/data/correlations/")

# ── STEP 3: CROSS-COUNTRY CORRELATIONS (rigorous pipeline) ──
print("\n[3/5] Cross-country correlations...")
cross_edges = []
eid = 0

# Pre-compute first-differenced + common-factor-removed residuals per country
_xc_resid = {}  # (iso3, ind) -> {year: residual_value}
for iso3 in COUNTRIES:
    sub = df[df['iso3']==iso3]
    diff_series = {}
    for ind in sub['indicator_name'].unique():
        rows = sub[sub['indicator_name']==ind].sort_values('year')
        vals = dict(zip(rows['year'], rows['value']))
        yrs = sorted(vals.keys())
        if len(yrs) < 6: continue
        diffs = {yrs[i]: vals[yrs[i]] - vals[yrs[i-1]] for i in range(1, len(yrs))}
        diff_series[ind] = diffs
    # Common factor per country (median first-diff across all indicators per year)
    all_yrs = set()
    for d in diff_series.values(): all_yrs |= set(d.keys())
    cf = {}
    for yr in sorted(all_yrs):
        vv = [diff_series[k][yr] for k in diff_series if yr in diff_series[k]]
        if len(vv) >= 3: cf[yr] = float(np.median(vv))
    # Subtract common factor
    for ind, diffs in diff_series.items():
        cy = sorted(set(diffs.keys()) & set(cf.keys()))
        if len(cy) < 4: continue
        xf = np.array([cf[y] for y in cy])
        yv = np.array([diffs[y] for y in cy])
        std_xf = float(np.std(xf))
        if std_xf > 1e-9:
            beta = float(np.cov(xf, yv)[0, 1] / np.var(xf))
            resid = yv - beta * xf
        else:
            resid = yv
        _xc_resid[(iso3, ind)] = dict(zip(cy, resid.tolist()))

for iso_a, iso_b in combinations(COUNTRIES, 2):
    sub_a = df[df['iso3']==iso_a]
    sub_b = df[df['iso3']==iso_b]
    for ind_a in sub_a['indicator_name'].unique():
        cat_a = cat_map.get(ind_a, 'Other')
        ra = _xc_resid.get((iso_a, ind_a))
        if ra is None: continue
        for ind_b in sub_b['indicator_name'].unique():
            cat_b = cat_map.get(ind_b, 'Other')
            # Cross-country: require same category (different categories across
            # borders have no plausible causal mechanism)
            if cat_a != cat_b: continue
            # Skip same-indicator (global cycles create spurious r) except Climate
            if ind_a == ind_b and cat_a != 'Climate': continue
            # Flag different indicators from the SAME source in the same category
            # across countries — these share global development trends.
            # See within-country block for q bitfield semantics (bit 0 = dup, bit 1 = ssc).
            _TIGHT_DOMAIN_XC = {'Damodaran', 'Fraser', 'DoingBusiness'}
            src_a = IND_SOURCE.get(ind_a, 'WB')
            src_b = IND_SOURCE.get(ind_b, 'WB')
            _xc_q = 0
            if ind_a != ind_b and src_a == src_b and src_a not in ('WB', 'NASA'):
                if src_a in _TIGHT_DOMAIN_XC:
                    _xc_q |= 1  # bit 0 — Balanced filter (tight-domain source)
                else:
                    _xc_q |= 2  # bit 1 — Strict only
            if _sim_indicator(ind_a.lower(), ind_b.lower()):
                _xc_q |= 1  # bit 0 — near-duplicate name stem
            rb = _xc_resid.get((iso_b, ind_b))
            if rb is None: continue
            common = sorted(set(ra) & set(rb))
            n = len(common)
            if n < 8: continue
            xs = [ra[y] for y in common]
            ys = [rb[y] for y in common]
            r = _quick_corr(xs, ys)
            ar = abs(r)
            if ar < 0.55 or ar >= 1.0 - 1e-9: continue  # raised threshold

            # Consistency check: same sign in both halves
            mid = n // 2
            if mid >= 3 and n - mid >= 3:
                r1 = _quick_corr(xs[:mid], ys[:mid])
                r2 = _quick_corr(xs[mid:], ys[mid:])
                if r1 * r2 < 0 and min(abs(r1), abs(r2)) > 0.25:
                    continue

            # Granger: at least one direction predictive
            if n >= 8:
                g_ab = _granger_score(xs, ys, lag=1)
                g_ba = _granger_score(ys, xs, lag=1)
                if g_ab < 1.10 and g_ba < 1.10:
                    continue

            # Classify arc type
            cats = {cat_a, cat_b}
            if 'Climate' in cats: arc = 'climate_economic'
            elif 'Agriculture' in cats: arc = 'agriculture'
            elif 'Governance' in cats: arc = 'governance'
            elif 'Energy' in cats: arc = 'energy'
            elif 'Social' in cats or 'Health' in cats or 'Education' in cats: arc = 'social_social'
            elif cat_a == cat_b == 'Economy': arc = 'economic_economic'
            else: arc = 'social_social'

            xc_edge = {
                'id': eid, 'country_a': iso_a, 'country_b': iso_b,
                'indicator_a': ind_a, 'indicator_b': ind_b,
                'cat_a': cat_a, 'cat_b': cat_b,
                'correlation': round(float(r), 3), 'strength': round(ar, 3),
                'sign': 'pos' if r > 0 else 'neg', 'arc_type': arc
            }
            if _xc_q:
                xc_edge['q'] = _xc_q  # severity bitfield (see within-country block)
            cross_edges.append(xc_edge)
            eid += 1

# Filter: top 6 per country-pair per arc_type
grp = defaultdict(list)
for e in cross_edges: grp[(e['country_a'], e['country_b'], e['arc_type'])].append(e)
cross_filtered = []
for k, edges in grp.items():
    cross_filtered.extend(sorted(edges, key=lambda x: -x['strength'])[:6])
cross_filtered.sort(key=lambda x: -x['strength'])

with open(OUT_BASE/'cross_country.json','w') as f:
    json.dump(cross_filtered, f, separators=(',',':'), cls=_SafeEncoder)
print(f"  {len(cross_filtered)} cross-country edges → app/static/data/cross_country.json")

# ── STEP 4: CITY DATA + RISK SIGNALS ──
print("\n[4/5] City data and risk signals...")
cities = {}
for _,row in city_df[['iso3','city_name','lat','lon']].drop_duplicates().iterrows():
    cities[row['city_name']] = {'iso3':row['iso3'],'lat':float(row['lat']),'lon':float(row['lon'])}

city_cross = []
for ca, cb in combinations(list(cities.keys()),2):
    if cities[ca]['iso3']==cities[cb]['iso3']: continue
    for param in ['NASA_T2M','NASA_PRECTOTCORR','NASA_RH2M']:
        sub = city_df[city_df['param']==param]
        sa  = sub[sub['city_name']==ca].set_index('year')['value']
        sb  = sub[sub['city_name']==cb].set_index('year')['value']
        common = sa.index.intersection(sb.index)
        if len(common)<6: continue
        r = sa[common].corr(sb[common])
        if np.isnan(r) or abs(r)<0.25: continue
        city_cross.append({'city_a':ca,'iso3_a':cities[ca]['iso3'],
                           'city_b':cb,'iso3_b':cities[cb]['iso3'],
                           'param':param.replace('NASA_',''),'correlation':round(float(r),3),
                           'strength':abs(round(float(r),3)),'sign':'pos' if r>0 else 'neg'})

# Within-country city correlations (intercity view)
city_intra = []
for iso3 in COUNTRIES:
    iso_cities = sorted([c for c in cities if cities[c]['iso3']==iso3])
    for ca, cb in combinations(iso_cities, 2):
        for param in ['NASA_T2M','NASA_PRECTOTCORR','NASA_RH2M']:
            sub = city_df[city_df['param']==param]
            sa  = sub[sub['city_name']==ca].set_index('year')['value']
            sb  = sub[sub['city_name']==cb].set_index('year')['value']
            common = sa.index.intersection(sb.index)
            if len(common)<5: continue
            r = sa[common].corr(sb[common])
            if np.isnan(r) or abs(r)<0.20: continue
            city_intra.append({'iso3':iso3,'city_a':ca,'city_b':cb,
                               'param':param.replace('NASA_',''),
                               'correlation':round(float(r),3),
                               'strength':abs(round(float(r),3)),'sign':'pos' if r>0 else 'neg'})
print(f"  City intra: {len(city_intra)} within-country city edges")

THEME_INDICATORS = {
    'Financial':     ['GDP growth rate (annual %)','Inflation, consumer prices (annual %)','Official exchange rate (LCU per USD)'],
    'Environmental': ['CO2 emissions per capita (metric tons)','Energy use per capita (kg oil equiv)'],
    'Social':        ['GINI index','Human Development Index','Life expectancy at birth (years)','Unemployment rate (%)'],
    'Energy':        ['Energy use per capita (kg oil equiv)','Access to electricity (% of population)','Renewable energy share in total final energy consumption (%)'],
    'Minerals':      [],
    'SovereignDebt': ['General govt gross debt (% of GDP)'],
    'Geopolitical':  ['Democracy Index (EIU)','Corruption Perception Index','Political Stability Index'],
    'Health':        ['Life expectancy at birth (years)'],
    'Demographic':   ['Total population'],
    'Agriculture':   ['Crop production index (2014-2016=100)','Food production index (2014-2016=100)','Arable land (% of land area)','Prevalence of undernourishment (% of population)'],
}

risk_signals = {}
for iso3 in COUNTRIES:
    risk_signals[iso3] = {}
    sub = df[df['iso3']==iso3]
    for theme, indicators in THEME_INDICATORS.items():
        sigs = []
        for ind in indicators:
            rows = sub[sub['indicator_name']==ind].sort_values('year')
            if len(rows)<3: continue
            recent = rows.tail(5)
            latest = float(recent['value'].iloc[-1])
            trend  = float(recent['value'].diff().mean())
            direction = 'up' if trend>0 else 'down' if trend<0 else 'flat'
            danger = False
            if 'Inflation' in ind and latest>6: danger=True
            if 'GINI' in ind and direction=='up': danger=True
            if 'CO2' in ind and direction=='up':  danger=True
            if 'debt' in ind.lower() and latest>90: danger=True
            if 'Life expectancy' in ind and direction=='down': danger=True
            sigs.append({'indicator':ind,'latest':round(latest,3),'trend':round(trend,4),'direction':direction,'danger':danger})
        risk_signals[iso3][theme] = sigs

# ── STEP 5: META PAYLOAD (small, always loaded) ──
print("\n[5/5] Building meta payload...")

node_meta = {}
for iso3 in COUNTRIES:
    sub = df[df['iso3']==iso3]
    cat_map_c = dict(zip(sub['indicator_name'], sub['category']))
    # Compute degree from full-range correlations
    edges = compute_corr({ind: list(zip(rows['year'],rows['value'])) for ind,rows in
                          {i: sub[sub['indicator_name']==i].sort_values('year') for i in sub['indicator_name'].unique()}.items()},
                         HIST_START, HIST_END, cat_map=cat_map_c)
    deg = defaultdict(int)
    for e in edges: deg[e['source']]+=1; deg[e['target']]+=1
    max_d = max(deg.values(),default=1)
    node_meta[iso3] = [{'id':n,'category':cat_map_c.get(n,'Other'),
                         'source':IND_SOURCE.get(n,'WB'),
                         'degree':deg[n],'centrality':round(deg[n]/max_d,3) if deg[n]>0 else 0}
                        for n in cat_map_c if deg[n]>0 or cat_map_c.get(n)=='Climate']

meta = {
    'countries':    COUNTRIES,
    'indicator_sources': IND_SOURCE,   # indicator_name → source key
    'hist_range':   [HIST_START, HIST_END],
    'fc_range':     [HIST_END+1, HIST_END+FC_HORIZON],
    'fc_years':     FC_YEARS,
    'year_windows': YEAR_WINDOWS,
    'cities':       cities,
    'city_cross':   city_cross,
    'city_intra':   city_intra,
    'risk_signals': risk_signals,
    'node_meta':    node_meta,
    'country_meta': {
        'USA':{'name':'United States','lat':37.09, 'lon':-95.71, 'color':'#5b8dd9'},
        'KEN':{'name':'Kenya',        'lat':-0.02, 'lon': 37.91, 'color':'#3eb8a8'},
        'BRA':{'name':'Brazil',       'lat':-14.24,'lon':-51.93, 'color':'#4daa6a'},
        'DEU':{'name':'Germany',      'lat': 51.17,'lon': 10.45, 'color':'#d4a843'},
        'CHN':{'name':'China',        'lat': 35.86,'lon':104.19, 'color':'#d45c52'},
        'RUS':{'name':'Russia',       'lat': 61.52,'lon': 105.32,'color':'#8f74c8'},
        'SGP':{'name':'Singapore',    'lat':  1.35,'lon': 103.82,'color':'#3dbec8'},
        'IND':{'name':'India',        'lat': 20.59,'lon':  78.96,'color':'#c8943a'},
        'JPN':{'name':'Japan',        'lat': 36.20,'lon': 138.25,'color':'#c87090'},
        'IDN':{'name':'Indonesia',    'lat':-0.79, 'lon': 113.92,'color':'#78b84e'},
    }
}

with open(OUT_BASE/'meta.json','w') as f: json.dump(meta, f, separators=(',',':'), cls=_SafeEncoder)
print(f"  Meta payload → app/static/data/meta.json ({os.path.getsize(OUT_BASE/'meta.json')//1024}KB)")

# Summary
print("\n" + "="*55)
print("BUILD COMPLETE")
total_kb = sum(os.path.getsize(OUT_BASE/f) for f in os.listdir(OUT_BASE) if f.endswith('.json')) // 1024
corr_kb  = sum(os.path.getsize(CORR_DIR/f) for f in os.listdir(CORR_DIR)) // 1024
fc_kb    = sum(os.path.getsize(FC_DIR/f) for f in os.listdir(FC_DIR)) // 1024
print(f"  meta.json:     {os.path.getsize(OUT_BASE/'meta.json')//1024}KB")
print(f"  correlations/: {corr_kb}KB ({len(list(CORR_DIR.iterdir()))} files)")
print(f"  forecasts/:    {fc_kb}KB ({len(list(FC_DIR.iterdir()))} files)")
print(f"  cross_country: {os.path.getsize(OUT_BASE/'cross_country.json')//1024}KB")
print(f"  Total:         {total_kb + corr_kb + fc_kb}KB")
