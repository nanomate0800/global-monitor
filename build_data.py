"""
build_data.py
Pre-computes all static data files for the Global Monitor app.
Run this after any database update: python3 build_data.py
Outputs land in app/static/data/
"""
import sys, json, sqlite3, numpy as np, pandas as pd, warnings, os
from itertools import combinations
from collections import defaultdict
from pathlib import Path
if hasattr(sys.stdout, 'reconfigure'):
    sys.stdout.reconfigure(encoding='utf-8')
warnings.filterwarnings('ignore')
from statsmodels.tsa.arima.model import ARIMA

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
HIST_END    = 2023
FC_HORIZON  = 15
FC_YEARS    = list(range(HIST_END+1, HIST_END+FC_HORIZON+1))
YEAR_WINDOWS = [
    (2000,2010),(2005,2015),(2010,2020),(2015,2023),
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

# ── STEP 1: ARIMA FORECASTS ──
print("\n[1/5] ARIMA forecasts...")
all_forecasts = {}
for iso3 in COUNTRIES:
    all_forecasts[iso3] = {}
    sub = df[df['iso3']==iso3]
    for ind_name in sub['indicator_name'].unique():
        rows = sub[sub['indicator_name']==ind_name].sort_values('year')
        if len(rows) < 8: continue
        vals = rows['value'].values.astype(float)
        years = rows['year'].tolist()
        for order in [(1,1,1),(0,1,1),(0,1,0)]:
            try:
                with warnings.catch_warnings():
                    warnings.simplefilter('ignore')
                    m   = ARIMA(vals, order=order).fit()
                    fco = m.get_forecast(FC_HORIZON)
                    fc  = fco.predicted_mean.tolist()
                    ci  = fco.conf_int(alpha=0.2)
                    lo  = ci[:,0].tolist() if hasattr(ci,'shape') else ci.iloc[:,0].tolist()
                    hi  = ci[:,1].tolist() if hasattr(ci,'shape') else ci.iloc[:,1].tolist()
                    all_forecasts[iso3][ind_name] = {
                        'years':  years,
                        'values': [round(v,4) for v in vals.tolist()],
                        'fc_years':  FC_YEARS,
                        'fc_values': [round(v,4) for v in fc],
                        'fc_lo':     [round(v,4) for v in lo],
                        'fc_hi':     [round(v,4) for v in hi],
                    }
                    break
            except: continue

# Save per-country forecast files
for iso3, fc_data in all_forecasts.items():
    path = FC_DIR / f'{iso3}.json'
    with open(path,'w') as f: json.dump(fc_data, f, separators=(',',':'))
total = sum(len(v) for v in all_forecasts.values())
print(f"  {total} series → app/static/data/forecasts/")

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

def compute_corr(data_dict, y1, y2, min_obs=5):
    """
    Robust correlation pipeline:
      1. First-differencing  — removes shared linear trend
      2. Common-factor residual — removes shared global cycle (median across all series)
      3. Fisher z significance test — removes noise correlations (p < 0.15)
      4. Consistency check — same sign in both halves of the period
    """
    # ── Step 1: first-difference all series ──
    diff_map = {}
    for key, pairs in data_dict.items():
        da = {yr: v for yr, v in pairs if y1 <= yr <= y2}
        yrs = sorted(da.keys())
        if len(yrs) < max(4, min_obs): continue
        diffs = first_diff([da[y] for y in yrs])
        diff_map[key] = dict(zip(yrs[1:], diffs))

    if len(diff_map) < 2:
        return []

    # ── Step 2: compute common factor (median across all series per year) ──
    all_years_set = set()
    for d in diff_map.values(): all_years_set |= set(d.keys())
    factor = {}
    for yr in sorted(all_years_set):
        vals = [diff_map[k][yr] for k in diff_map if yr in diff_map[k]]
        if len(vals) >= 3:
            factor[yr] = float(np.median(vals))

    # ── Step 3: subtract common factor to get residuals ──
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
            common = sorted(set(residuals[a]) & set(residuals[b]))
            n = len(common)
            if n < max(4, min_obs - 1): continue
            xs = [residuals[a][y] for y in common]
            ys = [residuals[b][y] for y in common]
            r = _quick_corr(xs, ys)

            # ── Step 4: Fisher z significance (no scipy) ──
            ar = abs(r)
            if ar >= 1.0 - 1e-9: continue
            z = 0.5 * np.log((1 + ar) / (1 - ar))
            z_thr = 1.44 / max(1, (n - 3)) ** 0.5  # ≈ 15% two-tailed
            if z < z_thr: continue

            # ── Step 5: consistency — same sign in both halves ──
            mid = n // 2
            if mid >= 3 and n - mid >= 3:
                r1 = _quick_corr(xs[:mid], ys[:mid])
                r2 = _quick_corr(xs[mid:], ys[mid:])
                # Reject only if halves clearly contradict each other
                if r1 * r2 < 0 and min(abs(r1), abs(r2)) > 0.25:
                    continue

            edges.append({'id': eid, 'source': a, 'target': b,
                          'correlation': round(r, 3), 'strength': round(ar, 3),
                          'sign': 'pos' if r > 0 else 'neg'})
            eid += 1
    return edges

corr_count = 0
for iso3 in COUNTRIES:
    sub = df[df['iso3']==iso3]
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
        edges = compute_corr(data_dict, y1, y2)
        for e in edges: e['projected'] = is_proj
        out = {
            'iso3': iso3, 'year_start': y1, 'year_end': y2,
            'is_forecast': is_proj, 'edges': edges
        }
        fname = CORR_DIR / f'{iso3}_{y1}_{y2}.json'
        with open(fname,'w') as f: json.dump(out, f, separators=(',',':'))
        corr_count += len(edges)

# Global correlations
data_dict_g = {}
cat_map = dict(zip(df['indicator_name'], df['category']))
for ind_name in df['indicator_name'].unique():
    rows = df[df['indicator_name']==ind_name].sort_values('year')
    data_dict_g[ind_name] = list(zip(rows['year'].tolist(), rows['value'].tolist()))
for (y1,y2) in YEAR_WINDOWS:
    edges = compute_corr(data_dict_g, y1, y2)
    out = {'iso3':'GLOBAL','year_start':y1,'year_end':y2,'is_forecast':y2>HIST_END,'edges':edges}
    with open(CORR_DIR/f'GLOBAL_{y1}_{y2}.json','w') as f: json.dump(out,f,separators=(',',':'))

print(f"  {corr_count} correlation entries → app/static/data/correlations/")

# ── STEP 3: CROSS-COUNTRY CORRELATIONS ──
print("\n[3/5] Cross-country correlations...")
cross_edges = []
eid = 0
for iso_a, iso_b in combinations(COUNTRIES,2):
    sub_a = df[df['iso3']==iso_a]
    sub_b = df[df['iso3']==iso_b]
    for ind_a in sub_a['indicator_name'].unique():
        for ind_b in sub_b['indicator_name'].unique():
            if ind_a==ind_b: continue
            sa = sub_a[sub_a['indicator_name']==ind_a].set_index('year')['value']
            sb = sub_b[sub_b['indicator_name']==ind_b].set_index('year')['value']
            common = sa.index.intersection(sb.index)
            if len(common)<8: continue
            # First-difference both series to remove shared trends
            r = sa[common].diff().dropna().corr(sb[common].diff().dropna())
            if np.isnan(r) or abs(r)<0.55: continue  # lower bar since diff reduces spurious high-r
            cat_a = cat_map.get(ind_a,'Other')
            cat_b = cat_map.get(ind_b,'Other')
            arc   = 'climate_economic' if 'Climate' in [cat_a,cat_b] else \
                    'economic_economic' if cat_a==cat_b=='Economy' else 'social_social'
            cross_edges.append({
                'id':eid,'country_a':iso_a,'country_b':iso_b,
                'indicator_a':ind_a,'indicator_b':ind_b,
                'correlation':round(float(r),3),'strength':abs(round(float(r),3)),
                'sign':'pos' if r>0 else 'neg','arc_type':arc
            })
            eid+=1

# Filter: top 4 per country-pair per arc_type
grp = defaultdict(list)
for e in cross_edges: grp[(e['country_a'],e['country_b'],e['arc_type'])].append(e)
cross_filtered = []
for k,edges in grp.items():
    cross_filtered.extend(sorted(edges,key=lambda x:-x['strength'])[:4])
cross_filtered.sort(key=lambda x:-x['strength'])

with open(OUT_BASE/'cross_country.json','w') as f:
    json.dump(cross_filtered, f, separators=(',',':'))
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
    'Energy':        ['Energy use per capita (kg oil equiv)'],
    'Minerals':      [],
    'SovereignDebt': ['General govt gross debt (% of GDP)'],
    'Geopolitical':  [],
    'Health':        ['Life expectancy at birth (years)'],
    'Demographic':   ['Total population'],
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
                         HIST_START, HIST_END)
    deg = defaultdict(int)
    for e in edges: deg[e['source']]+=1; deg[e['target']]+=1
    max_d = max(deg.values(),default=1)
    node_meta[iso3] = [{'id':n,'category':cat_map_c.get(n,'Other'),
                         'degree':deg[n],'centrality':round(deg[n]/max_d,3)}
                        for n in cat_map_c if deg[n]>0]

meta = {
    'countries':    COUNTRIES,
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

with open(OUT_BASE/'meta.json','w') as f: json.dump(meta, f, separators=(',',':'))
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
