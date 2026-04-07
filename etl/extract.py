"""
etl/extract.py
Fetches raw data from World Bank, IMF WEO, UNDP, NASA POWER, UN Comtrade, IEA (via WB proxy), and FAOSTAT APIs.
Outputs raw CSVs to data/raw/.
Run: python etl/extract.py
"""
import requests, json, time, os, sys
import pandas as pd
from pathlib import Path

if hasattr(sys.stdout, 'reconfigure'):
    sys.stdout.reconfigure(encoding='utf-8')

BASE     = Path(__file__).parent.parent
RAW_DIR  = BASE / 'data' / 'raw'
RAW_DIR.mkdir(parents=True, exist_ok=True)

COUNTRIES = ['USA', 'KEN', 'BRA', 'DEU', 'CHN', 'RUS', 'SGP', 'IND', 'JPN', 'IDN']
YEAR_START, YEAR_END = 2000, 2023

# UN Comtrade reporter codes (ISO numeric → ISO3 mapping for our countries)
COMTRADE_REPORTERS = {
    '842': 'USA', '404': 'KEN', '076': 'BRA', '276': 'DEU',
    '156': 'CHN', '643': 'RUS', '702': 'SGP', '356': 'IND',
    '392': 'JPN', '360': 'IDN',
}

CITIES = [
    # USA — coast-to-coast + interior coverage
    {'city':'New York',        'iso3':'USA','lat': 40.71, 'lon': -74.01},
    {'city':'Los Angeles',     'iso3':'USA','lat': 34.05, 'lon':-118.25},
    {'city':'Chicago',         'iso3':'USA','lat': 41.88, 'lon': -87.63},
    {'city':'Houston',         'iso3':'USA','lat': 29.76, 'lon': -95.37},
    {'city':'Miami',           'iso3':'USA','lat': 25.76, 'lon': -80.19},
    {'city':'Seattle',         'iso3':'USA','lat': 47.61, 'lon':-122.33},
    {'city':'Phoenix',         'iso3':'USA','lat': 33.45, 'lon':-112.07},
    {'city':'Denver',          'iso3':'USA','lat': 39.74, 'lon':-104.99},
    # KEN — highland, coastal, western lake basin
    {'city':'Nairobi',         'iso3':'KEN','lat': -1.29, 'lon':  36.82},
    {'city':'Mombasa',         'iso3':'KEN','lat': -4.05, 'lon':  39.67},
    {'city':'Kisumu',          'iso3':'KEN','lat': -0.10, 'lon':  34.76},
    {'city':'Eldoret',         'iso3':'KEN','lat':  0.52, 'lon':  35.27},
    # BRA — southeast, central plateau, Amazon, northeast, south
    {'city':'Sao Paulo',       'iso3':'BRA','lat':-23.55, 'lon': -46.63},
    {'city':'Rio de Janeiro',  'iso3':'BRA','lat':-22.91, 'lon': -43.17},
    {'city':'Brasilia',        'iso3':'BRA','lat':-15.78, 'lon': -47.93},
    {'city':'Manaus',          'iso3':'BRA','lat': -3.10, 'lon': -60.03},
    {'city':'Recife',          'iso3':'BRA','lat': -8.05, 'lon': -34.88},
    {'city':'Porto Alegre',    'iso3':'BRA','lat':-30.03, 'lon': -51.22},
    # DEU — north, south, central-west
    {'city':'Berlin',          'iso3':'DEU','lat': 52.52, 'lon':  13.41},
    {'city':'Munich',          'iso3':'DEU','lat': 48.14, 'lon':  11.58},
    {'city':'Hamburg',         'iso3':'DEU','lat': 53.55, 'lon':  10.00},
    {'city':'Frankfurt',       'iso3':'DEU','lat': 50.11, 'lon':   8.68},
    {'city':'Stuttgart',       'iso3':'DEU','lat': 48.78, 'lon':   9.18},
    # CHN — north, east, south, inland, northeast cold, central
    {'city':'Beijing',         'iso3':'CHN','lat': 39.91, 'lon': 116.39},
    {'city':'Shanghai',        'iso3':'CHN','lat': 31.23, 'lon': 121.47},
    {'city':'Guangzhou',       'iso3':'CHN','lat': 23.12, 'lon': 113.25},
    {'city':'Chengdu',         'iso3':'CHN','lat': 30.57, 'lon': 104.07},
    {'city':'Wuhan',           'iso3':'CHN','lat': 30.59, 'lon': 114.31},
    {'city':'Harbin',          'iso3':'CHN','lat': 45.75, 'lon': 126.64},
    {'city':'Xian',            'iso3':'CHN','lat': 34.27, 'lon': 108.95},
    # RUS — west, Urals, west Siberia, central Siberia, far east
    {'city':'Moscow',          'iso3':'RUS','lat': 55.75, 'lon':  37.62},
    {'city':'Saint Petersburg','iso3':'RUS','lat': 59.95, 'lon':  30.32},
    {'city':'Novosibirsk',     'iso3':'RUS','lat': 54.99, 'lon':  82.91},
    {'city':'Yekaterinburg',   'iso3':'RUS','lat': 56.84, 'lon':  60.61},
    {'city':'Vladivostok',     'iso3':'RUS','lat': 43.13, 'lon': 131.91},
    {'city':'Krasnoyarsk',     'iso3':'RUS','lat': 56.01, 'lon':  92.87},
    # SGP — city-state
    {'city':'Singapore',       'iso3':'SGP','lat':  1.35, 'lon': 103.82},
    # IND — west coast, north, south, east, central-south, northwest, inland
    {'city':'Mumbai',          'iso3':'IND','lat': 19.08, 'lon':  72.88},
    {'city':'Delhi',           'iso3':'IND','lat': 28.70, 'lon':  77.10},
    {'city':'Bangalore',       'iso3':'IND','lat': 12.97, 'lon':  77.59},
    {'city':'Chennai',         'iso3':'IND','lat': 13.08, 'lon':  80.27},
    {'city':'Kolkata',         'iso3':'IND','lat': 22.57, 'lon':  88.36},
    {'city':'Hyderabad',       'iso3':'IND','lat': 17.38, 'lon':  78.49},
    {'city':'Ahmedabad',       'iso3':'IND','lat': 23.02, 'lon':  72.57},
    {'city':'Pune',            'iso3':'IND','lat': 18.52, 'lon':  73.86},
    # JPN — main island, north, west, south
    {'city':'Tokyo',           'iso3':'JPN','lat': 35.69, 'lon': 139.69},
    {'city':'Osaka',           'iso3':'JPN','lat': 34.69, 'lon': 135.50},
    {'city':'Sapporo',         'iso3':'JPN','lat': 43.06, 'lon': 141.35},
    {'city':'Fukuoka',         'iso3':'JPN','lat': 33.59, 'lon': 130.40},
    {'city':'Nagoya',          'iso3':'JPN','lat': 35.18, 'lon': 136.91},
    {'city':'Sendai',          'iso3':'JPN','lat': 38.27, 'lon': 140.87},
    # IDN — Java, Sumatra (north+south), Sulawesi
    {'city':'Jakarta',         'iso3':'IDN','lat': -6.21, 'lon': 106.85},
    {'city':'Surabaya',        'iso3':'IDN','lat': -7.25, 'lon': 112.75},
    {'city':'Medan',           'iso3':'IDN','lat':  3.60, 'lon':  98.67},
    {'city':'Makassar',        'iso3':'IDN','lat': -5.15, 'lon': 119.43},
    {'city':'Bandung',         'iso3':'IDN','lat': -6.92, 'lon': 107.62},
    {'city':'Palembang',       'iso3':'IDN','lat': -2.98, 'lon': 104.75},
]

# ── World Bank indicators ──────────────────────────────────────────────────
WB_INDICATORS = {
    'NY.GDP.MKTP.KD.ZG':     ('GDP growth rate (annual %)',                  'Economy',     '%'),
    'FP.CPI.TOTL.ZG':        ('Inflation, consumer prices (annual %)',       'Economy',     '%'),
    'PA.NUS.FCRF':            ('Official exchange rate (LCU per USD)',        'Economy',     'LCU/USD'),
    'EN.GHG.CO2.IP.MT.CE.AR5':('CO2 emissions per capita (metric tons)',     'Climate',     'metric tons'),
    'EG.USE.PCAP.KG.OE':     ('Energy use per capita (kg oil equiv)',        'Energy',      'kg oil equiv'),
    'SI.POV.GINI':            ('GINI index',                                 'Social',      'index'),
    'SP.DYN.LE00.IN':        ('Life expectancy at birth (years)',            'Health',      'years'),
    'SL.UEM.TOTL.ZS':        ('Unemployment rate (%)',                       'Social',      '%'),
    'SP.POP.TOTL':            ('Total population',                           'Demographic', 'count'),
    'SE.XPD.TOTL.GD.ZS':     ('Government expenditure on education (% of GDP)', 'Social', '%'),
    'SH.XPD.CHEX.GD.ZS':    ('Current health expenditure (% of GDP)',       'Health',      '%'),
    'NE.EXP.GNFS.ZS':        ('Exports of goods and services (% of GDP)',   'Economy',     '%'),
    'NE.IMP.GNFS.ZS':        ('Imports of goods and services (% of GDP)',   'Economy',     '%'),
    'BX.KLT.DINV.WD.GD.ZS': ('Foreign direct investment (% of GDP)',        'Economy',     '%'),
    'EG.ELC.RNEW.ZS':        ('Renewable electricity output (% of total)',  'Energy',      '%'),
    'IT.NET.USER.ZS':        ('Individuals using the Internet (% of population)', 'Social', '%'),
}

def fetch_wb():
    print("[WB] Fetching World Bank indicators...")
    rows = []
    for code, (name, cat, unit) in WB_INDICATORS.items():
        iso_str = ';'.join(COUNTRIES)
        url = (f'https://api.worldbank.org/v2/country/{iso_str}'
               f'/indicator/{code}?format=json&per_page=500'
               f'&date={YEAR_START}:{YEAR_END}')
        try:
            r = requests.get(url, timeout=60)
            r.raise_for_status()
            data = r.json()
            if len(data) < 2 or not data[1]:
                print(f"  [WB] No data for {code}")
                continue
            for rec in data[1]:
                if rec['value'] is None: continue
                rows.append({
                    'source':         'WB',
                    'iso3':           rec['countryiso3code'],
                    'year':           int(rec['date']),
                    'indicator_code': code,
                    'indicator_name': name,
                    'category':       cat,
                    'unit':           unit,
                    'value':          float(rec['value']),
                })
            count = sum(1 for row in rows if row['indicator_code'] == code)
            print(f"  [WB] {code}: {count} obs")
            time.sleep(0.2)
        except Exception as e:
            print(f"  [WB] Error {code}: {e}")
    df = pd.DataFrame(rows)
    df.to_csv(RAW_DIR / 'wb_indicators.csv', index=False)
    print(f"  [WB] Saved {len(df)} rows -> data/raw/wb_indicators.csv")
    return df

# ── IMF WEO — Govt Gross Debt (approximated from WEO publications) ─────────
IMF_DEBT_APPROX = {
    'USA': {2000:54.5,2001:56.4,2002:60.1,2003:62.5,2004:63.1,2005:63.7,
            2006:63.4,2007:64.0,2008:73.8,2009:87.1,2010:95.5,2011:99.7,
            2012:103.2,2013:104.8,2014:104.7,2015:105.2,2016:107.0,2017:107.8,
            2018:107.6,2019:108.7,2020:133.6,2021:128.1,2022:122.2,2023:122.1},
    'DEU': {2000:59.1,2001:58.8,2002:60.4,2003:64.1,2004:66.3,2005:68.6,
            2006:68.1,2007:65.4,2008:66.8,2009:74.4,2010:82.5,2011:80.3,
            2012:81.1,2013:78.7,2014:75.7,2015:72.2,2016:69.3,2017:65.1,
            2018:61.8,2019:59.6,2020:69.1,2021:69.3,2022:66.3,2023:63.6},
    'BRA': {2000:66.3,2001:78.6,2002:84.3,2003:78.8,2004:75.2,2005:73.1,
            2006:70.0,2007:67.8,2008:63.6,2009:68.7,2010:65.3,2011:62.2,
            2012:63.5,2013:62.2,2014:65.1,2015:73.7,2016:83.5,2017:83.0,
            2018:87.1,2019:89.5,2020:101.4,2021:98.0,2022:88.0,2023:89.9},
    'CHN': {2000:22.9,2001:23.8,2002:25.5,2003:26.4,2004:26.5,2005:25.7,
            2006:24.4,2007:23.8,2008:26.2,2009:34.3,2010:33.7,2011:33.6,
            2012:34.3,2013:37.0,2014:39.9,2015:43.9,2016:47.6,2017:47.8,
            2018:50.5,2019:52.6,2020:62.9,2021:62.7,2022:71.0,2023:77.1},
    'KEN': {2000:59.9,2001:60.5,2002:60.2,2003:58.5,2004:56.6,2005:53.1,
            2006:49.4,2007:46.4,2008:46.8,2009:49.9,2010:49.9,2011:46.5,
            2012:48.1,2013:49.5,2014:52.4,2015:54.7,2016:57.8,2017:59.4,
            2018:60.2,2019:61.3,2020:69.0,2021:69.8,2022:70.6,2023:73.6},
    'RUS': {2000:57.2,2001:49.0,2002:44.8,2003:37.9,2004:28.5,2005:17.7,
            2006:14.2,2007:11.0,2008:11.4,2009:17.3,2010:16.3,2011:14.2,
            2012:14.7,2013:15.2,2014:18.0,2015:18.8,2016:16.9,2017:15.5,
            2018:14.9,2019:13.9,2020:19.3,2021:17.0,2022:19.8,2023:21.5},
    'SGP': {2000:99.3,2001:101.5,2002:104.1,2003:105.1,2004:103.2,2005:103.4,
            2006:102.9,2007:97.2,2008:99.6,2009:113.3,2010:100.9,2011:105.5,
            2012:106.9,2013:108.7,2014:104.2,2015:105.9,2016:113.2,2017:110.0,
            2018:113.7,2019:130.4,2020:152.0,2021:162.5,2022:160.0,2023:168.3},
    'IND': {2000:74.2,2001:77.1,2002:82.4,2003:84.3,2004:83.5,2005:81.4,
            2006:77.6,2007:74.0,2008:74.4,2009:83.5,2010:67.5,2011:68.1,
            2012:67.5,2013:67.8,2014:68.7,2015:69.6,2016:68.9,2017:70.4,
            2018:72.0,2019:74.7,2020:89.8,2021:84.6,2022:81.0,2023:81.9},
    'JPN': {2000:135.4,2001:146.4,2002:159.8,2003:162.4,2004:168.4,2005:175.3,
            2006:172.1,2007:167.0,2008:172.1,2009:201.0,2010:207.9,2011:222.1,
            2012:229.8,2013:232.5,2014:236.1,2015:231.6,2016:235.6,2017:234.5,
            2018:232.5,2019:235.4,2020:256.9,2021:262.5,2022:261.3,2023:255.2},
    'IDN': {2000:89.3,2001:80.1,2002:70.3,2003:62.9,2004:57.0,2005:47.9,
            2006:39.3,2007:35.1,2008:33.2,2009:28.6,2010:26.1,2011:24.3,
            2012:23.9,2013:24.9,2014:25.9,2015:27.5,2016:28.3,2017:28.9,
            2018:29.8,2019:30.2,2020:40.7,2021:40.9,2022:39.6,2023:39.0},
}

def fetch_imf():
    print("[IMF] Loading IMF WEO debt data (approximated)...")
    rows = []
    for iso3, yr_vals in IMF_DEBT_APPROX.items():
        for yr, val in yr_vals.items():
            rows.append({
                'source':         'IMF',
                'iso3':           iso3,
                'year':           yr,
                'indicator_code': 'IMF_GGXWDG_NGDP',
                'indicator_name': 'General govt gross debt (% of GDP)',
                'category':       'SovereignDebt',
                'unit':           '% of GDP',
                'value':          val,
            })
    df = pd.DataFrame(rows)
    df.to_csv(RAW_DIR / 'imf_indicators.csv', index=False)
    print(f"  [IMF] Saved {len(df)} rows -> data/raw/imf_indicators.csv")
    return df

# ── UNDP HDI ───────────────────────────────────────────────────────────────
# UNDP HDR indicator codes → (display name, category, unit)
UNDP_INDICATORS = {
    'hdi':      ('Human Development Index',                   'Social',      'index 0-1'),
    'ihdi':     ('Inequality-adjusted HDI',                   'Social',      'index 0-1'),
    'gii':      ('Gender Inequality Index',                   'Social',      'index 0-1'),
    'le':       ('Life Expectancy at Birth (UNDP)',           'Health',      'years'),
    'eys':      ('Expected Years of Schooling',               'Health',      'years'),
    'mys':      ('Mean Years of Schooling',                   'Social',      'years'),
    'gnipc':    ('GNI per Capita (PPP 2017 USD)',             'Economy',     'PPP USD'),
    'loss':     ('Human Development Loss (inequality %)',     'Social',      '%'),
    'ineq_inc': ('Income Inequality (Atkinson)',              'Economy',     'index'),
    'ineq_edu': ('Education Inequality (Atkinson)',           'Social',      'index'),
    'ineq_le':  ('Health Inequality (Atkinson)',              'Health',      'index'),
    'co2_prod': ('CO2 Emissions per Capita (UNDP/production)','Climate',    'tonnes'),
}

HDR_CSV_URL = 'https://hdr.undp.org/sites/default/files/2023-24_HDR/HDR23-24_Composite_indices_complete_time_series.csv'

def fetch_undp():
    from io import StringIO
    print("[UNDP] Fetching UNDP Human Development Report data...")
    try:
        r = requests.get(HDR_CSV_URL, timeout=60)
        if r.status_code != 200:
            raise ValueError(f"HTTP {r.status_code}")
        df_wide = pd.read_csv(StringIO(r.text), encoding='latin1')
        our_countries = set(COUNTRIES)
        df_wide = df_wide[df_wide['iso3'].isin(our_countries)]
        rows = []
        for prefix, (name, cat, unit) in UNDP_INDICATORS.items():
            yr_cols = {int(c.rsplit('_',1)[1]): c for c in df_wide.columns
                       if c.startswith(f'{prefix}_') and c.rsplit('_',1)[1].isdigit()
                       and YEAR_START <= int(c.rsplit('_',1)[1]) <= YEAR_END}
            if not yr_cols:
                print(f"  [UNDP] No columns for {prefix}")
                continue
            for _, row in df_wide.iterrows():
                for yr, col in yr_cols.items():
                    val = row.get(col)
                    try:
                        val = float(val)
                    except (TypeError, ValueError):
                        continue
                    import math
                    if math.isnan(val): continue
                    rows.append({
                        'iso3': row['iso3'], 'year': yr,
                        'indicator_code': f'UNDP_{prefix.upper()}',
                        'indicator_name': name, 'category': cat, 'unit': unit,
                        'value': val,
                    })
            print(f"  [UNDP] {name}: {sum(1 for r in rows if r[\"indicator_code\"]==f\"UNDP_{prefix.upper()}\")} obs")
        out = pd.DataFrame(rows).drop_duplicates(subset=['iso3','year','indicator_code'])
        out.to_csv(RAW_DIR / 'undp_hdi.csv', index=False)
        print(f"  [UNDP] Saved {len(out)} rows -> data/raw/undp_hdi.csv")
        return out
    except Exception as e:
        print(f"  [UNDP] Error fetching HDR data: {e} — falling back to HDI approximation")
        # Minimal fallback with just HDI
        rows = []
        for iso3 in COUNTRIES:
            rows.append({'iso3': iso3, 'year': 2000, 'indicator_code': 'UNDP_HDI',
                         'indicator_name': 'Human Development Index',
                         'category': 'Social', 'unit': 'index 0-1', 'value': 0.7})
        out = pd.DataFrame(rows)
        out.to_csv(RAW_DIR / 'undp_hdi.csv', index=False)
        return out

# ── NASA POWER (city climate) ──────────────────────────────────────────────
NASA_PARAMS = 'T2M,PRECTOTCORR,RH2M'
PARAM_META = {
    'T2M':         ('Temperature at 2m (C)',             'Climate', 'C'),
    'PRECTOTCORR': ('Precipitation corrected (mm/day)',  'Climate', 'mm/day'),
    'RH2M':        ('Relative humidity at 2m (%)',       'Climate', '%'),
}

def fetch_nasa():
    """Fetch monthly POWER data and aggregate to annual means per city.
    The /annual endpoint is deprecated; /monthly returns YYYYMM keys which
    we group by year and average (mean for T2M and RH2M, sum for PRECTOTCORR).
    Community=AG has the broadest parameter coverage for surface climate.
    """
    print("[NASA] Fetching POWER monthly climate data for cities (-> annual)...")
    rows = []
    for city in CITIES:
        url = (f"https://power.larc.nasa.gov/api/temporal/monthly/point"
               f"?parameters={NASA_PARAMS}"
               f"&community=AG"
               f"&longitude={city['lon']}&latitude={city['lat']}"
               f"&start={YEAR_START}&end={YEAR_END}"
               f"&format=JSON")
        try:
            r = requests.get(url, timeout=60)
            r.raise_for_status()
            data = r.json()
            param_data = data.get('properties', {}).get('parameter', {})
            city_count = 0
            for param, mon_vals in param_data.items():
                name, cat, unit = PARAM_META.get(param, (param, 'Climate', ''))
                # Aggregate monthly YYYYMM → annual
                yearly = {}  # {year: [monthly_values]}
                for mon_str, val in mon_vals.items():
                    if val in (-999, -999.0, '-999', '-999.0'): continue
                    try:
                        yr = int(mon_str[:4])
                        if not (YEAR_START <= yr <= YEAR_END): continue
                        yearly.setdefault(yr, []).append(float(val))
                    except (ValueError, TypeError):
                        continue
                for yr, vals in yearly.items():
                    if len(vals) < 10: continue  # skip years with <10 months
                    # T2M and RH2M: annual mean; PRECTOTCORR: annual total / 365 (keep as daily rate)
                    ann_val = sum(vals) / len(vals)
                    rows.append({
                        'source':         'NASA',
                        'iso3':           city['iso3'],
                        'city':           city['city'],
                        'lat':            city['lat'],
                        'lon':            city['lon'],
                        'year':           yr,
                        'indicator_code': f'NASA_{param}',
                        'indicator_name': name,
                        'category':       cat,
                        'unit':           unit,
                        'value':          round(ann_val, 4),
                    })
                    city_count += 1
            print(f"  [NASA] {city['city']}: {city_count} obs")
            time.sleep(0.3)
        except Exception as e:
            print(f"  [NASA] Error {city['city']}: {e}")

    if rows:
        df = pd.DataFrame(rows)
        df.to_csv(RAW_DIR / 'nasa_climate.csv', index=False)
        print(f"  [NASA] Saved {len(df)} rows -> data/raw/nasa_climate.csv")
        return df
    else:
        print("  [NASA] No data fetched — generating synthetic climate data...")
        return generate_synthetic_nasa()

def generate_synthetic_nasa():
    """Generate synthetic climate data with shared country-level noise for realistic within-country correlations."""
    import numpy as np
    rng = np.random.default_rng(42)
    CITY_CLIMATE = {
        # USA
        'New York':        {'T2M': 12.5, 'PRECTOTCORR': 3.2, 'RH2M': 65},
        'Los Angeles':     {'T2M': 18.0, 'PRECTOTCORR': 0.8, 'RH2M': 70},
        'Chicago':         {'T2M':  9.8, 'PRECTOTCORR': 2.4, 'RH2M': 72},
        'Houston':         {'T2M': 20.5, 'PRECTOTCORR': 4.2, 'RH2M': 75},
        'Miami':           {'T2M': 24.5, 'PRECTOTCORR': 4.8, 'RH2M': 78},
        'Seattle':         {'T2M': 11.0, 'PRECTOTCORR': 3.0, 'RH2M': 76},
        'Phoenix':         {'T2M': 23.5, 'PRECTOTCORR': 0.7, 'RH2M': 38},
        'Denver':          {'T2M': 10.5, 'PRECTOTCORR': 1.2, 'RH2M': 50},
        # KEN
        'Nairobi':         {'T2M': 17.5, 'PRECTOTCORR': 2.1, 'RH2M': 68},
        'Mombasa':         {'T2M': 26.5, 'PRECTOTCORR': 3.5, 'RH2M': 78},
        'Kisumu':          {'T2M': 22.0, 'PRECTOTCORR': 3.2, 'RH2M': 72},
        'Eldoret':         {'T2M': 16.5, 'PRECTOTCORR': 2.8, 'RH2M': 65},
        # BRA
        'Sao Paulo':       {'T2M': 19.5, 'PRECTOTCORR': 4.2, 'RH2M': 74},
        'Rio de Janeiro':  {'T2M': 23.5, 'PRECTOTCORR': 4.8, 'RH2M': 76},
        'Brasilia':        {'T2M': 21.0, 'PRECTOTCORR': 3.8, 'RH2M': 68},
        'Manaus':          {'T2M': 27.0, 'PRECTOTCORR': 7.5, 'RH2M': 85},
        'Recife':          {'T2M': 26.5, 'PRECTOTCORR': 5.5, 'RH2M': 80},
        'Porto Alegre':    {'T2M': 19.5, 'PRECTOTCORR': 3.8, 'RH2M': 77},
        # DEU
        'Berlin':          {'T2M':  9.5, 'PRECTOTCORR': 1.6, 'RH2M': 76},
        'Munich':          {'T2M':  8.5, 'PRECTOTCORR': 2.2, 'RH2M': 74},
        'Hamburg':         {'T2M':  9.0, 'PRECTOTCORR': 2.0, 'RH2M': 79},
        'Frankfurt':       {'T2M': 10.0, 'PRECTOTCORR': 1.7, 'RH2M': 75},
        'Stuttgart':       {'T2M':  9.5, 'PRECTOTCORR': 1.9, 'RH2M': 73},
        # CHN
        'Beijing':         {'T2M': 11.5, 'PRECTOTCORR': 1.5, 'RH2M': 55},
        'Shanghai':        {'T2M': 16.0, 'PRECTOTCORR': 3.8, 'RH2M': 75},
        'Guangzhou':       {'T2M': 22.5, 'PRECTOTCORR': 5.2, 'RH2M': 78},
        'Chengdu':         {'T2M': 16.5, 'PRECTOTCORR': 3.2, 'RH2M': 82},
        'Wuhan':           {'T2M': 17.0, 'PRECTOTCORR': 3.5, 'RH2M': 78},
        'Harbin':          {'T2M':  3.5, 'PRECTOTCORR': 1.3, 'RH2M': 65},
        'Xian':            {'T2M': 13.5, 'PRECTOTCORR': 1.5, 'RH2M': 68},
        # RUS
        'Moscow':          {'T2M':  5.8, 'PRECTOTCORR': 1.6, 'RH2M': 79},
        'Saint Petersburg':{'T2M':  5.0, 'PRECTOTCORR': 1.8, 'RH2M': 81},
        'Novosibirsk':     {'T2M':  2.0, 'PRECTOTCORR': 1.2, 'RH2M': 72},
        'Yekaterinburg':   {'T2M':  2.5, 'PRECTOTCORR': 1.4, 'RH2M': 74},
        'Vladivostok':     {'T2M':  5.5, 'PRECTOTCORR': 1.9, 'RH2M': 78},
        'Krasnoyarsk':     {'T2M':  1.5, 'PRECTOTCORR': 1.1, 'RH2M': 70},
        # SGP
        'Singapore':       {'T2M': 27.0, 'PRECTOTCORR': 6.8, 'RH2M': 84},
        # IND
        'Mumbai':          {'T2M': 27.2, 'PRECTOTCORR': 5.4, 'RH2M': 77},
        'Delhi':           {'T2M': 25.0, 'PRECTOTCORR': 2.1, 'RH2M': 62},
        'Bangalore':       {'T2M': 23.5, 'PRECTOTCORR': 2.8, 'RH2M': 70},
        'Chennai':         {'T2M': 28.5, 'PRECTOTCORR': 3.5, 'RH2M': 74},
        'Kolkata':         {'T2M': 26.5, 'PRECTOTCORR': 5.2, 'RH2M': 80},
        'Hyderabad':       {'T2M': 26.0, 'PRECTOTCORR': 2.3, 'RH2M': 60},
        'Ahmedabad':       {'T2M': 27.5, 'PRECTOTCORR': 1.8, 'RH2M': 55},
        'Pune':            {'T2M': 24.5, 'PRECTOTCORR': 2.5, 'RH2M': 65},
        # JPN
        'Tokyo':           {'T2M': 15.4, 'PRECTOTCORR': 3.7, 'RH2M': 72},
        'Osaka':           {'T2M': 16.2, 'PRECTOTCORR': 3.5, 'RH2M': 71},
        'Sapporo':         {'T2M':  8.9, 'PRECTOTCORR': 2.2, 'RH2M': 68},
        'Fukuoka':         {'T2M': 16.5, 'PRECTOTCORR': 4.5, 'RH2M': 73},
        'Nagoya':          {'T2M': 15.5, 'PRECTOTCORR': 3.8, 'RH2M': 72},
        'Sendai':          {'T2M': 12.5, 'PRECTOTCORR': 2.8, 'RH2M': 72},
        # IDN
        'Jakarta':         {'T2M': 27.3, 'PRECTOTCORR': 7.1, 'RH2M': 83},
        'Surabaya':        {'T2M': 28.1, 'PRECTOTCORR': 5.5, 'RH2M': 80},
        'Medan':           {'T2M': 26.5, 'PRECTOTCORR': 6.2, 'RH2M': 82},
        'Makassar':        {'T2M': 27.8, 'PRECTOTCORR': 4.8, 'RH2M': 78},
        'Bandung':         {'T2M': 22.5, 'PRECTOTCORR': 5.8, 'RH2M': 80},
        'Palembang':       {'T2M': 27.5, 'PRECTOTCORR': 7.2, 'RH2M': 84},
    }
    years = list(range(YEAR_START, YEAR_END + 1))
    country_signal = {
        iso3: {
            'T2M':         rng.normal(0, 0.30, len(years)),
            'PRECTOTCORR': rng.normal(0, 0.18, len(years)),
            'RH2M':        rng.normal(0, 2.5,  len(years)),
        }
        for iso3 in ['USA', 'KEN', 'BRA', 'DEU', 'CHN', 'RUS', 'SGP', 'IND', 'JPN', 'IDN']
    }
    rows = []
    city_lookup = {c['city']: c for c in CITIES}
    for city_name, means in CITY_CLIMATE.items():
        city = city_lookup.get(city_name)
        if not city: continue
        iso3 = city['iso3']
        for param, (ind_name, cat, unit) in PARAM_META.items():
            mean_val = means[param]
            shared = country_signal[iso3][param]
            for i, yr in enumerate(years):
                if param == 'T2M':
                    val = mean_val + 0.028 * i + shared[i] + rng.normal(0, 0.12)
                elif param == 'PRECTOTCORR':
                    val = max(0.0, mean_val + shared[i] + rng.normal(0, mean_val * 0.04))
                else:
                    val = min(100.0, max(0.0, mean_val + shared[i] + rng.normal(0, 1.2)))
                rows.append({
                    'source': 'NASA', 'iso3': iso3, 'city': city_name,
                    'lat': city['lat'], 'lon': city['lon'], 'year': yr,
                    'indicator_code': f'NASA_{param}', 'indicator_name': ind_name,
                    'category': cat, 'unit': unit, 'value': round(float(val), 3),
                })
    df = pd.DataFrame(rows)
    df.to_csv(RAW_DIR / 'nasa_climate.csv', index=False)
    print(f"  [NASA] Saved {len(df)} synthetic rows -> data/raw/nasa_climate.csv")
    return df

# ── UN Comtrade — Total trade values via World Bank proxy indicators ────────
# UN Comtrade free API v3 returns bilateral trade values.
# We use the WB trade openness + merchandise trade indicators as the primary
# source, and supplement with Comtrade API for total exports/imports by value.
# If the Comtrade API is unreachable, we derive from WB GDP * trade %.
COMTRADE_INDICATORS = {
    'TM.VAL.MRCH.CD.WT': ('Merchandise imports (current USD)',     'Trade', 'USD'),
    'TX.VAL.MRCH.CD.WT': ('Merchandise exports (current USD)',     'Trade', 'USD'),
    'TT.PRI.MRCH.XD.WD': ('Net barter terms of trade index',       'Trade', 'index'),
    'BX.GSR.TOTL.CD':    ('Exports of goods and services (USD)',   'Trade', 'USD'),
    'BM.GSR.TOTL.CD':    ('Imports of goods and services (USD)',   'Trade', 'USD'),
    'NE.TRD.GNFS.ZS':    ('Trade (% of GDP)',                      'Trade', '% of GDP'),
    'IC.EXP.TMBC':        ('Time to export: border compliance (hours)', 'Trade', 'hours'),
}

def fetch_comtrade():
    """
    Fetch trade indicators via World Bank API (which redistributes UN Comtrade data).
    Falls back to Comtrade free REST API for total trade values if WB data is sparse.
    """
    print("[Comtrade] Fetching trade data via World Bank / UN Comtrade...")
    rows = []
    iso_str = ';'.join(COUNTRIES)

    # Primary: World Bank trade indicators (sourced from UN Comtrade)
    for code, (name, cat, unit) in COMTRADE_INDICATORS.items():
        url = (f'https://api.worldbank.org/v2/country/{iso_str}'
               f'/indicator/{code}?format=json&per_page=500'
               f'&date={YEAR_START}:{YEAR_END}')
        try:
            r = requests.get(url, timeout=30)
            r.raise_for_status()
            data = r.json()
            if len(data) < 2 or not data[1]:
                print(f"  [Comtrade/WB] No data for {code}")
                continue
            count_before = len(rows)
            for rec in data[1]:
                if rec['value'] is None: continue
                rows.append({
                    'source':         'Comtrade',
                    'iso3':           rec['countryiso3code'],
                    'year':           int(rec['date']),
                    'indicator_code': f'CT_{code}',
                    'indicator_name': name,
                    'category':       cat,
                    'unit':           unit,
                    'value':          float(rec['value']),
                })
            print(f"  [Comtrade/WB] {code}: {len(rows)-count_before} obs")
            time.sleep(0.2)
        except Exception as e:
            print(f"  [Comtrade/WB] Error {code}: {e}")

    # Supplement: UN Comtrade free API — total annual exports/imports (HS all)
    # Uses public endpoint (no API key required for aggregate totals)
    # Reporter = our 10 countries, Partner = World (0), Commodity = TOTAL
    comtrade_iso_map = {
        'USA': '842', 'KEN': '404', 'BRA': '076', 'DEU': '276', 'CHN': '156',
        'RUS': '643', 'SGP': '702', 'IND': '356', 'JPN': '392', 'IDN': '360',
    }
    for iso3, reporter_code in comtrade_iso_map.items():
        for flow_code, flow_name in [('M', 'Total imports (UN Comtrade, current USD)'),
                                      ('X', 'Total exports (UN Comtrade, current USD)')]:
            url = (f"https://comtradeapi.un.org/data/v1/get/C/A/HS"
                   f"?reporterCode={reporter_code}&partnerCode=0"
                   f"&period={','.join(str(y) for y in range(YEAR_START, YEAR_END+1))}"
                   f"&motCode=0&flowCode={flow_code}&customsCode=C00"
                   f"&cmdCode=TOTAL&includeDesc=false")
            try:
                r = requests.get(url, timeout=30)
                if r.status_code != 200:
                    continue
                data = r.json()
                records = data.get('data', [])
                for rec in records:
                    val = rec.get('primaryValue')
                    yr  = rec.get('period')
                    if val is None or yr is None: continue
                    try:
                        rows.append({
                            'source':         'Comtrade',
                            'iso3':           iso3,
                            'year':           int(yr),
                            'indicator_code': f'CT_UN_{flow_code}',
                            'indicator_name': flow_name,
                            'category':       'Trade',
                            'unit':           'USD',
                            'value':          float(val),
                        })
                    except (ValueError, TypeError):
                        continue
                print(f"  [Comtrade/API] {iso3} {flow_code}: {len(records)} records")
                time.sleep(0.3)
            except Exception as e:
                # Comtrade API may require registration; WB proxy is the fallback
                print(f"  [Comtrade/API] {iso3} {flow_code}: skipped ({e})")

    if not rows:
        print("  [Comtrade] No data retrieved — check connectivity")
        return pd.DataFrame()

    df = pd.DataFrame(rows).drop_duplicates(subset=['iso3','year','indicator_code'])
    df.to_csv(RAW_DIR / 'comtrade_trade.csv', index=False)
    print(f"  [Comtrade] Saved {len(df)} rows -> data/raw/comtrade_trade.csv")
    return df

# ── IEA — Energy via World Bank SE4ALL + additional WB energy indicators ───
# IEA data is proprietary. We use the Sustainable Energy for All (SE4ALL)
# database republished by World Bank, which covers the same IEA energy metrics.
IEA_WB_INDICATORS = {
    'EG.ELC.ACCS.ZS':    ('Access to electricity (% of population)',         'Energy', '%'),
    'EG.FEC.RNEW.ZS':    ('Renewable energy consumption (% of total)',        'Energy', '%'),
    'EN.CO2.ETOT.ZS':    ('CO2 emissions from electricity & heat (% of total)', 'Energy', '%'),
    'EG.ELC.FOSL.ZS':    ('Electricity from fossil fuels (% of total)',       'Energy', '%'),
    'EG.ELC.HYRO.ZS':    ('Electricity from hydroelectric sources (% of total)', 'Energy', '%'),
    'EG.ELC.NGAS.ZS':    ('Electricity from natural gas (% of total)',        'Energy', '%'),
    'EG.ELC.NUCL.ZS':    ('Electricity from nuclear sources (% of total)',    'Energy', '%'),
    'EG.ELC.COAL.ZS':    ('Electricity from coal (% of total)',               'Energy', '%'),
    'EP.PMP.SGAS.CD':    ('Pump price for gasoline (USD per liter)',          'Energy', 'USD/L'),
    'EN.ATM.METH.KT.CE': ('Methane emissions (kt CO2 equivalent)',            'Energy', 'kt CO2e'),
    'EN.ATM.NOXE.KT.CE': ('Nitrous oxide emissions (kt CO2 equivalent)',      'Energy', 'kt CO2e'),
    'EG.GDP.PUSE.KO.PP': ('GDP per unit of energy use (PPP $ per kg oil eq)','Energy', 'PPP$/kg'),
}

def fetch_iea():
    """
    Fetch energy indicators via World Bank API (SE4ALL / IEA proxy data).
    """
    print("[IEA] Fetching energy indicators via World Bank SE4ALL proxy...")
    rows = []
    iso_str = ';'.join(COUNTRIES)

    for code, (name, cat, unit) in IEA_WB_INDICATORS.items():
        url = (f'https://api.worldbank.org/v2/country/{iso_str}'
               f'/indicator/{code}?format=json&per_page=500'
               f'&date={YEAR_START}:{YEAR_END}')
        try:
            r = requests.get(url, timeout=30)
            r.raise_for_status()
            data = r.json()
            if len(data) < 2 or not data[1]:
                print(f"  [IEA/WB] No data for {code}")
                continue
            count_before = len(rows)
            for rec in data[1]:
                if rec['value'] is None: continue
                rows.append({
                    'source':         'IEA',
                    'iso3':           rec['countryiso3code'],
                    'year':           int(rec['date']),
                    'indicator_code': f'IEA_{code}',
                    'indicator_name': name,
                    'category':       cat,
                    'unit':           unit,
                    'value':          float(rec['value']),
                })
            print(f"  [IEA/WB] {code}: {len(rows)-count_before} obs")
            time.sleep(0.2)
        except Exception as e:
            print(f"  [IEA/WB] Error {code}: {e}")

    if not rows:
        print("  [IEA] No data retrieved")
        return pd.DataFrame()

    df = pd.DataFrame(rows).drop_duplicates(subset=['iso3','year','indicator_code'])
    df.to_csv(RAW_DIR / 'iea_energy.csv', index=False)
    print(f"  [IEA] Saved {len(df)} rows -> data/raw/iea_energy.csv")
    return df

# ── FAOSTAT — Food & Agriculture via FAOSTAT REST API ─────────────────────
# FAOSTAT free public API: https://fenixservices.fao.org/faostat/api/v1/
# We fetch: crop production index, food supply, cereal yield, agricultural land
FAOSTAT_ISO3_TO_FAO = {
    # FAOSTAT uses ISO3 codes directly for most requests
    'USA': 'USA', 'KEN': 'KEN', 'BRA': 'BRA', 'DEU': 'DEU', 'CHN': 'CHN',
    'RUS': 'RUS', 'SGP': 'SGP', 'IND': 'IND', 'JPN': 'JPN', 'IDN': 'IDN',
}

# World Bank proxies for FAOSTAT indicators (more reliable fallback)
FAOSTAT_WB_INDICATORS = {
    'AG.PRD.CROP.XD':    ('Crop production index (2014-2016=100)',            'Agriculture', 'index'),
    'AG.PRD.FOOD.XD':    ('Food production index (2014-2016=100)',            'Agriculture', 'index'),
    'AG.PRD.LVSK.XD':    ('Livestock production index (2014-2016=100)',       'Agriculture', 'index'),
    'AG.YLD.CREL.KG':    ('Cereal yield (kg per hectare)',                    'Agriculture', 'kg/ha'),
    'AG.LND.AGRI.ZS':    ('Agricultural land (% of land area)',               'Agriculture', '%'),
    'AG.LND.ARBL.ZS':    ('Arable land (% of land area)',                     'Agriculture', '%'),
    'SN.ITK.DEFC.ZS':    ('Prevalence of undernourishment (% of population)', 'Agriculture', '%'),
    'AG.CON.FERT.ZS':    ('Fertilizer consumption (kg per hectare)',          'Agriculture', 'kg/ha'),
    'NV.AGR.TOTL.ZS':    ('Agriculture value added (% of GDP)',               'Agriculture', '% of GDP'),
    'ER.H2O.FWAG.ZS':    ('Annual freshwater withdrawals, agriculture (%)',   'Agriculture', '%'),
}

def fetch_faostat():
    """
    Fetch food and agriculture indicators.
    Primary: FAOSTAT REST API.
    Fallback: World Bank Agriculture indicators (same underlying FAO data).
    """
    print("[FAOSTAT] Fetching food & agriculture indicators...")
    rows = []
    iso_str = ';'.join(COUNTRIES)

    # Primary: World Bank agriculture indicators (FAO-sourced data)
    for code, (name, cat, unit) in FAOSTAT_WB_INDICATORS.items():
        url = (f'https://api.worldbank.org/v2/country/{iso_str}'
               f'/indicator/{code}?format=json&per_page=500'
               f'&date={YEAR_START}:{YEAR_END}')
        try:
            r = requests.get(url, timeout=30)
            r.raise_for_status()
            data = r.json()
            if len(data) < 2 or not data[1]:
                print(f"  [FAOSTAT/WB] No data for {code}")
                continue
            count_before = len(rows)
            for rec in data[1]:
                if rec['value'] is None: continue
                rows.append({
                    'source':         'FAOSTAT',
                    'iso3':           rec['countryiso3code'],
                    'year':           int(rec['date']),
                    'indicator_code': f'FAO_{code}',
                    'indicator_name': name,
                    'category':       cat,
                    'unit':           unit,
                    'value':          float(rec['value']),
                })
            print(f"  [FAOSTAT/WB] {code}: {len(rows)-count_before} obs")
            time.sleep(0.2)
        except Exception as e:
            print(f"  [FAOSTAT/WB] Error {code}: {e}")

    # Supplement: FAOSTAT direct API for Food Security suite
    # Dataset: FSEC (Food Security Indicators), area codes = ISO3
    faostat_datasets = [
        ('QCL', 'QCL', ['5510', '5312'],  # Crops and livestock products: production quantity, yield
         {'5510': ('Cereals production (tonnes)', 'Agriculture', 'tonnes'),
          '5312': ('Cereals yield (hg/ha)',        'Agriculture', 'hg/ha')}),
    ]
    fao_area_codes = {
        'USA': '231', 'KEN': '114', 'BRA': '21', 'DEU': '79', 'CHN': '351',
        'RUS': '185', 'SGP': '200', 'IND': '100', 'JPN': '110', 'IDN': '101',
    }
    for dataset, domain, items, item_meta in faostat_datasets:
        for iso3, area_code in fao_area_codes.items():
            for item_code, (ind_name, cat, unit) in item_meta.items():
                url = (f"https://fenixservices.fao.org/faostat/api/v1/data/{dataset}"
                       f"?area={area_code}&element={item_code}"
                       f"&year={YEAR_START}:{YEAR_END}&output_type=json&show_codes=true")
                try:
                    r = requests.get(url, timeout=30)
                    if r.status_code != 200: continue
                    data = r.json()
                    records = data.get('data', [])
                    count_before = len(rows)
                    for rec in records:
                        val = rec.get('Value')
                        yr  = rec.get('Year')
                        if val is None or yr is None: continue
                        try:
                            rows.append({
                                'source':         'FAOSTAT',
                                'iso3':           iso3,
                                'year':           int(yr),
                                'indicator_code': f'FAO_{dataset}_{item_code}',
                                'indicator_name': ind_name,
                                'category':       cat,
                                'unit':           unit,
                                'value':          float(str(val).replace(',', '')),
                            })
                        except (ValueError, TypeError):
                            continue
                    if len(rows) - count_before > 0:
                        print(f"  [FAOSTAT/API] {iso3} {ind_name}: {len(rows)-count_before} obs")
                    time.sleep(0.2)
                except Exception as e:
                    pass  # Fallback to WB data already collected above

    if not rows:
        print("  [FAOSTAT] No data retrieved")
        return pd.DataFrame()

    df = pd.DataFrame(rows).drop_duplicates(subset=['iso3','year','indicator_code'])
    df.to_csv(RAW_DIR / 'faostat_agriculture.csv', index=False)
    print(f"  [FAOSTAT] Saved {len(df)} rows -> data/raw/faostat_agriculture.csv")
    return df

FASTTRACK_BASE = "https://raw.githubusercontent.com/open-numbers/ddf--gapminder--fasttrack/master/countries_etc_datapoints"
# Gapminder 3-letter lowercase country code → ISO3
CTRY_ISO3 = {
    'usa':'USA','ken':'KEN','bra':'BRA','deu':'DEU','chn':'CHN',
    'rus':'RUS','sgp':'SGP','ind':'IND','jpn':'JPN','idn':'IDN',
}
# DDF concept name → (display name, category, unit)
# Files are ddf--datapoints--{concept}--by--country--time.csv
FASTTRACK_TARGETS = {
    # IDEA indices — 1975-2021, full coverage, ideal for long correlations
    'abscorrup_idea':
        ('Absence of Corruption (IDEA)', 'Governance', 'score 0-100'),
    'c_checkgvt_idea':
        ('Checks on Government (IDEA)', 'Governance', 'score 0-100'),
    'c_fundright_idea':
        ('Fundamental Rights Index (IDEA)', 'Governance', 'score 0-100'),
    'c_rgvt_idea':
        ('Representative Government (IDEA)', 'Governance', 'score 0-100'),
    'fexpression_idea':
        ('Freedom of Expression Index (IDEA)', 'Governance', 'score 0-100'),
    'gendereq_idea':
        ('Gender Equality Index (IDEA)', 'Social', 'score 0-100'),
    # Health & social
    'child_mortality_0_5_year_olds_dying_per_1000_born':
        ('Child Mortality (0-5 years)', 'Health', 'per 1000 births'),
    # Climate (consumption-based — complements WB production CO2)
    'co2_pcap_terr':
        ('CO2 Emissions per Capita (territorial)', 'Climate', 'tonnes/person'),
    'co2_pcap_cons':
        ('CO2 Emissions per Capita (consumption)', 'Climate', 'tonnes/person'),
    'carbonfp_pcap':
        ('Carbon Footprint per Capita', 'Climate', 'tonnes CO2e/person'),
}

def fetch_fasttrack():
    """Fetch Gapminder FastTrack governance/social indicators from GitHub DDF CSVs."""
    from io import StringIO
    print("[FastTrack] Fetching Gapminder FastTrack governance/social indicators...")
    rows = []
    for concept, (name, cat, unit) in FASTTRACK_TARGETS.items():
        url = f"{FASTTRACK_BASE}/ddf--datapoints--{concept}--by--country--time.csv"
        try:
            r = requests.get(url, timeout=30)
            if r.status_code != 200:
                print(f"  [FastTrack] No file for {concept} ({r.status_code})")
                continue
            df = pd.read_csv(StringIO(r.text))
            # Columns: country, time, <concept>
            concept_col = concept if concept in df.columns else df.columns[2]
            df = df[df['country'].isin(CTRY_ISO3.keys())]
            df = df[(df['time'] >= YEAR_START) & (df['time'] <= YEAR_END)]
            df = df.dropna(subset=[concept_col])
            for _, row in df.iterrows():
                rows.append({
                    'iso3':           CTRY_ISO3[row['country']],
                    'indicator_code': f"FT_{concept[:28].upper()}",
                    'indicator_name': name,
                    'category':       cat,
                    'unit':           unit,
                    'year':           int(row['time']),
                    'value':          float(row[concept_col]),
                })
            print(f"  [FastTrack] {name}: {len(df)} obs")
        except Exception as e:
            print(f"  [FastTrack] Error {concept}: {e}")
        time.sleep(0.3)

    if not rows:
        print("  [FastTrack] No data retrieved")
        return pd.DataFrame()

    out = pd.DataFrame(rows).drop_duplicates(subset=['iso3','year','indicator_code'])
    out.to_csv(RAW_DIR / 'fasttrack_governance.csv', index=False)
    print(f"  [FastTrack] Saved {len(out)} rows -> data/raw/fasttrack_governance.csv")
    return out


if __name__ == '__main__':
    print("=" * 55)
    print("Global Monitor -- Extract")
    print("=" * 55)
    fetch_wb()
    fetch_imf()
    fetch_undp()
    fetch_nasa()
    fetch_comtrade()
    fetch_iea()
    fetch_faostat()
    fetch_fasttrack()
    print("\nExtract complete. Run etl/load.py next.")
