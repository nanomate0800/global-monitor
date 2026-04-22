"""
etl/load.py
Transforms raw CSVs and loads into the SQLite database.
Run: python etl/load.py
"""
import sqlite3, pandas as pd
from pathlib import Path

BASE    = Path(__file__).parent.parent
RAW_DIR = BASE / 'data' / 'raw'
DB_PATH = BASE / 'db' / 'database.db'
DB_PATH.parent.mkdir(parents=True, exist_ok=True)

CITIES = [
    {'city':'New York',        'iso3':'USA','lat': 40.71, 'lon': -74.01},
    {'city':'Los Angeles',     'iso3':'USA','lat': 34.05, 'lon':-118.25},
    {'city':'Chicago',         'iso3':'USA','lat': 41.88, 'lon': -87.63},
    {'city':'Houston',         'iso3':'USA','lat': 29.76, 'lon': -95.37},
    {'city':'Miami',           'iso3':'USA','lat': 25.76, 'lon': -80.19},
    {'city':'Seattle',         'iso3':'USA','lat': 47.61, 'lon':-122.33},
    {'city':'Phoenix',         'iso3':'USA','lat': 33.45, 'lon':-112.07},
    {'city':'Denver',          'iso3':'USA','lat': 39.74, 'lon':-104.99},
    {'city':'Nairobi',         'iso3':'KEN','lat': -1.29, 'lon':  36.82},
    {'city':'Mombasa',         'iso3':'KEN','lat': -4.05, 'lon':  39.67},
    {'city':'Kisumu',          'iso3':'KEN','lat': -0.10, 'lon':  34.76},
    {'city':'Eldoret',         'iso3':'KEN','lat':  0.52, 'lon':  35.27},
    {'city':'Sao Paulo',       'iso3':'BRA','lat':-23.55, 'lon': -46.63},
    {'city':'Rio de Janeiro',  'iso3':'BRA','lat':-22.91, 'lon': -43.17},
    {'city':'Brasilia',        'iso3':'BRA','lat':-15.78, 'lon': -47.93},
    {'city':'Manaus',          'iso3':'BRA','lat': -3.10, 'lon': -60.03},
    {'city':'Recife',          'iso3':'BRA','lat': -8.05, 'lon': -34.88},
    {'city':'Porto Alegre',    'iso3':'BRA','lat':-30.03, 'lon': -51.22},
    {'city':'Berlin',          'iso3':'DEU','lat': 52.52, 'lon':  13.41},
    {'city':'Munich',          'iso3':'DEU','lat': 48.14, 'lon':  11.58},
    {'city':'Hamburg',         'iso3':'DEU','lat': 53.55, 'lon':  10.00},
    {'city':'Frankfurt',       'iso3':'DEU','lat': 50.11, 'lon':   8.68},
    {'city':'Stuttgart',       'iso3':'DEU','lat': 48.78, 'lon':   9.18},
    {'city':'Beijing',         'iso3':'CHN','lat': 39.91, 'lon': 116.39},
    {'city':'Shanghai',        'iso3':'CHN','lat': 31.23, 'lon': 121.47},
    {'city':'Guangzhou',       'iso3':'CHN','lat': 23.12, 'lon': 113.25},
    {'city':'Chengdu',         'iso3':'CHN','lat': 30.57, 'lon': 104.07},
    {'city':'Wuhan',           'iso3':'CHN','lat': 30.59, 'lon': 114.31},
    {'city':'Harbin',          'iso3':'CHN','lat': 45.75, 'lon': 126.64},
    {'city':'Xian',            'iso3':'CHN','lat': 34.27, 'lon': 108.95},
    {'city':'Moscow',          'iso3':'RUS','lat': 55.75, 'lon':  37.62},
    {'city':'Saint Petersburg','iso3':'RUS','lat': 59.95, 'lon':  30.32},
    {'city':'Novosibirsk',     'iso3':'RUS','lat': 54.99, 'lon':  82.91},
    {'city':'Yekaterinburg',   'iso3':'RUS','lat': 56.84, 'lon':  60.61},
    {'city':'Vladivostok',     'iso3':'RUS','lat': 43.13, 'lon': 131.91},
    {'city':'Krasnoyarsk',     'iso3':'RUS','lat': 56.01, 'lon':  92.87},
    {'city':'Singapore',       'iso3':'SGP','lat':  1.35, 'lon': 103.82},
    {'city':'Mumbai',          'iso3':'IND','lat': 19.08, 'lon':  72.88},
    {'city':'Delhi',           'iso3':'IND','lat': 28.70, 'lon':  77.10},
    {'city':'Bangalore',       'iso3':'IND','lat': 12.97, 'lon':  77.59},
    {'city':'Chennai',         'iso3':'IND','lat': 13.08, 'lon':  80.27},
    {'city':'Kolkata',         'iso3':'IND','lat': 22.57, 'lon':  88.36},
    {'city':'Hyderabad',       'iso3':'IND','lat': 17.38, 'lon':  78.49},
    {'city':'Ahmedabad',       'iso3':'IND','lat': 23.02, 'lon':  72.57},
    {'city':'Pune',            'iso3':'IND','lat': 18.52, 'lon':  73.86},
    {'city':'Tokyo',           'iso3':'JPN','lat': 35.69, 'lon': 139.69},
    {'city':'Osaka',           'iso3':'JPN','lat': 34.69, 'lon': 135.50},
    {'city':'Sapporo',         'iso3':'JPN','lat': 43.06, 'lon': 141.35},
    {'city':'Fukuoka',         'iso3':'JPN','lat': 33.59, 'lon': 130.40},
    {'city':'Nagoya',          'iso3':'JPN','lat': 35.18, 'lon': 136.91},
    {'city':'Sendai',          'iso3':'JPN','lat': 38.27, 'lon': 140.87},
    {'city':'Jakarta',         'iso3':'IDN','lat': -6.21, 'lon': 106.85},
    {'city':'Surabaya',        'iso3':'IDN','lat': -7.25, 'lon': 112.75},
    {'city':'Medan',           'iso3':'IDN','lat':  3.60, 'lon':  98.67},
    {'city':'Makassar',        'iso3':'IDN','lat': -5.15, 'lon': 119.43},
    {'city':'Bandung',         'iso3':'IDN','lat': -6.92, 'lon': 107.62},
    {'city':'Palembang',       'iso3':'IDN','lat': -2.98, 'lon': 104.75},
]
COUNTRY_NAMES = {
    'USA':'United States','KEN':'Kenya','BRA':'Brazil','DEU':'Germany','CHN':'China',
    'RUS':'Russia','SGP':'Singapore','IND':'India','JPN':'Japan','IDN':'Indonesia',
}

def create_schema(conn):
    conn.executescript('''
    DROP TABLE IF EXISTS fact_indicators;
    DROP TABLE IF EXISTS dim_indicator;
    DROP TABLE IF EXISTS dim_country;
    DROP TABLE IF EXISTS dim_city;

    CREATE TABLE dim_country (
        country_id   INTEGER PRIMARY KEY AUTOINCREMENT,
        iso3         TEXT NOT NULL UNIQUE,
        country_name TEXT NOT NULL
    );

    CREATE TABLE dim_city (
        city_id    INTEGER PRIMARY KEY AUTOINCREMENT,
        city_name  TEXT NOT NULL,
        iso3       TEXT NOT NULL,
        lat        REAL,
        lon        REAL
    );

    CREATE TABLE dim_indicator (
        indicator_id           INTEGER PRIMARY KEY AUTOINCREMENT,
        source_indicator_code  TEXT NOT NULL,
        indicator_name         TEXT NOT NULL,
        category               TEXT,
        unit                   TEXT,
        UNIQUE(source_indicator_code, indicator_name)
    );

    CREATE TABLE fact_indicators (
        fact_id      INTEGER PRIMARY KEY AUTOINCREMENT,
        iso3         TEXT NOT NULL,
        country_id   INTEGER,
        city_id      INTEGER,
        indicator_id INTEGER NOT NULL,
        year         INTEGER NOT NULL,
        value        REAL,
        FOREIGN KEY (country_id)   REFERENCES dim_country(country_id),
        FOREIGN KEY (city_id)      REFERENCES dim_city(city_id),
        FOREIGN KEY (indicator_id) REFERENCES dim_indicator(indicator_id)
    );

    CREATE INDEX IF NOT EXISTS idx_fact_iso3_year ON fact_indicators(iso3, year);
    CREATE INDEX IF NOT EXISTS idx_fact_indicator  ON fact_indicators(indicator_id);
    ''')
    print("  Schema created.")

def load_dimensions(conn):
    # Countries
    for iso3, name in COUNTRY_NAMES.items():
        conn.execute(
            'INSERT OR IGNORE INTO dim_country(iso3, country_name) VALUES(?,?)',
            (iso3, name)
        )
    # Cities
    for c in CITIES:
        conn.execute(
            'INSERT OR IGNORE INTO dim_city(city_name, iso3, lat, lon) VALUES(?,?,?,?)',
            (c['city'], c['iso3'], c['lat'], c['lon'])
        )
    conn.commit()
    print(f"  Loaded {len(COUNTRY_NAMES)} countries, {len(CITIES)} cities.")

def get_or_create_indicator(conn, code, name, cat, unit):
    row = conn.execute(
        'SELECT indicator_id FROM dim_indicator WHERE source_indicator_code=? AND indicator_name=?',
        (code, name)
    ).fetchone()
    if row:
        return row[0]
    cur = conn.execute(
        'INSERT INTO dim_indicator(source_indicator_code, indicator_name, category, unit) VALUES(?,?,?,?)',
        (code, name, cat, unit)
    )
    return cur.lastrowid

def load_country_facts(conn):
    """Load non-city indicator facts (WB, IMF, UNDP, Comtrade, IEA, FAOSTAT)."""
    files = ['wb_indicators.csv', 'imf_indicators.csv', 'undp_hdi.csv',
             'comtrade_trade.csv', 'iea_energy.csv', 'faostat_agriculture.csv',
             'fasttrack_governance.csv',
             'who_health.csv', 'ilo_labour.csv', 'wto_services_trade.csv',
             'unpop_demographic.csv',
             'supply_chain_maritime.csv', 'unctad_maritime.csv',
             'damodaran_capital_markets.csv',
             'fraser_efw.csv', 'wb_innovation.csv', 'doing_business.csv',
             'usgs_minerals.csv', 'bgs_minerals.csv', 'wb_pink_sheets.csv',
             'comtrade_hs.csv', 'comtrade_v2_metals.csv']
    total = 0
    country_ids = {r[0]: r[1] for r in conn.execute('SELECT iso3, country_id FROM dim_country').fetchall()}
    for fname in files:
        path = RAW_DIR / fname
        if not path.exists():
            print(f"  Skipping missing {fname}")
            continue
        df = pd.read_csv(path)
        rows = []
        for _, rec in df.iterrows():
            iso3 = str(rec['iso3'])
            if iso3 not in country_ids: continue
            ind_id = get_or_create_indicator(
                conn, rec['indicator_code'], rec['indicator_name'],
                rec['category'], rec['unit']
            )
            rows.append((
                iso3, country_ids[iso3], None,
                ind_id, int(rec['year']), float(rec['value'])
            ))
        conn.executemany(
            'INSERT INTO fact_indicators(iso3,country_id,city_id,indicator_id,year,value) VALUES(?,?,?,?,?,?)',
            rows
        )
        conn.commit()
        print(f"  {fname}: {len(rows)} rows inserted")
        total += len(rows)
    return total

def load_city_facts(conn):
    """Load NASA city-level climate facts."""
    path = RAW_DIR / 'nasa_climate.csv'
    if not path.exists():
        print("  Skipping missing nasa_climate.csv")
        return 0
    df = pd.read_csv(path)
    country_ids = {r[0]: r[1] for r in conn.execute('SELECT iso3, country_id FROM dim_country').fetchall()}
    city_ids    = {r[0]: r[1] for r in conn.execute('SELECT city_name, city_id FROM dim_city').fetchall()}
    rows = []
    for _, rec in df.iterrows():
        iso3 = str(rec['iso3'])
        city = str(rec['city'])
        if iso3 not in country_ids or city not in city_ids: continue
        ind_id = get_or_create_indicator(
            conn, rec['indicator_code'], rec['indicator_name'],
            rec['category'], rec['unit']
        )
        rows.append((
            iso3, country_ids[iso3], city_ids[city],
            ind_id, int(rec['year']), float(rec['value'])
        ))
    conn.executemany(
        'INSERT INTO fact_indicators(iso3,country_id,city_id,indicator_id,year,value) VALUES(?,?,?,?,?,?)',
        rows
    )
    conn.commit()
    print(f"  nasa_climate.csv: {len(rows)} rows inserted")
    return len(rows)

def aggregate_nasa_country(conn):
    """Aggregate NASA city-level T2M to country-level annual mean.

    Only temperature (T2M) is aggregated — precipitation and humidity are too
    spatially variable to produce a meaningful country-level signal from a small
    set of cities. The averaged T2M series is stored as a country-level indicator
    (city_id = NULL) so the correlation pipeline can pair it with WB/IMF data.
    """
    path = RAW_DIR / 'nasa_climate.csv'
    if not path.exists():
        print("  Skipping NASA aggregation — nasa_climate.csv not found")
        return 0
    df = pd.read_csv(path)
    t2m = df[df['indicator_code'] == 'NASA_T2M'].copy()
    if t2m.empty:
        print("  No NASA T2M rows found — skipping country aggregation")
        return 0
    # Mean across all cities per country per year
    agg = t2m.groupby(['iso3', 'year'])['value'].mean().reset_index()
    country_ids = {r[0]: r[1] for r in conn.execute(
        'SELECT iso3, country_id FROM dim_country').fetchall()}
    ind_id = get_or_create_indicator(
        conn,
        'NASA_T2M_COUNTRY',
        'Mean surface temperature (°C)',
        'Climate',
        '°C',
    )
    rows = []
    for _, rec in agg.iterrows():
        iso3 = str(rec['iso3'])
        if iso3 not in country_ids:
            continue
        rows.append((
            iso3, country_ids[iso3], None,
            ind_id, int(rec['year']), float(rec['value'])
        ))
    conn.executemany(
        'INSERT INTO fact_indicators'
        '(iso3,country_id,city_id,indicator_id,year,value) VALUES(?,?,?,?,?,?)',
        rows
    )
    conn.commit()
    n_countries = agg['iso3'].nunique()
    print(f"  nasa_climate.csv -> country T2M: {len(rows)} rows "
          f"({n_countries} countries, {len(rows)//max(n_countries,1)} years avg)")
    return len(rows)


if __name__ == '__main__':
    print("="*55)
    print("Global Monitor — Load")
    print("="*55)
    conn = sqlite3.connect(DB_PATH)
    create_schema(conn)
    load_dimensions(conn)
    n1 = load_country_facts(conn)
    n2 = load_city_facts(conn)
    n3 = aggregate_nasa_country(conn)
    total = conn.execute('SELECT COUNT(*) FROM fact_indicators').fetchone()[0]
    print(f"\n  Total fact rows in DB: {total}")
    conn.close()
    print("Load complete. Run build_data.py next.")
