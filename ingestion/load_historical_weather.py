import requests
import psycopg2
import os
import time
from dotenv import load_dotenv

load_dotenv()

AIRPORTS = {
    'LSZH': (47.46417, 8.54917),
    'EGLL': (51.47750, -0.46139),
    'LFPG': (49.00972, 2.54778),
    'EHAM': (52.30806, 4.76417),
    'EDDF': (50.03333, 8.57056),
    'LEMD': (40.49361, -3.56639),
    'KJFK': (40.63972,-73.77889),
    'KORD': (41.97861,-87.90472),
    'KLAX': (33.94250,-118.40806),
    'CYYZ': (43.67722,-79.63056)
}

conn = psycopg2.connect(
    host=os.getenv('POSTGRES_HOST'),
    port=os.getenv('POSTGRES_PORT'),
    dbname=os.getenv('POSTGRES_DB'),
    user=os.getenv('POSTGRES_USER'),
    password=os.getenv('POSTGRES_PASSWORD')
)
cursor = conn.cursor()

INSERT_SQL = '''
    INSERT INTO weather_readings
        (airport_code, recorded_at, temperature_c, wind_speed_kmh,
         wind_gust_kmh, precipitation_mm, visibility_km,
         cloud_cover_pct, pressure_hpa, weather_code)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    ON CONFLICT (airport_code, recorded_at) DO NOTHING
'''

def fetch_historical_weather(airport_code, lat, lon):
    """Fetch hourly weather for September 2022 from Open-Meteo archive API."""
    url = 'https://archive-api.open-meteo.com/v1/archive'
    params = {
        'latitude':   lat,
        'longitude':  lon,
        'start_date': '2022-09-01',
        'end_date':   '2022-09-30',
        'hourly': (
            'temperature_2m,'
            'wind_speed_10m,'
            'wind_gusts_10m,'
            'precipitation,'
            'visibility,'
            'cloud_cover,'
            'surface_pressure,'
            'weather_code'
        ),
        'timezone': 'UTC'
    }

    try:
        r = requests.get(url, params=params, timeout=30)
        if r.status_code != 200:
            print(f'  Error {r.status_code} for {airport_code}: {r.text[:100]}')
            return None
        return r.json()
    except Exception as e:
        print(f'  Request failed for {airport_code}: {e}')
        return None


def insert_weather(airport_code, data):
    """Parse the hourly response and insert each hour into weather_readings."""
    hourly   = data.get('hourly', {})
    times    = hourly.get('time', [])
    temps    = hourly.get('temperature_2m', [])
    winds    = hourly.get('wind_speed_10m', [])
    gusts    = hourly.get('wind_gusts_10m', [])
    precip   = hourly.get('precipitation', [])
    vis      = hourly.get('visibility', [])
    cloud    = hourly.get('cloud_cover', [])
    pressure = hourly.get('surface_pressure', [])
    wcode    = hourly.get('weather_code', [])

    inserted = 0
    skipped  = 0

    for i, t in enumerate(times):
        try:
            # visibility comes in metres from archive API — convert to km
            vis_km = (vis[i] / 1000) if vis[i] is not None else None

            cursor.execute(INSERT_SQL, (
                airport_code,
                t,                          # ISO string e.g. "2022-09-01T00:00"
                temps[i],
                winds[i],
                gusts[i],
                precip[i],
                vis_km,
                cloud[i],
                pressure[i],
                wcode[i],
            ))
            inserted += 1
        except Exception as e:
            conn.rollback()
            skipped += 1
            continue

    conn.commit()
    return inserted, skipped


# ── Main ─────────────────────────────────────────────────────────────────────
print('Fetching historical weather for September 2022...\n')

total_inserted = 0
total_skipped  = 0

for airport_code, (lat, lon) in AIRPORTS.items():
    print(f'  Fetching {airport_code}...')
    data = fetch_historical_weather(airport_code, lat, lon)

    if data is None:
        print(f'  Skipping {airport_code} — no data returned')
        continue

    inserted, skipped = insert_weather(airport_code, data)
    total_inserted += inserted
    total_skipped  += skipped
    print(f'  {airport_code}: inserted={inserted}  skipped={skipped}')

    time.sleep(0.5)   # be polite to the API

cursor.close()
conn.close()
print(f'\nDone. Total inserted: {total_inserted}  Total skipped: {total_skipped}')