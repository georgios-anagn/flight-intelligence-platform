import pandas as pd
import psycopg2
import glob
import os
from dotenv import load_dotenv

load_dotenv()

AIRPORTS = ['LSZH','EGLL','LFPG','EHAM','EDDF','LEMD',
    'KJFK','KORD','KLAX','CYYZ']

conn = psycopg2.connect(
    host=os.getenv('POSTGRES_HOST'),
    port=os.getenv('POSTGRES_PORT'),
    dbname=os.getenv('POSTGRES_DB'),
    user=os.getenv('POSTGRES_USER'),
    password=os.getenv('POSTGRES_PASSWORD')
)
cursor = conn.cursor()

INSERT_SQL = '''
    INSERT INTO flights
        (icao24, callsign, dest_airport, event_type, polled_at)
    VALUES (%s, %s, %s, %s, to_timestamp(%s))
    ON CONFLICT DO NOTHING
'''

files = sorted(glob.glob('data/lake/flight_sample_*.csv.gz'))
print(f'Found {len(files)} files\n')

total_inserted = 0
total_skipped  = 0

for fpath in files:
    fname = os.path.basename(fpath)
    try:
        df = pd.read_csv(
            fpath,
            compression='gzip',
            low_memory=False,
            encoding='utf-8',
            encoding_errors='ignore'   
        )
    except Exception as e:
        print(f'  ERROR reading {fname}: {e}')
        continue

    # filter to monitored airports
    df = df[df['estarrivalairport'].isin(AIRPORTS)]

    if df.empty:
        print(f'  {fname}: no matching airports, skipping')
        continue

    # use landingtime if available, fall back to lastseen
    df['arrival_ts'] = df['landingtime'].fillna(df['lastseen'])
    df = df.dropna(subset=['arrival_ts', 'estarrivalairport'])
    df['callsign'] = df['callsign'].str.strip()

    inserted = 0
    skipped  = 0

    for _, row in df.iterrows():
        try:
            cursor.execute(INSERT_SQL, (
                str(row.get('icao24', '')),
                str(row.get('callsign', '')),
                row['estarrivalairport'],
                'historical_load',
                float(row['arrival_ts'])
            ))
            inserted += 1
        except Exception as e:
            conn.rollback()
            skipped += 1
            continue

    conn.commit()
    total_inserted += inserted
    total_skipped  += skipped
    print(f'  {fname}: inserted={inserted}  skipped={skipped}')

cursor.close()
conn.close()
print(f'\nAll done. Total inserted: {total_inserted}  Total skipped: {total_skipped}')