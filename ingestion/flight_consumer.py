import json, os
from kafka import KafkaConsumer
import psycopg2
from datetime import datetime, timezone
from dotenv import load_dotenv

load_dotenv()

consumer = KafkaConsumer(
    'flights',
    bootstrap_servers=os.getenv('KAFKA_BOOTSTRAP_SERVERS'),
    value_deserializer=lambda m: json.loads(m.decode('utf-8')),
    auto_offset_reset='earliest',
    group_id='flight-consumer-group'
    )

conn = psycopg2.connect(
    host=os.getenv('POSTGRES_HOST_LOCAL'),
    port=os.getenv('POSTGRES_PORT'),
    dbname=os.getenv('POSTGRES_DB'),
    user=os.getenv('POSTGRES_USER'),
    password=os.getenv('POSTGRES_PASSWORD')
    )

cursor = conn.cursor()

INSERT_SQL = """
    INSERT INTO flights
        (icao24, callsign, dest_airport, lat, lon, altitude, velocity_kmh, vertical_rate, event_type, polled_at, landing_bucket)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        ON CONFLICT (icao24, dest_airport, landing_bucket) WHERE event_type IN ('landing_detected', 'landing_detected_geo') DO NOTHING
"""

print('Flight consumer started...')
for message in consumer:
    f = message.value
    try:
        polled_at = datetime.fromtimestamp(
            f.get('polled_at'),
            tz=timezone.utc
        )

        # using 30minute bucket instead of hour in order to capture the case of duplicate landing recognition at e.g. 10:55 and 11:10
        landing_bucket = polled_at.replace(   
            minute=(polled_at.minute // 30) * 30,
            second=0,
            microsecond=0
        )

        cursor.execute(INSERT_SQL,  (
            f.get('icao24'), 
            f.get('callsign'), 
            f.get('dest_airport'), 
            f.get('lat'), 
            f.get('lon'), 
            f.get('altitude'), 
            f.get('velocity_kmh'), 
            f.get('vertical_rate'), 
            f.get('event_type'), 
            polled_at,
            landing_bucket
        ))
        conn.commit()
        print(f'Inserted flight: {f.get("callsign")} -> {f.get("dest_airport")}')
    except UniqueViolation:
        conn.rollback()
        print(
            f'🚫 DUPLICATE LANDING IGNORED: '
            f'{f.get("callsign")} -> {f.get("dest_airport")} '
            f'({f.get("event_type")})'
        )

    except Exception as e:
        conn.rollback()
        print(f'Error inserting flight: {e}')