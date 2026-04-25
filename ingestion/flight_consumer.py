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
    host=os.getenv('POSTGRES_HOST'),
    port=os.getenv('POSTGRES_PORT'),
    dbname=os.getenv('POSTGRES_DB'),
    user=os.getenv('POSTGRES_USER'),
    password=os.getenv('POSTGRES_PASSWORD')
    )

cursor = conn.cursor()

INSERT_SQL = """
    INSERT INTO flights
        (icao24, callsign, dest_airport, lat, lon, altitude, velocity_kmh, vertical_rate, event_type, polled_at)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
"""

print('Flight consumer started...')
for message in consumer:
    f = message.value
    try:
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
            datetime.fromtimestamp(f.get('polled_at'), tz=timezone.utc)
        ))
        conn.commit()
        print(f'Inserted flight: {f.get("callsign")} -> {f.get("dest_airport")}')
    except Exception as e:
        conn.rollback()
        print(f'Error inserting flight: {e}')