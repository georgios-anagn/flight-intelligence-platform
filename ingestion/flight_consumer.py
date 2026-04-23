import json, os
from kafka import KafkaConsumer
import psycopg2
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
        (flight_icao, origin_airport, dest_airport, actual_arr, delay_minutes, status)
        VALUES (%s, %s, %s, %s, %s, %s)
        ON CONFLICT DO NOTHING
"""

print('Flight consumer started...')
for message in consumer:
    f = message.value
    try:
        delay = f.get('arrivalDelay', 0) or 0
        status = 'delayed' if delay > 900 else 'on_time'
        cursor.execute(INSERT_SQL,  (
            f.get('callsign','').strip(),
            f.get('estDepartureAirport'),
            f.get('dest_airport'),
            f.get('lastSeen'),
            int(delay / 60),
            status
))
        conn.commit()
        print(f'Inserted flight: {f.get("callsign","?")}')
    except Exception as e:
        conn.rollback()
        print(f'Error inserting flight: {e}')