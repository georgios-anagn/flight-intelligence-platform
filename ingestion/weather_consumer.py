import json, os
from kafka import KafkaConsumer
import psycopg2
from dotenv import load_dotenv

load_dotenv()

consumer = KafkaConsumer(
    'weather',
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
    INSERT INTO weather_readings
    (airport_code, recorded_at, temperature_c, wind_speed_kmh, wind_gust_kmh, precipitation_mm, visibility_km, cloud_cover_pct, pressure_hpa, weather_code) 
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    ON CONFLICT (airport_code, recorded_at) DO NOTHING
"""

print('Weather consumer started...')
for message in consumer:
    w = message.value
    try:
        cursor.execute(INSERT_SQL, (
            w['airport_code'], w['recorded_at'],
            w['temperature_c'], w['wind_speed_kmh'], 
            w['wind_gust_kmh'], w['precipitation_mm'],
            w.get('visibility_km'), w['cloud_cover_pct'],
            w['pressure_hpa'], w['weather_code']
        ))
        conn.commit()
        print(f'Inserted weather: {w["airport_code"]} {w["temperature_c"]}C')
    except Exception as e:
        conn.rollback()
        print(f'Error inserting weather: {e}')