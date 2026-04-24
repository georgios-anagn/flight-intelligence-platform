import time, json, requests 
from kafka import KafkaProducer 
from dotenv import load_dotenv 
import os

load_dotenv()

# Airport coordinates for Open-Meteo
AIRPORTS = {
    'LSZH': (47.46417, 8.54917),
    'EGLL': (51.47750, -0.46139),
    'LFPG': (49.00972, 2.54778),
    'EHAM': (52.30806, 4.76417),
    'EDDF': (50.03333, 8.57056),
    'LEMD': (40.49361, -3.56639),
    'LIRF': (41.80028,12.23889),
    'KJFK': (40.63972,-73.77889),
    'KORD': (41.97861,-87.90472),
    'KLAX': (33.94250,-118.40806),
    'CYYZ': (43.67722,79.63056),
    'YMML': (37.67333,144.84333),
    'OMDB': (25.25278, 55.36444),
    'WSSS': ( 1.35917,103.98917),
    'VHHH': (22.30806,113.91417)
}

producer = KafkaProducer(
    bootstrap_servers=os.getenv('KAFKA_BOOTSTRAP_SERVERS'),
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )

def fetch_weather(airport_code, lat, lon) :
    """Fetch current weather from Open-Meteo for given coordinates."""
    url = 'https://api.open-meteo.com/v1/forecast'
    params = {
        'latitude': lat,
        'longitude': lon,
        'current':  'temperature_2m,wind_speed_10m,wind_gusts_10m,'
                    'precipitation,visibility,cloud_cover,'
                    'surface_pressure,weather_code',
        'timezone': 'UTC'
        }
    try:
        r = requests.get(url, params=params, timeout=10)
        if r.status_code == 200:
            data = r.json()
            current = data['current']
            return {
                'airport_code': airport_code,
                'recorded_at': current['time'],
                'temperature_c': current['temperature_2m'],
                'wind_speed_kmh': current['wind_speed_10m'],
                'wind_gust_kmh': current['wind_gusts_10m'],
                'precipitation_mm': current['precipitation'],
                'visibility_km': current.get('visibility', None),
                'cloud_cover_pct': current['cloud_cover'],
                'pressure_hpa': current['surface_pressure'],
                'weather_code': current['weather_code'],
            }
    except Exception as e:
        print(f'Error fetching weather for {airport_code}: {e}')
    return None
    
def main():
    print('Weather producer started...')
    while True:
        for code, (lat, lon) in AIRPORTS.items():
            reading = fetch_weather(code, lat, lon)
            if reading:
                producer.send('weather', value=reading)
                print(f'Sent weather for {code}: {reading["temperature_c"]}C')
        producer.flush()
        print('Weather batch complete. Sleeping 120s...')
        time.sleep(120)

if __name__ == '__main__': 
    main()
