import time, json, requests 
from kafka import KafkaProducer 
from dotenv import load_dotenv 
import os

load_dotenv()

# Airports to monitor — ICAO codes matching your airports table 

AIRPORTS = ['LSZH','EGLL','LFPG','EHAM','EDDF','LEMD', 'LIRF','KJFK','KORD','KLAX','CYYZ','YMML','OMDB','WSSS','VHHH']

producer = KafkaProducer(
    bootstrap_servers=os.getenv('KAFKA_BOOTSTRAP_SERVERS'), 
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
    ) 

def fetch_flights(airport_icao) :
    """Fetch current arrivals at an airport from OpenSky.""" 
    url = 'https://opensky-network.org/api/flights/arrival' 
    params = {
        'airport': airport_icao,
        'begin': int(time.time()) - 7200, # last 2 hours
        'end': int(time.time()) 
        } 
    try:
        r = requests.get(url, params=params, timeout=10)
        if r.status_code == 200:
            return r.json()
        return []
    except Exception as e:
        print(f'Error fetching {airport_icao}: {e}')
        return []
    
def main():
    print('OpenSky producer started...')
    while True:
        for airport in AIRPORTS:
            flights = fetch_flights(airport)
            for flight in flights:
                flight['dest_airport'] = airport
                flight['polled_at'] = int(time.time())
                producer.send('flights', value=flight)
                print(f'Sent flight {flight.get("callsign","?")} -> {airport}')
        producer.flush()
        print(f'Batch complete. Sleeping 120s...')
        time.sleep(120)

if __name__ == '__main__':
    main()
