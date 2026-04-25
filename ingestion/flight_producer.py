import time, json, requests, math, os
from kafka import KafkaProducer
from datetime import datetime, timedelta
from dotenv import load_dotenv

TOKEN_URL = "https://auth.opensky-network.org/auth/realms/opensky-network/protocol/openid-connect/token"
TOKEN_REFRESH_MARGIN = 30

load_dotenv()

# ---------------- OPENSKY TOKEN MANAGER ----------------
class TokenManager:
    def __init__(self):
        self.token = None
        self.expires_at = None

    def get_token(self):
        if self.token and self.expires_at and datetime.now() < self.expires_at:
            return self.token
        return self._refresh()

    def _refresh(self):
        r = requests.post(
            TOKEN_URL,
            data={
                "grant_type": "client_credentials",
                "client_id": os.getenv("OPENSKY_CLIENT_ID"),
                "client_secret": os.getenv("OPENSKY_CLIENT_SECRET"),
            },
        )
        r.raise_for_status()

        data = r.json()
        self.token = data["access_token"]
        expires_in = data.get("expires_in", 1800)
        self.expires_at = datetime.now() + timedelta(seconds=expires_in - TOKEN_REFRESH_MARGIN)

        return self.token

    def headers(self):
        return {"Authorization": f"Bearer {self.get_token()}"}


tokens = TokenManager()


# ---------------- AIRPORTS ----------------
AIRPORTS = [
    'LSZH','EGLL','LFPG','EHAM','EDDF','LEMD',
    'KJFK','KORD','KLAX','CYYZ'
]

AIRPORT_COORDS = {
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

AIRPORT_BOXES = {
    'LSZH': (47.42, 47.52, 8.51,  8.61),
    'EGLL': (51.44, 51.51, -0.50, -0.42),
    'LFPG': (48.97, 49.04,  2.51,  2.59),
    'EHAM': (52.25, 52.37,  4.68,  4.84),
    'EDDF': (49.99, 50.07,  8.53,  8.61),
    'LEMD': (40.41, 40.58, -3.65, -3.53),
    'KJFK': (40.60, 40.68, -73.84, -73.72),
    'KORD': (41.94, 42.02, -87.97, -87.84),
    'KLAX': (33.91, 33.98, -118.46, -118.35),
    'CYYZ': (43.65, 43.71,-79.66,-79.59)
}

AIRPORT_ELEVATION_M = {
    'LSZH': 432,
    'EGLL': 25,
    'LFPG': 119,
    'EHAM': -3,
    'EDDF': 111,
    'LEMD': 610,   # highest 
    'KJFK': 3,
    'KORD': 203,
    'KLAX': 38,
    'CYYZ': 173
}

# ---------------- KAFKA ----------------
producer = KafkaProducer(
    bootstrap_servers=os.getenv('KAFKA_BOOTSTRAP_SERVERS'),
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# ---------------- STATE TRACKING ----------------
aircraft_state = {}
STATE_RESET_TIME = 900  # 15 min cleanup


# ---------------- DISTANCE FUNCTION TO INFER ARRIVALS ----------------
def distance_km(lat1, lon1, lat2, lon2):
    R = 6371
    dlat = math.radians(lat2 - lat1)
    dlon = math.radians(lon2 - lon1)

    a = math.sin(dlat/2)**2 + math.cos(math.radians(lat1)) * math.cos(math.radians(lat2)) * math.sin(dlon/2)**2
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1-a))

    return R * c

# ---------------- FETCH STATES ----------------
def fetch_states():
    url = "https://opensky-network.org/api/states/all"

    r = requests.get(
        url,
        headers=tokens.headers(),
        timeout=10
    )

    if r.status_code != 200:
        print("ERROR:", r.status_code, r.text)
        return []
    
    data = r.json().get("states", [])

    return data

# ---------------- MAIN LOOP ----------------
def main():
    print("OpenSky producer started...")

    while True:
        states = fetch_states()
        print(f"Fetched {len(states)} aircraft states from OpenSky")

        if not states:
            print("No states returned, sleeping...")
            time.sleep(60)
            continue

        # Counters for this batch
        counts = {
            "skipped_no_callsign": 0,
            "skipped_slow": 0,
            "skipped_far": 0,
            "on_ground": 0,
            "departing": 0,
            "climbing": 0,
            "approaching": 0,
            "enroute": 0,
            "landings": 0,
        }

        for state in states:

            if not state or len(state) < 17:
                continue

            icao24 = state[0]
            callsign = (state[1] or "").strip()
            lon = state[5]
            lat = state[6]
            on_ground = state[8]
            altitude = state[13] if state[13] is not None else state[7]
            velocity = state[9]
            vertical_rate = state[11]

            if len(callsign) < 3:
                continue
            if lat is None or lon is None:
                continue
            if altitude is None or velocity is None :
                continue

            # convert velocity m/s -> km/h
            velocity_kmh = velocity * 3.6

             # Exclude helicopters and slow aircraft
            if velocity_kmh < 150 and not on_ground:
                counts["skipped_slow"] += 1
                continue

            # --- BOUNDING BOXES FOR AIRPORT
            airport = None
            for ap, (lat_min, lat_max, lon_min, lon_max) in AIRPORT_BOXES.items():
                if lat_min <= lat <= lat_max and lon_min <= lon <= lon_max:
                    airport = ap
                    break

            if airport is None:
                counts["skipped_far"] += 1
                continue   # not inside any monitored airport box — ignore

            dist = distance_km(lat, lon,
                            AIRPORT_COORDS[airport][0],
                            AIRPORT_COORDS[airport][1])

            key = (icao24, airport)
            prev = aircraft_state.get(key, {})
            prev_state = prev.get("state", "NONE")
            was_on_ground = prev.get("on_ground", False)

            elevation = AIRPORT_ELEVATION_M.get(airport, 0)
            altitude_agl = altitude - elevation if altitude is not None else None

            # ── Landing detection: airborne last poll, on ground this poll ──
            # --- Real Landing signal
            if on_ground:
                counts["on_ground"] += 1
                if (not was_on_ground and prev_state in ("APPROACHING", "NONE") and prev.get("last_seen") and time.time() - prev.get("last_seen", 0) < 300):
                    counts["landings"] += 1
                    event = {
                        "icao24": icao24,
                        "callsign": callsign,
                        "dest_airport": airport,
                        "lat": lat,
                        "lon": lon,
                        "altitude": altitude,
                        "velocity_kmh": round(velocity_kmh, 1),
                        "vertical_rate": vertical_rate,
                        "event_type": "landing_detected",
                        "polled_at": int(time.time())
                    }
                    producer.send("flights", value=event)
                    print(f"🛬 LANDING detected {callsign} -> {airport}")

                aircraft_state[key] = {
                    "state": "ON_GROUND",
                    "on_ground": True,
                    "last_seen": time.time()
                }
                continue
            
            # ── Skip fresh departures 
            if was_on_ground:
                counts["departing"] += 1
                aircraft_state[key] = {
                    "state": "DEPARTING",
                    "on_ground": False,
                    "last_seen": time.time()
                }
                continue

             # ── Skip climbing aircraft 
            if vertical_rate is not None and vertical_rate > 1:
                counts["climbing"] += 1
                aircraft_state[key] = {
                    "state": "CLIMBING",
                    "on_ground": False,
                    "last_seen": time.time()
                }
                continue
            
            # --- LANDING APPROXIMATION
            if (dist < 8 and
                altitude_agl is not None and
                altitude_agl < 150 and
                velocity_kmh < 320 and
                vertical_rate is not None and 
                vertical_rate < -0.5 and
                prev_state != "LANDED_GEO" and
                prev_state != "ON_GROUND"):
                counts["landings"] += 1
                event = {
                    "icao24": icao24,
                    "callsign": callsign,
                    "dest_airport": airport,
                    "lat": lat,
                    "lon": lon,
                    "altitude": altitude,
                    "velocity_kmh": round(velocity_kmh, 1),
                    "vertical_rate": vertical_rate,
                    "event_type": "landing_detected_geo",
                    "polled_at": int(time.time())
                }
                producer.send("flights", value=event)
                print(f"🛬 LANDING (geo) detected {callsign} -> {airport}")
                aircraft_state[key] = {
                    "state": "LANDED_GEO",
                    "on_ground": False,
                    "last_seen": time.time()
                }
                continue

            # ── Classify airborne aircraft near the airport ──
            is_approaching = (
                dist < 50 and
                altitude_agl is not None and
                altitude_agl < 8000 and
                prev_state != "LANDED_GEO"
            )

            new_state = "APPROACHING" if is_approaching else "ENROUTE"

            if new_state == "APPROACHING":
                counts["approaching"] += 1
                print(f"  ✈  APPROACHING  {callsign:10} -> {airport}  "
                      f"dist={dist:.1f}km  alt_agl={altitude_agl:.0f}m  "
                      f"spd={velocity_kmh:.0f}km/h  vrate={vertical_rate}")
            else:
                counts["enroute"] += 1

            aircraft_state[key] = {
                "state": new_state,
                "on_ground": False,
                "last_seen": time.time()
            }

        producer.flush()
        # Print batch summary
        print(f"\nBatch summary:")
        print(f"  Tracked near airports : {counts['approaching'] + counts['on_ground'] + counts['departing'] + counts['climbing']}")
        print(f"  Approaching           : {counts['approaching']}")
        print(f"  On ground             : {counts['on_ground']}")
        print(f"  Departing             : {counts['departing']}")
        print(f"  Climbing              : {counts['climbing']}")
        print(f"  Skipped (far)         : {counts['skipped_far']}")
        print(f"  Skipped (slow)        : {counts['skipped_slow']}")
        print(f"  Landings fired        : {counts['landings']}")
        print(f"  aircraft_state size   : {len(aircraft_state)}")
        print(f"Sleeping 60s...")

        #print(f"Batch complete — {len(states)} states processed. Sleeping 60s...")

        # Cleanup AFTER processing
        now = time.time()
        stale = {
            k: v for k, v in aircraft_state.items()
            if now - v["last_seen"] < STATE_RESET_TIME
        }
        aircraft_state.clear()
        aircraft_state.update(stale)

        time.sleep(60)

if __name__ == "__main__":
    main() 