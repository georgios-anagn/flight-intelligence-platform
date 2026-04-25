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
    'LIRF','KJFK','KORD','KLAX','CYYZ','YMML',
    'OMDB','WSSS','VHHH'
]

AIRPORT_COORDS = {
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
    'CYYZ': (43.67722,-79.63056),
    'YMML': (37.67333,144.84333),
    'OMDB': (25.25278, 55.36444),
    'WSSS': ( 1.35917,103.98917),
    'VHHH': (22.30806,113.91417),
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

            # -------- find closest airport --------
            closest_airport = None
            closest_dist = float("inf")

            for airport, (alat, alon) in AIRPORT_COORDS.items():
                d = distance_km(lat, lon, alat, alon)
                if d < closest_dist:
                    closest_dist = d
                    closest_airport = airport

             # Ignore aircraft far from all monitored airports
            if closest_dist > 200:
                counts["skipped_far"] += 1
                continue

            airport = closest_airport
            dist = closest_dist
            key = (icao24, airport)
            prev = aircraft_state.get(key, {})
            prev_state = prev.get("state", "NONE")
            was_on_ground = prev.get("on_ground", False)

            # ── Landing detection: airborne last poll, on ground this poll ──
            if on_ground:
                counts["on_ground"] += 1
                if not was_on_ground and prev_state == "APPROACHING":
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

            # ── Classify airborne aircraft near the airport ──
            is_approaching = (
                dist < 50 and
                altitude < 6000 
            )

            new_state = "APPROACHING" if is_approaching else "ENROUTE"

            if new_state == "APPROACHING":
                counts["approaching"] += 1
                # Print every approaching aircraft so you can see the pipeline is working
                print(f"  ✈  APPROACHING  {callsign:10} -> {airport}  "
                      f"dist={dist:.1f}km  alt={altitude:.0f}m  "
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


            # # ------------- Update distance history ----------------
            # if key not in trajectory:
            #     trajectory[key] = []
            # trajectory[key].append(dist)

            # # keep only last 3 values
        #     if len(trajectory[key]) > MAX_HISTORY:
        #         trajectory[key].pop(0)

        #     prev_state = aircraft_state.get(key, {}).get("state", "NONE")

        #     # ---------------- FLIGHT ARRIVAL LOGIC ------------------------------

        #     is_arrival = (
        #         dist < 20 and
        #         altitude < 1500 and
        #         velocity_kmh < 200 and
        #         (vertical_rate is None or vertical_rate < 0) # and
        #         #is_getting_closer(trajectory[key])
        #     )

             

        #     new_state = "LANDED" if is_arrival else "APPROACHING" if is_approaching else "ENROUTE"

        #     # ---------------- STATE TRANSITION DETECTION ----------------
        #     if prev_state != "LANDED" and new_state == "LANDED":
        #             event = {
        #                 "icao24": icao24,
        #                 "callsign": callsign,
        #                 "dest_airport": airport,
        #                 "lat": lat,
        #                 "lon": lon,
        #                 "altitude": altitude,
        #                 "velocity_kmh": round(velocity_kmh, 1),
        #                 "vertical_rate": vertical_rate,
        #                 "on_ground": on_ground,
        #                 "event_type": "landing_detected",
        #                 "polled_at": int(time.time())
        #             }

        #             producer.send("flights", value=event)
        #             print(f"🛬 LANDING detected {callsign} -> {airport}")

        #     # ---------------- UPDATE STATE ----------------
        #     aircraft_state[key] = {
        #         "state": new_state,
        #         "on_ground": on_ground,
        #         "last_seen": time.time()
        #     }

        # producer.flush()
        # print(f"Batch complete — {len(states)} states processed. Sleeping 60s...")
        
        


