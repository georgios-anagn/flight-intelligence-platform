SELECT
    id,
    icao24, 
    callsign,
    dest_airport, 
    lat, lon, 
    altitude,
    velocity_kmh, 
    vertical_rate, 
    polled_at,
    EXTRACT(HOUR FROM polled_at) AS hour_of_day,
    EXTRACT(DOW FROM polled_at) AS day_of_week,
    CASE WHEN EXTRACT(DOW FROM polled_at) 
        IN (0,6) THEN true ELSE false END AS is_weekend,
    ingested_at 
FROM flights
WHERE dest_airport IS NOT NULL 
    AND callsign IS NOT NULL