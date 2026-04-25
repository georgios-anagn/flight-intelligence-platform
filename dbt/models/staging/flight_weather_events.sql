SELECT 
    f.dest_airport AS airport_code,
    DATE_TRUNC('hour', f.polled_at)   AS hour_bucket,
    COUNT(*)   AS landings_this_hour,
    AVG(f.velocity_kmh)   AS avg_landing_speed,
    AVG(w.temperature_c)  AS temperature_c,
    AVG(w.wind_speed_kmh)  AS wind_speed_kmh,
    AVG(w.wind_gust_kmh)   AS wind_gust_kmh,
    AVG(w.precipitation_mm)  AS precipitation_mm,
    AVG(w.visibility_km)   AS visibility_km,
    AVG(w.cloud_cover_pct)   AS cloud_cover_pct, 
    AVG(w.pressure_hpa)    AS pressure_hpa,
    MAX(f.hour_of_day)    AS hour_of_day,
    MAX(f.day_of_week)   AS day_of_week, 
    BOOL_OR(f.is_weekend)  AS is_weekend
FROM {{ ref('stg_flights') }} f 
JOIN {{ ref('stg_weather') }} w 
    ON f.dest_airport = w.airport_code 
    AND w.recorded_at BETWEEN
        f.polled_at - INTERVAL '30 minutes' 
        AND f.polled_at + INTERVAL '30 minutes' 
GROUP BY
    f.dest_airport, 
    DATE_TRUNC('hour', f.polled_at)

     
        