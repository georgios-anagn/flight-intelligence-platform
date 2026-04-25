SELECT
    id,
    airport_code, 
    recorded_at, 
    temperature_c, 
    wind_speed_kmh, 
    wind_gust_kmh,
    precipitation_mm,
    COALESCE(visibility_km, 10) AS visibility_km, 
    cloud_cover_pct, 
    pressure_hpa, 
    weather_code
FROM weather_readings
