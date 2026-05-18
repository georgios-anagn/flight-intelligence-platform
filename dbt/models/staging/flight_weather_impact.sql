SELECT
    f.airport_code,
    f.hour_bucket,
    f.landings_this_hour,
    f.temperature_c,
    f.wind_speed_kmh,
    f.wind_gust_kmh,
    f.precipitation_mm,
    f.visibility_km,
    f.cloud_cover_pct,
    f.pressure_hpa,
    f.hour_of_day,
    f.day_of_week,
    f.is_weekend,
    b.baseline_landings,
    b.stddev_landings,

    f.landings_this_hour - b.baseline_landings          AS landing_deviation,
     -- normalised deviation in standard deviations
    CASE WHEN b.stddev_landings > 0
         THEN (f.landings_this_hour - b.baseline_landings) 
              / b.stddev_landings
         ELSE 0
    END                                                  AS deviation_zscore,
    -- binary: was this hour significantly disrupted?
    CASE WHEN f.landings_this_hour < b.baseline_landings * 0.7
         THEN 1 ELSE 0
    END                                                  AS is_disrupted
FROM {{ ref('flight_weather_events') }} f
JOIN {{ ref('airport_hourly_baseline') }} b
  ON  f.airport_code = b.airport_code
  AND f.hour_of_day  = b.hour_of_day
  AND f.day_of_week  = b.day_of_week
WHERE b.sample_size >= 3