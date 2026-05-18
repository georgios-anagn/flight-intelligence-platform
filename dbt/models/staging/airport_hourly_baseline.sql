SELECT
    airport_code,
    hour_of_day,
    day_of_week,
    AVG(landings_this_hour)    AS baseline_landings,
    STDDEV(landings_this_hour) AS stddev_landings,
    COUNT(*)                   AS sample_size
FROM {{ ref('flight_weather_events') }}
GROUP BY airport_code, hour_of_day, day_of_week