from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
import os
from dotenv import load_dotenv

load_dotenv()

# ── You need the PostgreSQL JDBC driver jar ──────────────────────────────────
# Download from: https://jdbc.postgresql.org/download/
# Save as: spark/postgresql-42.7.3.jar
# ─────────────────────────────────────────────────────────────────────────────

spark = SparkSession.builder \
    .appName('FlightFeatureEngineering') \
    .config('spark.driver.memory', '2g') \
    .config('spark.jars', 'spark/postgresql-42.7.3.jar') \
    .getOrCreate()

spark.sparkContext.setLogLevel('WARN')

JDBC_URL = f"jdbc:postgresql://{os.getenv('POSTGRES_HOST', 'localhost')}:{os.getenv('POSTGRES_PORT', '5432')}/{os.getenv('POSTGRES_DB', 'flightdb')}"
JDBC_PROPS = {
    "user":     os.getenv('POSTGRES_USER', 'flightuser'),
    "password": os.getenv('POSTGRES_PASSWORD', 'flightpass'),
    "driver":   "org.postgresql.Driver"
}

# ── Step 1: Read from PostgreSQL ─────────────────────────────────────────────
print("Reading flights from PostgreSQL...")
flights = spark.read.jdbc(JDBC_URL, "flights", properties=JDBC_PROPS)
print(f"  Flights loaded: {flights.count()} rows")

print("Reading weather from PostgreSQL...")
weather = spark.read.jdbc(JDBC_URL, "weather_readings", properties=JDBC_PROPS)
print(f"  Weather loaded: {weather.count()} rows")

# ── Step 2: Save raw snapshots to Parquet lake ───────────────────────────────
print("Saving raw snapshots to Parquet lake...")
flights.write.mode('overwrite').parquet('data/lake/flights/')
weather.write.mode('overwrite').parquet('data/lake/weather/')
print("  Raw snapshots saved.")

# ── Step 3: Read back from Parquet for feature engineering ───────────────────
# We read from Parquet from this point forward — not PostgreSQL
# This is intentional: Spark is designed to work on files, not live DB queries
flights = spark.read.parquet('data/lake/flights/')
weather = spark.read.parquet('data/lake/weather/')

# ── Step 4: Feature engineering on flights ───────────────────────────────────
print("Engineering flight features...")

# Cast polled_at to timestamp if it is not already
flights = flights.withColumn(
    'polled_at', F.col('polled_at').cast('timestamp')
)

# Rolling 7-day average landings per airport
# rowsBetween(-7*24, 0) means 168 rows back — approximates 7 days at hourly data
window_7d = Window.partitionBy('dest_airport') \
                  .orderBy(F.col('polled_at').cast('long')) \
                  .rowsBetween(-168, 0)

flights_enriched = flights \
    .withColumn('rolling_avg_landings', F.count('id').over(window_7d)) \
    .withColumn('hour_of_day',  F.hour('polled_at')) \
    .withColumn('day_of_week',  F.dayofweek('polled_at')) \
    .withColumn('is_weekend',
        F.when(F.dayofweek('polled_at').isin([1, 7]), 1).otherwise(0)) \
    .withColumn('month',        F.month('polled_at'))

# ── Step 5: Feature engineering on weather ───────────────────────────────────
print("Engineering weather features...")

weather = weather.withColumn(
    'recorded_at', F.col('recorded_at').cast('timestamp')
)

weather_enriched = weather \
    .withColumn('is_high_wind',
        F.when(F.col('wind_speed_kmh') > 50, 1).otherwise(0)) \
    .withColumn('is_low_visibility',
        F.when(F.col('visibility_km') < 3, 1).otherwise(0)) \
    .withColumn('is_heavy_rain',
        F.when(F.col('precipitation_mm') > 5, 1).otherwise(0)) \
    .withColumn('is_extreme_heat',
        F.when(F.col('temperature_c') > 40, 1).otherwise(0)) \
    .withColumn('is_freezing',
        F.when(F.col('temperature_c') < 0, 1).otherwise(0))

# ── Step 6: Join flights and weather ─────────────────────────────────────────
print("Joining flights with weather...")

# Round both timestamps to the nearest 15 minutes to align them
# (weather updates every 15 min, flights are event-based)
flights_rounded = flights_enriched.withColumn(
    'time_bucket',
    F.date_trunc('hour', F.col('polled_at'))
)

weather_rounded = weather_enriched.withColumn(
    'time_bucket',
    F.date_trunc('hour', F.col('recorded_at'))
)

joined = flights_rounded.join(
    weather_rounded,
    on=[
        flights_rounded.dest_airport == weather_rounded.airport_code,
        flights_rounded.time_bucket  == weather_rounded.time_bucket
    ],
    how='left'
).select(
    flights_rounded.id,
    flights_rounded.icao24,
    flights_rounded.callsign,
    flights_rounded.dest_airport,
    flights_rounded.polled_at,
    flights_rounded.velocity_kmh,
    flights_rounded.altitude,
    flights_rounded.event_type,
    flights_rounded.rolling_avg_landings,
    flights_rounded.hour_of_day,
    flights_rounded.day_of_week,
    flights_rounded.is_weekend,
    flights_rounded.month,
    weather_rounded.temperature_c,
    weather_rounded.wind_speed_kmh,
    weather_rounded.wind_gust_kmh,
    weather_rounded.precipitation_mm,
    weather_rounded.visibility_km,
    weather_rounded.cloud_cover_pct,
    weather_rounded.pressure_hpa,
    weather_rounded.weather_code,
    weather_rounded.is_high_wind,
    weather_rounded.is_low_visibility,
    weather_rounded.is_heavy_rain,
    weather_rounded.is_extreme_heat,
    weather_rounded.is_freezing
)

print(f"  Joined dataset: {joined.count()} rows")

# ── Step 7: Write enriched features to Parquet ───────────────────────────────
print("Writing enriched features to Parquet...")
joined.write.mode('overwrite').parquet('data/lake/enriched_features/')
print("  Enriched features saved.")

# ── Step 8: Write back to PostgreSQL for dbt and ML to use ───────────────────
# This creates an enriched_flights table in PostgreSQL with all Spark features
# dbt and the ML model can then read from this instead of raw tables
print("Writing enriched features back to PostgreSQL...")
joined.write \
    .jdbc(JDBC_URL, "enriched_flights",
          mode="overwrite",
          properties=JDBC_PROPS)
print("  enriched_flights table written to PostgreSQL.")

print("\nFeature engineering complete.")
spark.stop()