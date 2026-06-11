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
    .config("spark.hadoop.io.native.lib.available", "false") \
    .config("spark.hadoop.fs.file.impl", "org.apache.hadoop.fs.LocalFileSystem") \
    .config("spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version", "2") \
    .config("spark.hadoop.fs.file.impl.disable.cache", "true") \
    .config('spark.driver.memory', '2g') \
    .config(
    'spark.jars',
    '/opt/airflow/spark/postgresql-42.7.3.jar') \
    .getOrCreate()

spark.sparkContext.setLogLevel('WARN')

JDBC_URL = f"jdbc:postgresql://{os.getenv('POSTGRES_HOST_DOCKER', 'postgres')}:{os.getenv('POSTGRES_PORT', '5432')}/{os.getenv('POSTGRES_DB', 'flightdb')}"  # "postgres" as host and not "localhost" because spark runs in docker.
JDBC_PROPS = {
    "user":     os.getenv('POSTGRES_USER'),
    "password": os.getenv('POSTGRES_PASSWORD'),
    "driver":   "org.postgresql.Driver"
}

# ── Step 1: Read from PostgreSQL ─────────────────────────────────────────────
print("Reading flights from PostgreSQL...")
flights = spark.read.jdbc(JDBC_URL, "flights", properties=JDBC_PROPS)
print(f"Flights loaded: {flights.count()} rows")

print("Reading weather from PostgreSQL...")
weather = spark.read.jdbc(JDBC_URL, "weather_readings", properties=JDBC_PROPS)
print(f"Weather loaded: {weather.count()} rows")

# ── Step 2: Save raw snapshots to Parquet lake ───────────────────────────────
print("Saving raw snapshots to Parquet lake...")
flights.write.mode('overwrite').parquet('/opt/airflow/data/lake/flights/')
weather.write.mode('overwrite').parquet("/opt/airflow/data/lake/weather/")
print("Raw snapshots saved.")

# ── Step 3: Read back from Parquet for feature engineering ───────────────────
# Read from Parquet from this point forward — not PostgreSQL
flights = spark.read.parquet('/opt/airflow/data/lake/flights/')
weather = spark.read.parquet('/opt/airflow/data/lake/weather/')

# ── Step 4: Feature engineering on flights ───────────────────────────────────
print("Engineering flight features...")

# Cast polled_at to timestamp if it is not already
flights = flights.withColumn(
    'polled_at', F.col('polled_at').cast('timestamp')
).withColumn(
    "hour_bucket", F.date_trunc("hour", "polled_at")
)

flights_hourly = flights.groupBy(
    "dest_airport",
    "hour_bucket"
).agg(
    F.count("id").alias("landings_this_hour"),
    F.avg("velocity_kmh").alias("avg_velocity"),
    F.avg("altitude").alias("avg_altitude")
)

flights_with_hourly = flights.join(
    flights_hourly,
    on=["dest_airport", "hour_bucket"],
    how="left"
)

# Rolling 7-day average landings per airport
# rowsBetween(-7*24, 0) means 168 rows back — approximates 7 days at hourly data
window_7d = Window.partitionBy("dest_airport") \
    .orderBy(F.col("polled_at").cast("timestamp").cast("long")) \
    .rangeBetween(-7 * 24 * 3600, -1)

# average hourly landings in past 7 days
flights_enriched = flights_with_hourly \
    .withColumn('rolling_avg_landings', F.avg('landings_this_hour').over(window_7d)) \
    .withColumn('hour_of_day',  F.hour("hour_bucket")) \
    .withColumn('day_of_week',  F.dayofweek('hour_bucket')) \
    .withColumn('is_weekend',   F.when(F.dayofweek('hour_bucket').isin([1, 7]), 1).otherwise(0)) \
    .withColumn('month',        F.month('hour_bucket'))

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
        F.when(F.col('temperature_c') > 38, 1).otherwise(0)) \
    .withColumn('is_freezing',
        F.when(F.col('temperature_c') < 0, 1).otherwise(0))

weather_hourly = weather_enriched.groupBy(
    "airport_code",
    F.date_trunc("hour", "recorded_at").alias("hour_bucket")
).agg(
    F.avg("temperature_c").alias("temperature_c"),
    F.avg("wind_speed_kmh").alias("wind_speed_kmh"),
    F.avg("wind_gust_kmh").alias("wind_gust_kmh"),
    F.avg("precipitation_mm").alias("precipitation_mm"),
    F.avg("visibility_km").alias("visibility_km"),
    F.avg("cloud_cover_pct").alias("cloud_cover_pct"),
    F.avg("pressure_hpa").alias("pressure_hpa"),
    F.first("weather_code").alias("weather_code"),
    F.max("is_high_wind").alias("is_high_wind"),
    F.max("is_low_visibility").alias("is_low_visibility"),
    F.max("is_heavy_rain").alias("is_heavy_rain"),
    F.max("is_extreme_heat").alias("is_extreme_heat"),
    F.max("is_freezing").alias("is_freezing")
)

# ── Step 6: Join flights and weather ─────────────────────────────────────────
print("Joining flights with weather...")

print("FLIGHTS COLUMNS:", flights_enriched.columns)
flights_enriched.printSchema()

joined = flights_enriched.join(
    weather_hourly,
    on=[
        flights_enriched.dest_airport == weather_hourly.airport_code,
        flights_enriched.hour_bucket  == weather_hourly.hour_bucket
    ],
    how='left'
).select(
    flights_enriched.id,
    flights_enriched.icao24,
    flights_enriched.callsign,
    flights_enriched.dest_airport,
    flights_enriched.hour_bucket,
    flights_enriched.polled_at,
    flights_enriched.velocity_kmh,
    flights_enriched.altitude,
    flights_enriched.event_type,
    flights_enriched.rolling_avg_landings,
    flights_enriched.hour_of_day,
    flights_enriched.day_of_week,
    flights_enriched.is_weekend,
    flights_enriched.month,
    weather_hourly.temperature_c,
    weather_hourly.wind_speed_kmh,
    weather_hourly.wind_gust_kmh,
    weather_hourly.precipitation_mm,
    weather_hourly.visibility_km,
    weather_hourly.cloud_cover_pct,
    weather_hourly.pressure_hpa,
    weather_hourly.weather_code,
    weather_hourly.is_high_wind,
    weather_hourly.is_low_visibility,
    weather_hourly.is_heavy_rain,
    weather_hourly.is_extreme_heat,
    weather_hourly.is_freezing
)

print(f"Joined dataset: {joined.count()} rows")

# ── Step 7: Write enriched features to Parquet ───────────────────────────────
print("Writing enriched features to Parquet...")
joined.write.mode('overwrite').parquet('/opt/airflow/data/lake/enriched_features/')
print(" Enriched features saved.")

# ── Step 8: Write back to PostgreSQL for dbt and ML to use ───────────────────
# This creates an enriched_flights table in PostgreSQL with all Spark features
# dbt and the ML model can then read from this instead of raw tables
print("Writing enriched features back to PostgreSQL...")
joined.write \
    .jdbc(JDBC_URL, "enriched_flights",
          mode="overwrite",
          properties=JDBC_PROPS)
print("enriched_flights table written to PostgreSQL.")

print("\nFeature engineering complete.")
spark.stop()