-- Run these commands in psql or PgAdmin to include a column to be used for unique index (along with icao24, dest_airport) and duplicate detection
-- the index is used in Kafka consumer to create conflict and skip duplicated landing detections 

ALTER TABLE flights
ADD COLUMN landing_bucket TIMESTAMP;

-- values for the historical data
UPDATE flights
SET landing_bucket =
    date_trunc('hour', polled_at)
    + floor(extract(minute from polled_at) / 30) * interval '30 minutes';

ALTER TABLE flights
ADD CONSTRAINT uq_flight_landing
UNIQUE (icao24, dest_airport, landing_bucket);