
# Flight Intelligence Platform

A real-time, end-to-end data engineering system that monitors global air traffic at 10 major airports and correlates live landing events with simultaneous weather conditions. Built as a portfolio project demonstrating the full modern data stack - from raw API ingestion through streaming, storage, transformation, machine learning, and live visualisation - running entirely on a local PC at zero cost.

## What This Platform Does

The system polls the OpenSky Network states/all API every 60 seconds, processing the live positions of every aircraft currently in the air worldwide. Using a state-machine approach, it identifies the moment a commercial flight transitions from airborne approach to touchdown at one of 10 monitored airports - from Zurich and London to Los Angeles and Toronto. Simultaneously, the Open-Meteo API records current weather at each airport at the same timestamp.

These two streams - landing events and weather readings - are published to Apache Kafka topics, consumed into a PostgreSQL database, transformed and joined by dbt into an analytical model, and processed by Apache Spark for feature engineering. Machine learning models then predict airport disruption - whether an airport will see significantly fewer landings than its historical baseline - based on weather conditions, time of day, and day of week. A Streamlit dashboard serves live results including a world map, per-airport statistics, and real-time predictions.

**Core analytical question:** Does weather at an airport cause measurable disruption to landing rates - and which airports are most vulnerable?

## Architecture

```
OpenSky Network (states/all)          Open-Meteo (weather)
         │                                     │
         ▼                                     ▼
   Kafka: flights topic              Kafka: weather topic
         │                                     │
         ▼                                     ▼
   PostgreSQL: flights table    PostgreSQL: weather_readings table
                    │                          │
                    └──────────┬───────────────┘
                               ▼
                     dbt: flight_weather_events
                               │
                               ▼
                    Apache Spark: feature engineering
                    (enriched_flights + Parquet lake)
                               │
                               ▼
                    ML Models: disruption classifier
                               + deviation regressor
                               │
                               ▼
                    Streamlit: live dashboard
```

Batch orchestration of dbt runs, Spark jobs and ML models is handled by **Apache Airflow**, running on a daily and weekly schedule respectively.

## Monitored Airports

| Code | Airport | City | Why Selected |
|------|---------|------|--------------|
| LSZH | Zurich Airport | Zurich, CH | Local Swiss relevance for DACH employers |
| EGLL | London Heathrow | London, UK | London fog - historically high weather sensitivity |
| LFPG | Paris CDG | Paris, FR | Europe's second busiest hub |
| EHAM | Amsterdam Schiphol | Amsterdam, NL | Major European hub, Polderbaan runway |
| EDDF | Frankfurt Airport | Frankfurt, DE | Largest German hub |
| LEMD | Madrid Barajas | Madrid, ES | High elevation (610m) - interesting weather dynamics |
| KJFK | New York JFK | New York, US | North Atlantic gateway, frequent storms |
| KORD | Chicago O'Hare | Chicago, US | Notorious for winter weather delays |
| KLAX | Los Angeles LAX | Los Angeles, US | West coast hub |
| CYYZ | Toronto Pearson | Toronto, CA | Severe Canadian winters |

## Tech Stack

| Tool | Role | Free Option |
|------|------|-------------|
| Python | Core language throughout | Free |
| Apache Kafka | Real-time streaming (flights + weather topics) | Docker / Confluent free tier |
| PostgreSQL | Primary relational database | Docker |
| dbt Core | SQL transformations and joined analytical models | Open source |
| Apache Airflow | Batch orchestration and scheduling | Docker |
| Apache Spark (PySpark) | Batch feature engineering on Parquet lake | Local mode via pip |
| scikit-learn | ML model training | Free |
| Streamlit | Live dashboard | Free / open source |
| Docker Compose | Local infrastructure orchestration | Free |
| GitHub | Version control and portfolio hosting | Free |

## Quick Start

### Prerequisites
- Docker Desktop (with at least 6GB RAM allocated)
- Miniconda or Python 3.11
- Java 11 (required for PySpark)
- Free OpenSky Network account with API credentials

### Setup

```bash
# Clone the repository
git clone https://github.com/YOUR_USERNAME/flight-intelligence-platform.git
cd flight-intelligence-platform

# Copy and fill in environment variables
cp .env.example .env
# Edit .env with your OpenSky credentials and PostgreSQL settings

# Start infrastructure (Kafka + PostgreSQL + Airflow)
docker compose up -d

# Create Python environment
conda env create -f environment.yml
conda activate flight-pipeline

# Run dbt transformations
cd dbt && dbt run --profiles-dir .
```

### Running the Pipeline

Open four separate terminals, all with `conda activate flight-pipeline`:

```bash
# Terminal 1 - Flight landing detection
python ingestion/flight_producer.py

# Terminal 2 - Weather data collection
python ingestion/weather_producer.py

# Terminal 3 - Write flights to PostgreSQL
python ingestion/flight_consumer.py

# Terminal 4 - Write weather to PostgreSQL
python ingestion/weather_consumer.py
```

After data accumulates (allow at least a few hours for live data):

```bash
# Run Spark feature engineering
python spark/feature_engineering.py

# Train ML models
python ml/train.py

# Launch dashboard
streamlit run dashboard/streamlit_app.py
```

Airflow runs automatically at http://localhost:8080 - dbt refreshes daily at 06:00 UTC, Spark feature engineering at 06:30 UTC, and ML retraining weekly on Mondays at 07:00 UTC.

## Data Collection - Methodology and Limitations

### How landing detection works

The OpenSky Network provides real-time ADS-B state vectors for every tracked aircraft globally via its `states/all` endpoint. This returns each aircraft's current position, altitude, velocity, vertical rate, and an `on_ground` transponder flag every 60 seconds.

Since the arrival endpoint is restricted for free accounts, landings are inferred using a **state-machine approach**:

1. An aircraft within a 50km radius and below 8,000m AGL is classified as `APPROACHING`
2. A landing event fires when the same aircraft transitions to `on_ground = True` in the next poll
3. A geometric fallback fires if the aircraft is within 4km, below 150m AGL, and still descending - catching cases where the transponder's `on_ground` flag is slow to update

Each airport has a defined bounding box to prevent misattribution between nearby airports, and a 10-minute cooldown per aircraft prevents duplicate events.

### Known limitations

**This is an inference system, not a verified arrival feed.** The following limitations are inherent to the approach and should be considered when interpreting results:

- **Duplicate detections:** The same physical landing can occasionally produce 2-3 events if the aircraft's transponder reports inconsistently between polls or if the callsign changes slightly. A database-level deduplication constraint (one event per callsign per airport per 30 minutes) mitigates this but does not eliminate it entirely.

- **Missed landings:** Aircraft that descend through the bounding box between polls - landing and taxiing away within a single 60-second window - may not be detected. This is more common at airports with thinner ADS-B receiver coverage, particularly outside Europe and North America.

- **No schedule data:** The pipeline captures actual landing times but has no access to scheduled arrival times. True delay calculation (actual minus scheduled) is therefore not possible with the current data sources. The ML models instead predict *disruption to baseline landing rates* rather than individual flight delays.

- **ADS-B coverage gaps:** OpenSky's coverage is denser over Europe and North America. Airports in Asia, the Middle East, and Australia may show lower detection rates than their actual traffic volumes. Consequently, for this first version of the platform only European and North American airports are selected.

- **Weather data resolution:** Open-Meteo updates its current weather readings every 15 minutes. Weather readings within the same 15-minute window are identical, creating minor redundancy in the database (handled by a unique constraint on airport + timestamp).

### Why this approach was chosen

The commercial flight schedule APIs that provide verified arrival data (FlightAware, AviationStack, OAG) are either expensive or have free tiers too restrictive for continuous polling. The OpenSky inference approach enables a fully functional, zero-cost pipeline that demonstrates all the intended data engineering concepts, with the trade-off of reduced data precision. In a production environment, this layer would be replaced by a verified schedule data feed.

## ML Models

Two models are trained on the `flight_weather_impact` table, which measures how much each airport-hour deviates from its historical baseline landing rate:

### Model 1: Disruption Classifier
- **Type:** Gradient Boosting Classifier
- **Target:** `is_disrupted` - did this airport-hour see 30% fewer landings than its historical average for that time slot?
- **Features:** Weather conditions (temperature, wind speed, precipitation, visibility, cloud cover, pressure), time features (hour of day, day of week, weekend flag), Spark-engineered binary flags (is_high_wind, is_low_visibility, is_heavy_rain), rolling 7-day landing average
- **Use case:** Real-time alert - is this airport currently being disrupted by weather?

### Model 2: Deviation Regressor
- **Type:** Random Forest Regressor
- **Target:** `landing_deviation` - how many landings above or below baseline is this airport-hour?
- **Features:** Same as classifier
- **Use case:** Quantify the magnitude of disruption, not just its presence

Feature importance analysis from both models reveals which weather factors most strongly predict disruption at each airport - a key analytical output of the platform.

## Repository Structure

```
flight-intelligence-platform/
├── README.md
├── docker-compose.yml
├── Dockerfile.airflow
├── environment.yml
├── .env.example
├── .gitignore
│
├── ingestion/
│   ├── flight_producer.py      ← State-machine landing detection
│   ├── weather_producer.py
│   ├── flight_consumer.py
│   ├── weather_consumer.py
│   ├── load_historical_flights.py       ← Bootstrap from OpenSky CSV samples
│   └── load_historical_weather.py
│
├── sql/
│   └── schema.sql
│   └── add_landing_bucket.sql
│
├── airflow/dags/
│   ├── dbt_daily_run.py
│   ├── spark_features.py
│   └── ml_train.py
│
├── dbt/
│   ├── dbt_project.yml
│   ├── profiles.yml.example
│   └── models/staging
│       ├── stg_flights.sql
│       ├── stg_weather.sql
│       ├── flight_weather_events.sql
│       ├── airport_hourly_baseline.sql
│       └── flight_weather_impact.sql
│
├── spark/
│   ├── feature_engineering.py
│   └── postgresql-42.7.3.jar
│
├── ml/
│   └── models/
│       ├── train.py
│       ├── disruption_classifier.pkl
│       ├── deviation_regressor.pkl
│       └── airport_encoder.pkl
│
├── dashboard/
│   └── streamlit_app.py

```

## Acknowledgements

- [OpenSky Network](https://opensky-network.org/) -- free real-time ADS-B flight data
- [Open-Meteo](https://open-meteo.com/) -- free historical and real-time weather API

