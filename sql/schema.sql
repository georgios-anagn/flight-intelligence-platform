-- airports: reference / dimension table
CREATE TABLE IF NOT EXISTS airports ( 
    airport_code VARCHAR(4) PRIMARY KEY,
    name VARCHAR(100) NOT NULL, 
    city VARCHAR(100),
    country VARCHAR(100),
    latitude DECIMAL(8,5),
    longitude DECIMAL(8,5),
    timezone VARCHAR(50)
); 

-- flights: raw data from OpenSky
CREATE TABLE IF NOT EXISTS flights ( 
    id SERIAL PRIMARY KEY,
    flight_icao VARCHAR(10),
    origin_airport VARCHAR(4) REFERENCES airports(airport_code),
    dest_airport VARCHAR(4) REFERENCES airports(airport_code),
    airline VARCHAR(100),
    scheduled_dep TIMESTAMP,
    actual_dep TIMESTAMP,
    scheduled_arr TIMESTAMP,
    actual_arr TIMESTAMP,
    delay_minutes INTEGER,
    status VARCHAR(20),
    ingested_at TIMESTAMP DEFAULT NOW()
); 

-- weather_readings: raw data from Open-Meteo 
CREATE TABLE IF NOT EXISTS weather_readings ( 
    id SERIAL PRIMARY KEY,
    airport_code VARCHAR(4) REFERENCES airports(airport_code),
    recorded_at TIMESTAMP,
    temperature_c DECIMAL(5,2),
    wind_speed_kmh DECIMAL(6,2),
    wind_gust_kmh DECIMAL(6,2),
    precipitation_mm DECIMAL(6,2),
    visibility_km DECIMAL(6,2),
    cloud_cover_pct INTEGER,
    pressure_hpa DECIMAL(7,2),
    weather_code INTEGER
); 

--seed airports table
INSERT INTO airports VALUES 
('LSZH','Zurich Airport','Zurich','Switzerland',47.46417,8.54917,'Europe/Zurich'), 
('EGLL','London Heathrow','London','United Kingdom',51.47750,0.46139,'Europe/London'),
('LFPG','Paris CDG','Paris','France',49.00972,2.54778,'Europe/Paris'),
('EHAM','Amsterdam Schiphol','Amsterdam','Netherlands',52.30806,4.76417,'Europe/Amsterdam'), 
('EDDF','Frankfurt Airport','Frankfurt','Germany',50.03333,8.57056,'Europe/Berlin'), 
('LEMD','Madrid Barajas','Madrid','Spain',40.49361,3.56639,'Europe/Madrid'),
('KJFK','New York JFK','New York','United States',40.63972,73.77889,'America/New_York'),
('KORD','Chicago O Hare','Chicago','United States',41.97861,87.90472,'America/Chicago'),
('KATL','Atlanta Hartsfield','Atlanta','United States',33.64028,84.42694,'America/New_York'),
('KLAX','Los Angeles LAX','Los Angeles','United States',33.94250,118.40806,'America/Los_Angeles'),
('OMDB','Dubai International','Dubai','UAE',25.25278,55.36444,'Asia/Dubai'), 
('WSSS','Singapore Changi','Singapore','Singapore',1.35917,103.98917,'Asia/Singapore'), 
('VHHH','Hong Kong Intl','Hong Kong','China',22.30806,113.91417,'Asia/Hong_Kong') 
ON CONFLICT DO NOTHING;