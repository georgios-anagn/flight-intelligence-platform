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

-- flights: landing events detected by the state-machine producer
CREATE TABLE IF NOT EXISTS flights ( 
    id SERIAL PRIMARY KEY,
    icao24 VARCHAR(10),
    callsign VARCHAR(10),
    dest_airport VARCHAR(4),
    lat DECIMAL(8,5),
    lon DECIMAL(8,5),
    altitude DECIMAL(8,2),
    velocity_kmh DECIMAL(6,1),
    vertical_rate DECIMAL(6,2),
    event_type VARCHAR(30),
    polled_at TIMESTAMP WITH TIME ZONE,
    ingested_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- weather_readings: raw data from Open-Meteo 
CREATE TABLE IF NOT EXISTS weather_readings ( 
    id SERIAL PRIMARY KEY,
    airport_code VARCHAR(4) REFERENCES airports(airport_code),
    recorded_at TIMESTAMP WITH TIME ZONE,
    temperature_c DECIMAL(5,2),
    wind_speed_kmh DECIMAL(7,2),
    wind_gust_kmh DECIMAL(7,2),
    precipitation_mm DECIMAL(6,2),
    visibility_km DECIMAL(7,2),
    cloud_cover_pct INTEGER,
    pressure_hpa DECIMAL(7,2),
    weather_code INTEGER,
    CONSTRAINT uq_weather_airport_time UNIQUE (airport_code, recorded_at)
); 

--seed airports table
INSERT INTO airports VALUES 
('LSZH','Zurich Airport','Zurich','Switzerland',47.46417,8.54917,'Europe/Zurich'), 
('EGLL','London Heathrow','London','United Kingdom',51.47750,-0.46139,'Europe/London'),
('LFPG','Paris CDG','Paris','France',49.00972,2.54778,'Europe/Paris'),
('EHAM','Amsterdam Schiphol','Amsterdam','Netherlands',52.30806,4.76417,'Europe/Amsterdam'), 
('EDDF','Frankfurt Airport','Frankfurt','Germany',50.03333,8.57056,'Europe/Frankfurt'), 
('LEMD','Madrid Barajas','Madrid','Spain',40.49361,-3.56639,'Europe/Madrid'),
('KJFK','New York JFK','New York','United States',40.63972,-73.77889,'America/New_York'),
('KORD','Chicago O Hare','Chicago','United States',41.97861,-87.90472,'America/Chicago'),
('KLAX','Los Angeles LAX','Los Angeles','United States',33.94250,-118.40806,'America/Los_Angeles'),
('CYYZ','Toronto Pearson','Toronto','Canada',43.67722,-79.63056,'America/Toronto')
ON CONFLICT DO NOTHING;