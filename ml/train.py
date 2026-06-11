import pandas as pd
import psycopg2
import os
import pickle
from dotenv import load_dotenv
from sklearn.ensemble import GradientBoostingClassifier, RandomForestRegressor
from sklearn.model_selection import train_test_split
from sklearn.metrics import classification_report, mean_absolute_error, r2_score
from sklearn.preprocessing import LabelEncoder

load_dotenv()

conn = psycopg2.connect(
    host=os.getenv('POSTGRES_HOST'),
    port=os.getenv('POSTGRES_PORT'),
    dbname=os.getenv('POSTGRES_DB'),
    user=os.getenv('POSTGRES_USER'),
    password=os.getenv('POSTGRES_PASSWORD')
)

# Read flight_weather_impact joined with Spark enriched features
df = pd.read_sql('''
    SELECT
        i.airport_code,
        i.landings_this_hour,
        i.landing_deviation,
        i.deviation_zscore,
        i.is_disrupted,
        i.baseline_landings,
        i.temperature_c,
        i.wind_speed_kmh,
        i.wind_gust_kmh,
        i.precipitation_mm,
        i.visibility_km,
        i.cloud_cover_pct,
        i.pressure_hpa,
        i.hour_of_day,
        i.day_of_week,
        i.is_weekend,
        -- Spark enriched features
        e.is_high_wind,
        e.is_low_visibility,
        e.is_heavy_rain,
        e.is_extreme_heat,
        e.is_freezing,
        e.rolling_avg_landings,
        e.polled_at   
    FROM flight_weather_impact i
    LEFT JOIN enriched_flights e
        ON  i.airport_code = e.dest_airport
        AND DATE_TRUNC(\'hour\', e.polled_at) = i.hour_bucket
    WHERE i.temperature_c IS NOT NULL
''', conn)

print(f'Loaded {len(df)} rows')
print(f'Disrupted hours: {df["is_disrupted"].sum()} ({df["is_disrupted"].mean()*100:.1f}%)')

# Encode airport as numeric
le = LabelEncoder()
df['airport_encoded'] = le.fit_transform(df['airport_code'])

features = [
    'airport_encoded',
    'temperature_c', 'wind_speed_kmh', 'wind_gust_kmh',
    'precipitation_mm', 'visibility_km', 'cloud_cover_pct',
    'pressure_hpa', 'hour_of_day', 'day_of_week', 'is_weekend',
    'is_high_wind', 'is_low_visibility', 'is_heavy_rain',
    'is_extreme_heat', 'is_freezing', 'rolling_avg_landings',
    'baseline_landings'
]

df = df.dropna(subset=features + ['is_disrupted', 'landing_deviation'])
df['is_weekend'] = df['is_weekend'].astype(int)
df['is_high_wind'] = df['is_high_wind'].fillna(0).astype(int)
df['is_low_visibility'] = df['is_low_visibility'].fillna(0).astype(int)
df['is_heavy_rain'] = df['is_heavy_rain'].fillna(0).astype(int)
df['is_extreme_heat'] = df['is_extreme_heat'].fillna(0).astype(int)
df['is_freezing'] = df['is_freezing'].fillna(0).astype(int)
df['rolling_avg_landings'] = df['rolling_avg_landings'].fillna(0)

# ── Train/Test data ───────────────────────────────────────────
# The question is "can the model predict the most recent unseen airport/weather conditions?" - therefore, we do not use random test-split but the latest days
# Ensure time order
df = df.sort_values('polled_at')

# Define time-based cutoff (last 1 day = test set)
cutoff = df['polled_at'].max() - pd.Timedelta(days=1)

train_df = df[df['polled_at'] <= cutoff]
test_df  = df[df['polled_at'] > cutoff]

# Features/targets
X_train = train_df[features]
X_test  = test_df[features]

yc_train = train_df['is_disrupted'].astype(int)
yc_test  = test_df['is_disrupted'].astype(int)

yr_train = train_df['landing_deviation']
yr_test  = test_df['landing_deviation']

# ── Model 1: Disruption classifier ───────────────────────────────────────────
print('\nTraining disruption classifier...')
clf = GradientBoostingClassifier(n_estimators=200, max_depth=4, random_state=42)
clf.fit(X_train, yc_train)
print(classification_report(yc_test, clf.predict(X_test)))

# Feature importance
importances = pd.Series(clf.feature_importances_, index=features)
print('\nTop features for disruption:')
print(importances.sort_values(ascending=False).head(8))

# ── Model 2: Deviation regressor ─────────────────────────────────────────────
print('\nTraining deviation regressor...')
reg = RandomForestRegressor(n_estimators=200, random_state=42)
reg.fit(X_train, yr_train)
preds = reg.predict(X_test)
print(f'MAE:  {mean_absolute_error(yr_test, preds):.2f} landings deviation')
print(f'R2:   {r2_score(yr_test, preds):.3f}')

# ── Save both models ─────────────────────────────────────────────────────────
# with open('ml/models/disruption_classifier.pkl', 'wb') as f:
#    pickle.dump(clf, f)
#with open('ml/models/deviation_regressor.pkl', 'wb') as f:
#    pickle.dump(reg, f)
#with open('ml/models/airport_encoder.pkl', 'wb') as f:
 #   pickle.dump(le, f)

MODEL_DIR = "/opt/airflow/ml/models"  # running inside Airflow environment
os.makedirs(MODEL_DIR, exist_ok=True)

with open(f"{MODEL_DIR}/disruption_classifier.pkl", "wb") as f:
    pickle.dump(clf, f)
with open(f"{MODEL_DIR}/deviation_regressor.pkl", 'wb') as f:
    pickle.dump(reg, f)
with open(f"{MODEL_DIR}/airport_encoder.pkl", 'wb') as f:
    pickle.dump(le, f)

print('\nAll models saved.')
conn.close()