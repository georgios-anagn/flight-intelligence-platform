# streamlit_app.py

import streamlit as st
import pandas as pd
import numpy as np
import pickle
import json
import os
import psycopg2
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from dotenv import load_dotenv
from sqlalchemy import create_engine
from sklearn.metrics import (
    confusion_matrix,
    mean_absolute_error,
    r2_score,
    roc_curve,
    auc,
    classification_report,
)
from sklearn.preprocessing import LabelEncoder

# ── Config ────────────────────────────────────────────────────────────────────
load_dotenv()

st.set_page_config(
    page_title="Flight Intelligence Platform",
    page_icon="✈️",
    layout="wide",
    initial_sidebar_state="expanded",
)

MODEL_DIR = "/opt/airflow/ml/models"

FEATURES = [
    "airport_encoded",
    "temperature_c",
    "wind_speed_kmh",
    "wind_gust_kmh",
    "precipitation_mm",
    "visibility_km",
    "cloud_cover_pct",
    "pressure_hpa",
    "hour_of_day",
    "day_of_week",
    "is_weekend",
    "is_high_wind",
    "is_low_visibility",
    "is_heavy_rain",
    "is_extreme_heat",
    "is_freezing",
    "rolling_avg_landings",
    "baseline_landings",
]

FEATURE_LABELS = {
    "airport_encoded":      "Airport (encoded)",
    "temperature_c":        "Temperature (°C)",
    "wind_speed_kmh":       "Wind Speed (km/h)",
    "wind_gust_kmh":        "Wind Gust (km/h)",
    "precipitation_mm":     "Precipitation (mm)",
    "visibility_km":        "Visibility (km)",
    "cloud_cover_pct":      "Cloud Cover (%)",
    "pressure_hpa":         "Pressure (hPa)",
    "hour_of_day":          "Hour of Day",
    "day_of_week":          "Day of Week",
    "is_weekend":           "Weekend",
    "is_high_wind":         "High Wind",
    "is_low_visibility":    "Low Visibility",
    "is_heavy_rain":        "Heavy Rain",
    "is_extreme_heat":      "Extreme Heat",
    "is_freezing":          "Freezing",
    "rolling_avg_landings": "Rolling Avg Landings (7d)",
    "baseline_landings":    "Baseline Landings",
}

PALETTE = {
    "primary": "#1f77b4",
    "danger":  "#d62728",
    "success": "#2ca02c",
    "warning": "#ff7f0e",
    "neutral": "#7f7f7f",
}

FLAG_COLS   = ["is_high_wind", "is_low_visibility", "is_heavy_rain", "is_extreme_heat", "is_freezing"]
FLAG_LABELS = ["💨 High Wind",  "🌫️ Low Vis.",       "🌧️ Heavy Rain", "🌡️ Extreme Heat",  "🧊 Freezing"]

WEATHER_NUMERIC = [
    "wind_speed_kmh", "wind_gust_kmh", "precipitation_mm",
    "visibility_km", "temperature_c", "pressure_hpa", "cloud_cover_pct",
]


# ══════════════════════════════════════════════════════════════════════════════
#  HELPERS
# ══════════════════════════════════════════════════════════════════════════════

@st.cache_resource
def get_engine():
    return create_engine(
        f"postgresql+psycopg2://"
        f"{os.getenv('POSTGRES_USER')}:{os.getenv('POSTGRES_PASSWORD')}"
        f"@{os.getenv('POSTGRES_HOST_LOCAL')}:{os.getenv('POSTGRES_PORT')}"
        f"/{os.getenv('POSTGRES_DB')}"
    )


@st.cache_data(ttl=300)
def load_main_dataframe():
    engine = get_engine()
    return pd.read_sql(
        """
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
            AND DATE_TRUNC('hour', e.polled_at) = i.hour_bucket
        WHERE i.temperature_c IS NOT NULL
        """,
        engine,
        parse_dates=["polled_at"],
    )


@st.cache_resource
def load_models():
    clf = reg = le = None
    try:
        with open(f"{MODEL_DIR}/disruption_classifier.pkl", "rb") as f:
            clf = pickle.load(f)
        with open(f"{MODEL_DIR}/deviation_regressor.pkl", "rb") as f:
            reg = pickle.load(f)
        with open(f"{MODEL_DIR}/airport_encoder.pkl", "rb") as f:
            le = pickle.load(f)
    except FileNotFoundError as exc:
        st.warning(f"Model file not found: {exc}")
    return clf, reg, le


@st.cache_data(ttl=3600)
def load_metadata():
    try:
        with open(f"{MODEL_DIR}/model_metadata.json") as f:
            return json.load(f)
    except FileNotFoundError:
        return {}


def prepare_ml_splits(df: pd.DataFrame):
    """Reproduce the exact train/test split from train.py."""
    df = df.copy()
    le_local = LabelEncoder()
    df["airport_encoded"] = le_local.fit_transform(df["airport_code"])
    for col in FLAG_COLS + ["is_weekend"]:
        df[col] = df[col].fillna(0).astype(int)
    df["rolling_avg_landings"] = df["rolling_avg_landings"].fillna(0)
    df = df.dropna(subset=FEATURES + ["is_disrupted", "landing_deviation"])
    df = df.sort_values("polled_at")
    cutoff = df["polled_at"].quantile(0.8)
    return df[df["polled_at"] <= cutoff], df[df["polled_at"] > cutoff], cutoff


def show(fig, height: int = None):
    """Render a Plotly figure at full width (new Streamlit API)."""
    if height:
        fig.update_layout(height=height)
    st.plotly_chart(fig, width="stretch")


# ══════════════════════════════════════════════════════════════════════════════
#  SIDEBAR
# ══════════════════════════════════════════════════════════════════════════════

st.sidebar.image("https://img.icons8.com/fluency/96/airplane-mode-on.png", width=64)
st.sidebar.title("Flight Intelligence Platform")
st.sidebar.markdown("---")

page = st.sidebar.radio(
    "Navigate",
    [
        "📊 Overview",
        "🌤️ Weather Impact",
        "✈️ Airport Analysis",
        "🎯 Model Performance",
        "🔮 Live Prediction",
    ],
)

st.sidebar.markdown("---")
st.sidebar.markdown("### Filters")

# ── Load data once ────────────────────────────────────────────────────────────
with st.spinner("Loading data…"):
    df_raw   = load_main_dataframe()
    clf, reg, le = load_models()
    metadata = load_metadata()

if df_raw.empty:
    st.error("No data returned from the database. Check your connection settings.")
    st.stop()

df_raw["polled_at"] = pd.to_datetime(df_raw["polled_at"])
train_df, test_df, cutoff_ts = prepare_ml_splits(df_raw)

all_airports = sorted(df_raw["airport_code"].unique())
selected_airports = st.sidebar.multiselect(
    "Airports", all_airports,
    default=all_airports[:5] if len(all_airports) >= 5 else all_airports,
)

min_date  = df_raw["polled_at"].min().date()
max_date  = df_raw["polled_at"].max().date()
date_range = st.sidebar.date_input(
    "Date Range", value=[min_date, max_date],
    min_value=min_date, max_value=max_date,
)

st.sidebar.markdown("---")
if metadata:
    st.sidebar.markdown("### 🏷️ Last Training")
    st.sidebar.caption(f"🕐 {metadata.get('training_time','N/A')[:19]} UTC")
    st.sidebar.caption(f"📋 {metadata.get('rows', 0):,} rows")
    st.sidebar.caption(f"⚡ {metadata.get('classifier_type','N/A')}")
    st.sidebar.caption(f"📈 {metadata.get('regressor_type','N/A')}")
    st.sidebar.caption(f"🚨 Disruption rate: {metadata.get('disruption_rate',0)*100:.1f}%")

# Apply filters
if len(date_range) == 2:
    df = df_raw[
        df_raw["airport_code"].isin(selected_airports)
        & (df_raw["polled_at"] >= pd.Timestamp(date_range[0]))
        & (df_raw["polled_at"] <= pd.Timestamp(date_range[1]) + pd.Timedelta(days=1))
    ]
else:
    df = df_raw[df_raw["airport_code"].isin(selected_airports)]


# ══════════════════════════════════════════════════════════════════════════════
#  PAGE 1 – OVERVIEW
# ══════════════════════════════════════════════════════════════════════════════

if page == "📊 Overview":
    st.title("📊 Flight Operations Overview")
    st.markdown(
        "High-level KPIs across all selected airports and the chosen date window. "
        "Data sourced from the joined `flight_weather_impact` × `enriched_flights` pipeline."
    )

    # ── KPIs ──────────────────────────────────────────────────────────────────
    k1, k2, k3, k4, k5, k6 = st.columns(6)
    k1.metric("✈️ Total Hour-Slots",    f"{len(df):,}")
    k2.metric("🚨 Disrupted Hours",     f"{int(df['is_disrupted'].sum()):,}")
    k3.metric("⚠️ Disruption Rate",     f"{df['is_disrupted'].mean()*100:.1f}%")
    k4.metric("📉 Avg Landing Dev.",    f"{df['landing_deviation'].mean():+.2f}")
    k5.metric("🛬 Avg Landings/Hour",   f"{df['landings_this_hour'].mean():.1f}")
    k6.metric("🏙️ Airports in View",    f"{df['airport_code'].nunique()}")

    st.markdown("---")

    # ── Time-series ───────────────────────────────────────────────────────────
    col_l, col_r = st.columns([3, 2])

    with col_l:
        st.subheader("Disruption Rate & Landings Over Time")
        ts = (
            df.set_index("polled_at")
            .resample("D")[["is_disrupted", "landings_this_hour"]]
            .agg({"is_disrupted": "mean", "landings_this_hour": "mean"})
            .reset_index()
        )
        ts["disruption_pct"] = ts["is_disrupted"] * 100

        fig_ts = make_subplots(specs=[[{"secondary_y": True}]])
        fig_ts.add_trace(
            go.Scatter(
                x=ts["polled_at"], y=ts["disruption_pct"],
                name="Disruption %",
                line=dict(color=PALETTE["danger"], width=2),
                fill="tozeroy", fillcolor="rgba(214,39,40,0.15)",
            ),
            secondary_y=False,
        )
        fig_ts.add_trace(
            go.Scatter(
                x=ts["polled_at"], y=ts["landings_this_hour"],
                name="Avg Landings/hr",
                line=dict(color=PALETTE["primary"], width=1.5, dash="dot"),
            ),
            secondary_y=True,
        )
        fig_ts.add_vline(
            x=cutoff_ts.timestamp() * 1000,
            line_dash="dash", line_color=PALETTE["warning"],
            annotation_text="Train/Test Split", annotation_position="top left",
        )
        fig_ts.update_layout(
            height=340, margin=dict(l=0, r=0, t=20, b=0),
            legend=dict(orientation="h", yanchor="bottom", y=1.02),
            yaxis_title="Disruption %", yaxis2_title="Avg Landings/hr",
            hovermode="x unified",
        )
        show(fig_ts)

    with col_r:
        st.subheader("Disruption Rate by Airport")
        apt = (
            df.groupby("airport_code")
            .agg(disruption_pct=("is_disrupted", lambda x: x.mean() * 100),
                 total_hours=("is_disrupted", "count"))
            .reset_index()
            .sort_values("disruption_pct", ascending=True)
        )
        fig_apt = px.bar(
            apt, x="disruption_pct", y="airport_code", orientation="h",
            color="disruption_pct",
            color_continuous_scale=["#2ca02c", "#ff7f0e", "#d62728"],
            labels={"disruption_pct": "Disruption %", "airport_code": "Airport"},
            text=apt["disruption_pct"].apply(lambda v: f"{v:.1f}%"),
        )
        fig_apt.update_layout(
            height=340, margin=dict(l=0, r=0, t=20, b=0),
            coloraxis_showscale=False,
        )
        fig_apt.update_traces(textposition="outside")
        show(fig_apt)

    st.markdown("---")

    # ── Heatmap + deviation histogram ─────────────────────────────────────────
    col_h1, col_h2 = st.columns(2)

    with col_h1:
        st.subheader("Disruption Rate – Hour × Day of Week")
        pivot = (
            df.groupby(["hour_of_day", "day_of_week"])["is_disrupted"]
            .mean()
            .unstack(fill_value=0) * 100
        )
        day_map = {1:"Sun", 2:"Mon", 3:"Tue", 4:"Wed", 5:"Thu", 6:"Fri", 7:"Sat"}
        pivot.columns = [day_map.get(c, str(c)) for c in pivot.columns]
        fig_hm = px.imshow(
            pivot,
            labels=dict(x="Day of Week", y="Hour of Day", color="Disruption %"),
            color_continuous_scale="Reds", aspect="auto", text_auto=".1f",
        )
        fig_hm.update_layout(height=380, margin=dict(l=0, r=0, t=20, b=0))
        show(fig_hm)

    with col_h2:
        st.subheader("Landing Deviation Distribution")
        fig_dev = px.histogram(
            df, x="landing_deviation", nbins=60,
            color_discrete_sequence=[PALETTE["primary"]],
            labels={"landing_deviation": "Landing Deviation"},
        )
        fig_dev.add_vline(x=0, line_dash="dash", line_color="white",
                          annotation_text="Baseline")
        fig_dev.add_vline(
            x=df["landing_deviation"].mean(), line_dash="dot",
            line_color=PALETTE["warning"],
            annotation_text=f"Mean={df['landing_deviation'].mean():+.2f}",
        )
        fig_dev.update_layout(height=380, margin=dict(l=0, r=0, t=20, b=0))
        show(fig_dev)

    # ── Z-score distribution ──────────────────────────────────────────────────
    st.subheader("Deviation Z-Score (Disruption Signal)")
    zdf = df.copy()
    zdf["Status"] = zdf["is_disrupted"].map({0: "Normal", 1: "Disrupted"})
    fig_z = px.histogram(
        zdf, x="deviation_zscore", color="Status", barmode="overlay",
        nbins=80, opacity=0.75,
        color_discrete_map={"Normal": PALETTE["success"], "Disrupted": PALETTE["danger"]},
        labels={"deviation_zscore": "Z-Score"},
    )
    fig_z.add_vline(x=-2, line_dash="dash", line_color="white", annotation_text="-2σ")
    fig_z.add_vline(x= 2, line_dash="dash", line_color="white", annotation_text="+2σ")
    fig_z.update_layout(height=300, margin=dict(l=0, r=0, t=20, b=0))
    show(fig_z)


# ══════════════════════════════════════════════════════════════════════════════
#  PAGE 2 – WEATHER IMPACT
# ══════════════════════════════════════════════════════════════════════════════

elif page == "🌤️ Weather Impact":
    st.title("🌤️ Weather Impact Analysis")
    st.markdown(
        "Explore how Spark-engineered weather features correlate with "
        "disruptions and landing deviations."
    )

    # ── Weather flag KPIs ─────────────────────────────────────────────────────
    w_cols = st.columns(5)
    for col, flag, label in zip(w_cols, FLAG_COLS, FLAG_LABELS):
        col.metric(label, f"{df[flag].fillna(0).mean()*100:.1f}%")

    st.markdown("---")

    # ── Disruption rate & deviation by flag ───────────────────────────────────
    col_a, col_b = st.columns(2)

    with col_a:
        st.subheader("Disruption Rate by Weather Condition")
        rows = []
        for flag in FLAG_COLS:
            label = flag.replace("is_", "").replace("_", " ").title()
            rows.append({
                "Condition": label,
                "Active":   df[df[flag].fillna(0) == 1]["is_disrupted"].mean() * 100,
                "Inactive": df[df[flag].fillna(0) == 0]["is_disrupted"].mean() * 100,
            })
        fd = pd.DataFrame(rows)
        fig_fd = go.Figure([
            go.Bar(name="Condition Active",   x=fd["Condition"], y=fd["Active"],
                   marker_color=PALETTE["danger"]),
            go.Bar(name="Condition Inactive", x=fd["Condition"], y=fd["Inactive"],
                   marker_color=PALETTE["success"]),
        ])
        fig_fd.update_layout(
            barmode="group", height=340, margin=dict(l=0, r=0, t=20, b=0),
            yaxis_title="Disruption Rate (%)",
            legend=dict(orientation="h", yanchor="bottom", y=1.02),
        )
        show(fig_fd)

    with col_b:
        st.subheader("Mean Landing Deviation by Weather Condition")
        rows2 = []
        for flag in FLAG_COLS:
            label = flag.replace("is_", "").replace("_", " ").title()
            rows2.append({
                "Condition": label,
                "Active":   df[df[flag].fillna(0) == 1]["landing_deviation"].mean(),
                "Inactive": df[df[flag].fillna(0) == 0]["landing_deviation"].mean(),
            })
        fdev = pd.DataFrame(rows2)
        fig_fdev = go.Figure([
            go.Bar(name="Active",   x=fdev["Condition"], y=fdev["Active"],
                   marker_color=PALETTE["warning"]),
            go.Bar(name="Inactive", x=fdev["Condition"], y=fdev["Inactive"],
                   marker_color=PALETTE["primary"]),
        ])
        fig_fdev.update_layout(
            barmode="group", height=340, margin=dict(l=0, r=0, t=20, b=0),
            yaxis_title="Mean Landing Deviation",
            legend=dict(orientation="h", yanchor="bottom", y=1.02),
        )
        show(fig_fdev)

    st.markdown("---")

    # ── Scatter: weather var vs. deviation (manual OLS, no statsmodels) ───────
    st.subheader("Weather Variable vs. Landing Deviation")
    sel_weather = st.selectbox(
        "Select weather variable", WEATHER_NUMERIC,
        format_func=lambda x: FEATURE_LABELS.get(x, x),
    )

    sample_df = df.sample(min(3000, len(df)), random_state=42).copy()
    sample_df["Status"] = sample_df["is_disrupted"].map({0: "Normal", 1: "Disrupted"})

    # Manual linear trendline (no statsmodels needed)
    valid = sample_df[[sel_weather, "landing_deviation"]].dropna()
    m, b  = np.polyfit(valid[sel_weather], valid["landing_deviation"], 1)
    x_line = np.linspace(valid[sel_weather].min(), valid[sel_weather].max(), 100)

    fig_sc = px.scatter(
        sample_df, x=sel_weather, y="landing_deviation", color="Status",
        color_discrete_map={"Normal": PALETTE["success"], "Disrupted": PALETTE["danger"]},
        opacity=0.5,
        labels={
            sel_weather: FEATURE_LABELS.get(sel_weather, sel_weather),
            "landing_deviation": "Landing Deviation",
        },
        hover_data=["airport_code"],
    )
    fig_sc.add_trace(go.Scatter(
        x=x_line, y=m * x_line + b, mode="lines",
        name=f"OLS trend (slope={m:.3f})",
        line=dict(color="white", width=2, dash="dash"),
    ))
    fig_sc.update_layout(height=400, margin=dict(l=0, r=0, t=20, b=0))
    show(fig_sc)

    st.markdown("---")

    # ── Correlation heatmap ───────────────────────────────────────────────────
    st.subheader("Feature Correlation Matrix")
    num_cols = [
        "temperature_c", "wind_speed_kmh", "wind_gust_kmh", "precipitation_mm",
        "visibility_km", "cloud_cover_pct", "pressure_hpa",
        "landing_deviation", "is_disrupted", "landings_this_hour",
    ]
    corr = df[num_cols].corr()
    fig_corr = px.imshow(
        corr, text_auto=".2f", color_continuous_scale="RdBu_r",
        zmin=-1, zmax=1, aspect="auto",
    )
    fig_corr.update_layout(height=480, margin=dict(l=0, r=0, t=20, b=0))
    show(fig_corr)

    st.markdown("---")

    # ── Box: weather var by disruption status ─────────────────────────────────
    st.subheader("Weather Distribution: Disrupted vs. Normal Hours")
    box_var = st.selectbox(
        "Variable to compare", WEATHER_NUMERIC, key="box_var",
        format_func=lambda x: FEATURE_LABELS.get(x, x),
    )
    box_df = df.copy()
    box_df["Status"] = box_df["is_disrupted"].map({0: "Normal", 1: "Disrupted"})
    fig_box = px.box(
        box_df, x="Status", y=box_var, color="Status",
        color_discrete_map={"Normal": PALETTE["success"], "Disrupted": PALETTE["danger"]},
        labels={"Status": "Status", box_var: FEATURE_LABELS.get(box_var, box_var)},
        points="outliers",
    )
    fig_box.update_layout(height=380, margin=dict(l=0, r=0, t=20, b=0), showlegend=False)
    show(fig_box)


# ══════════════════════════════════════════════════════════════════════════════
#  PAGE 3 – AIRPORT ANALYSIS
# ══════════════════════════════════════════════════════════════════════════════

elif page == "✈️ Airport Analysis":
    st.title("✈️ Airport-Level Analysis")

    airport_sel = st.selectbox("Select Airport", all_airports)
    adf = df_raw[df_raw["airport_code"] == airport_sel].copy()

    if adf.empty:
        st.warning("No data for the selected airport.")
        st.stop()

    # ── KPIs ──────────────────────────────────────────────────────────────────
    a1, a2, a3, a4, a5 = st.columns(5)
    a1.metric("Total Hour-Slots",     f"{len(adf):,}")
    a2.metric("Disrupted Hours",      f"{int(adf['is_disrupted'].sum()):,}")
    a3.metric("Disruption Rate",      f"{adf['is_disrupted'].mean()*100:.1f}%")
    a4.metric("Avg Landings/hr",      f"{adf['landings_this_hour'].mean():.1f}")
    a5.metric("Avg Landing Deviation",f"{adf['landing_deviation'].mean():+.2f}")

    st.markdown("---")

    # ── Daily landings + disruption rate ─────────────────────────────────────
    st.subheader(f"Daily Landings & Disruptions — {airport_sel}")
    adf_ts = (
        adf.set_index("polled_at")
        .resample("D")[["landings_this_hour", "is_disrupted", "landing_deviation"]]
        .agg({"landings_this_hour": "sum", "is_disrupted": "mean", "landing_deviation": "mean"})
        .reset_index()
    )
    fig_at = make_subplots(
        rows=2, cols=1, shared_xaxes=True, vertical_spacing=0.08,
        subplot_titles=("Daily Total Landings", "Daily Disruption Rate (%)"),
    )
    fig_at.add_trace(
        go.Bar(x=adf_ts["polled_at"], y=adf_ts["landings_this_hour"],
               name="Total Landings", marker_color=PALETTE["primary"]),
        row=1, col=1,
    )
    fig_at.add_trace(
        go.Scatter(
            x=adf_ts["polled_at"], y=adf_ts["is_disrupted"] * 100,
            name="Disruption %", line=dict(color=PALETTE["danger"], width=2),
            fill="tozeroy", fillcolor="rgba(214,39,40,0.15)",
        ),
        row=2, col=1,
    )
    fig_at.update_layout(height=480, margin=dict(l=0, r=0, t=40, b=0),
                         showlegend=False, hovermode="x unified")
    show(fig_at)

    st.markdown("---")

    # ── Hourly profile ────────────────────────────────────────────────────────
    col_p1, col_p2 = st.columns(2)

    with col_p1:
        st.subheader("Avg Landings by Hour of Day")
        hourly = adf.groupby("hour_of_day")["landings_this_hour"].mean().reset_index()
        fig_hr = px.bar(
            hourly, x="hour_of_day", y="landings_this_hour",
            color="landings_this_hour", color_continuous_scale="Blues",
            labels={"hour_of_day": "Hour", "landings_this_hour": "Avg Landings"},
        )
        fig_hr.update_layout(height=320, margin=dict(l=0, r=0, t=20, b=0),
                              coloraxis_showscale=False)
        show(fig_hr)

    with col_p2:
        st.subheader("Disruption Rate by Hour of Day")
        d_hr = adf.groupby("hour_of_day")["is_disrupted"].mean().reset_index()
        d_hr["disruption_pct"] = d_hr["is_disrupted"] * 100
        fig_dhr = px.bar(
            d_hr, x="hour_of_day", y="disruption_pct",
            color="disruption_pct",
            color_continuous_scale=["#2ca02c", "#ff7f0e", "#d62728"],
            labels={"hour_of_day": "Hour", "disruption_pct": "Disruption %"},
        )
        fig_dhr.update_layout(height=320, margin=dict(l=0, r=0, t=20, b=0),
                               coloraxis_showscale=False)
        show(fig_dhr)

    st.markdown("---")

    # ── Weather conditions at this airport ────────────────────────────────────
    st.subheader(f"Weather Summary — {airport_sel}")
    w1, w2, w3, w4, w5, w6, w7 = st.columns(7)
    w1.metric("🌡️ Avg Temp (°C)",      f"{adf['temperature_c'].mean():.1f}")
    w2.metric("💨 Avg Wind (km/h)",     f"{adf['wind_speed_kmh'].mean():.1f}")
    w3.metric("💨 Avg Gust (km/h)",     f"{adf['wind_gust_kmh'].mean():.1f}")
    w4.metric("🌧️ Avg Precip (mm)",     f"{adf['precipitation_mm'].mean():.2f}")
    w5.metric("👁️ Avg Visibility (km)", f"{adf['visibility_km'].mean():.1f}")
    w6.metric("☁️ Avg Cloud Cover (%)", f"{adf['cloud_cover_pct'].mean():.1f}")
    w7.metric("🔵 Avg Pressure (hPa)",  f"{adf['pressure_hpa'].mean():.1f}")

    st.markdown("---")

    # ── Weather flag occurrence rates for this airport ────────────────────────
    st.subheader(f"Adverse Weather Occurrence Rates — {airport_sel}")
    flag_data = []
    for flag, label in zip(FLAG_COLS, FLAG_LABELS):
        rate     = adf[flag].fillna(0).mean() * 100
        dis_rate = adf[adf[flag].fillna(0) == 1]["is_disrupted"].mean() * 100
        flag_data.append({
            "Condition":      label,
            "Occurrence (%)": round(rate, 1),
            "Disruption when active (%)": round(dis_rate, 1),
        })
    flag_summary_df = pd.DataFrame(flag_data)

    col_f1, col_f2 = st.columns(2)
    with col_f1:
        fig_occ = px.bar(
            flag_summary_df, x="Condition", y="Occurrence (%)",
            color="Occurrence (%)",
            color_continuous_scale=["#2ca02c", "#ff7f0e", "#d62728"],
            text=flag_summary_df["Occurrence (%)"].apply(lambda v: f"{v:.1f}%"),
            title="How often does each condition occur?",
        )
        fig_occ.update_layout(height=320, margin=dict(l=0, r=0, t=40, b=0),
                               coloraxis_showscale=False)
        fig_occ.update_traces(textposition="outside")
        show(fig_occ)

    with col_f2:
        fig_dis_flag = px.bar(
            flag_summary_df, x="Condition", y="Disruption when active (%)",
            color="Disruption when active (%)",
            color_continuous_scale=["#2ca02c", "#ff7f0e", "#d62728"],
            text=flag_summary_df["Disruption when active (%)"].apply(lambda v: f"{v:.1f}%"),
            title="Disruption rate when each condition is active",
        )
        fig_dis_flag.update_layout(height=320, margin=dict(l=0, r=0, t=40, b=0),
                                    coloraxis_showscale=False)
        fig_dis_flag.update_traces(textposition="outside")
        show(fig_dis_flag)

    st.markdown("---")

    # ── Rolling avg landings trend ────────────────────────────────────────────
    st.subheader(f"7-Day Rolling Avg Landings vs. Actual — {airport_sel}")
    roll_df = adf.set_index("polled_at").resample("D").agg(
        actual=("landings_this_hour", "mean"),
        rolling=("rolling_avg_landings", "mean"),
    ).reset_index()

    fig_roll = go.Figure()
    fig_roll.add_trace(go.Scatter(
        x=roll_df["polled_at"], y=roll_df["actual"],
        name="Actual Avg Landings/hr",
        line=dict(color=PALETTE["primary"], width=2),
    ))
    fig_roll.add_trace(go.Scatter(
        x=roll_df["polled_at"], y=roll_df["rolling"],
        name="7d Rolling Avg",
        line=dict(color=PALETTE["warning"], width=2, dash="dot"),
    ))
    fig_roll.update_layout(
        height=340, margin=dict(l=0, r=0, t=20, b=0),
        yaxis_title="Landings/hr", hovermode="x unified",
        legend=dict(orientation="h", yanchor="bottom", y=1.02),
    )
    show(fig_roll)


# ══════════════════════════════════════════════════════════════════════════════
#  PAGE 4 – MODEL PERFORMANCE
# ══════════════════════════════════════════════════════════════════════════════

elif page == "🎯 Model Performance":
    st.title("🎯 Model Performance")
    st.markdown(
        "Evaluating both models on the **held-out test set** (last 20% of data by time), "
        "mirroring the exact split used in `train.py`."
    )

    if clf is None or reg is None:
        st.error("Models not loaded. Ensure the model .pkl files exist in the model directory.")
        st.stop()

    # Prepare test features
    X_test  = test_df[FEATURES]
    yc_test = test_df["is_disrupted"].astype(int)
    yr_test = test_df["landing_deviation"]

    yc_pred      = clf.predict(X_test)
    yc_prob      = clf.predict_proba(X_test)[:, 1]
    yr_pred      = reg.predict(X_test)

    mae  = mean_absolute_error(yr_test, yr_pred)
    r2   = r2_score(yr_test, yr_pred)
    fpr, tpr, _ = roc_curve(yc_test, yc_prob)
    roc_auc      = auc(fpr, tpr)

    # ── Model KPIs ────────────────────────────────────────────────────────────
    st.subheader("Key Metrics — Test Set")
    m1, m2, m3, m4, m5 = st.columns(5)
    m1.metric("ROC-AUC (Classifier)",       f"{roc_auc:.3f}")
    m2.metric("MAE (Regressor)",             f"{mae:.2f} landings")
    m3.metric("R² (Regressor)",              f"{r2:.3f}")
    m4.metric("Test Set Size",               f"{len(X_test):,} rows")
    m5.metric("Train/Test Cutoff",           str(cutoff_ts)[:10])

    st.markdown("---")

    # ── Classification report ─────────────────────────────────────────────────
    st.subheader("Classification Report — Disruption Classifier")
    report_dict = classification_report(yc_test, yc_pred, output_dict=True)
    report_df   = pd.DataFrame(report_dict).T.round(3)
    st.dataframe(
        report_df.style.background_gradient(cmap="RdYlGn", subset=["precision", "recall", "f1-score"]),
        width="stretch",
    )

    st.markdown("---")

    # ── Confusion matrix + ROC curve ──────────────────────────────────────────
    col_cm, col_roc = st.columns(2)

    with col_cm:
        st.subheader("Confusion Matrix")
        cm = confusion_matrix(yc_test, yc_pred)
        fig_cm = px.imshow(
            cm,
            text_auto=True,
            color_continuous_scale="Blues",
            labels=dict(x="Predicted", y="Actual", color="Count"),
            x=["Not Disrupted", "Disrupted"],
            y=["Not Disrupted", "Disrupted"],
        )
        fig_cm.update_layout(height=380, margin=dict(l=0, r=0, t=20, b=0))
        show(fig_cm)

    with col_roc:
        st.subheader(f"ROC Curve (AUC = {roc_auc:.3f})")
        fig_roc = go.Figure()
        fig_roc.add_trace(go.Scatter(
            x=fpr, y=tpr, mode="lines",
            name=f"GBM Classifier (AUC={roc_auc:.3f})",
            line=dict(color=PALETTE["primary"], width=2),
        ))
        fig_roc.add_trace(go.Scatter(
            x=[0, 1], y=[0, 1], mode="lines",
            name="Random Baseline",
            line=dict(color=PALETTE["neutral"], dash="dash"),
        ))
        fig_roc.update_layout(
            height=380, margin=dict(l=0, r=0, t=20, b=0),
            xaxis_title="False Positive Rate",
            yaxis_title="True Positive Rate",
            legend=dict(orientation="h", yanchor="bottom", y=1.02),
        )
        show(fig_roc)

    st.markdown("---")

    # ── Feature importance (classifier) ──────────────────────────────────────
    col_fi1, col_fi2 = st.columns(2)

    with col_fi1:
        st.subheader("Feature Importance — Disruption Classifier")
        imp_clf = (
            pd.Series(clf.feature_importances_, index=FEATURES)
            .sort_values(ascending=True)
        )
        fig_fi = px.bar(
            x=imp_clf.values, y=imp_clf.index,
            orientation="h",
            color=imp_clf.values,
            color_continuous_scale="Blues",
            labels={"x": "Importance", "y": "Feature"},
        )
        fig_fi.update_layout(height=480, margin=dict(l=0, r=0, t=20, b=0),
                              coloraxis_showscale=False)
        fig_fi.update_layout(
            yaxis=dict(
                tickmode="array",
                tickvals=list(imp_clf.index),
                ticktext=[FEATURE_LABELS.get(f, f) for f in imp_clf.index],
            )
        )
        show(fig_fi)

    with col_fi2:
        st.subheader("Feature Importance — Deviation Regressor")
        imp_reg = (
            pd.Series(reg.feature_importances_, index=FEATURES)
            .sort_values(ascending=True)
        )
        fig_fi2 = px.bar(
            x=imp_reg.values, y=imp_reg.index,
            orientation="h",
            color=imp_reg.values,
            color_continuous_scale="Oranges",
            labels={"x": "Importance", "y": "Feature"},
        )
        fig_fi2.update_layout(height=480, margin=dict(l=0, r=0, t=20, b=0),
                               coloraxis_showscale=False)
        fig_fi2.update_layout(
            yaxis=dict(
                tickmode="array",
                tickvals=list(imp_reg.index),
                ticktext=[FEATURE_LABELS.get(f, f) for f in imp_reg.index],
            )
        )
        show(fig_fi2)

    st.markdown("---")

    # ── Regressor: actual vs. predicted ──────────────────────────────────────
    st.subheader("Deviation Regressor — Actual vs. Predicted")
    reg_df = pd.DataFrame({
        "Actual":    yr_test.values,
        "Predicted": yr_pred,
        "Airport":   test_df["airport_code"].values,
    }).sample(min(2000, len(yr_test)), random_state=42)

    # Manual trendline
    m_r, b_r  = np.polyfit(reg_df["Actual"], reg_df["Predicted"], 1)
    x_r = np.linspace(reg_df["Actual"].min(), reg_df["Actual"].max(), 100)

    fig_avp = px.scatter(
        reg_df, x="Actual", y="Predicted", color="Airport",
        opacity=0.5,
        labels={"Actual": "Actual Deviation", "Predicted": "Predicted Deviation"},
    )
    fig_avp.add_trace(go.Scatter(
        x=x_r, y=m_r * x_r + b_r, mode="lines",
        name="OLS fit", line=dict(color="white", dash="dash", width=2),
    ))
    fig_avp.add_trace(go.Scatter(
        x=x_r, y=x_r, mode="lines",
        name="Perfect fit", line=dict(color=PALETTE["success"], dash="dot", width=1.5),
    ))
    fig_avp.update_layout(height=440, margin=dict(l=0, r=0, t=20, b=0))
    show(fig_avp)

    st.markdown("---")

    # ── Residuals distribution ────────────────────────────────────────────────
    st.subheader("Residuals Distribution (Actual − Predicted)")
    residuals = yr_test.values - yr_pred
    fig_res = px.histogram(
        x=residuals, nbins=60,
        color_discrete_sequence=[PALETTE["primary"]],
        labels={"x": "Residual"},
    )
    fig_res.add_vline(x=0, line_dash="dash", line_color="white", annotation_text="Zero error")
    fig_res.add_vline(
        x=float(np.mean(residuals)), line_dash="dot",
        line_color=PALETTE["warning"],
        annotation_text=f"Mean={np.mean(residuals):+.3f}",
    )
    fig_res.update_layout(height=300, margin=dict(l=0, r=0, t=20, b=0))
    show(fig_res)


# ══════════════════════════════════════════════════════════════════════════════
#  PAGE 5 – LIVE PREDICTION
# ══════════════════════════════════════════════════════════════════════════════

elif page == "🔮 Live Prediction":
    st.title("🔮 Live Disruption Prediction")
    st.markdown(
        "Manually enter current weather and operational conditions to get a "
        "real-time disruption probability and expected landing deviation from the trained models."
    )

    if clf is None or reg is None or le is None:
        st.error("Models not loaded. Ensure all .pkl files exist in the model directory.")
        st.stop()

    # ── Input form ────────────────────────────────────────────────────────────
    with st.form("prediction_form"):
        st.subheader("✈️ Operational Context")
        fc1, fc2, fc3 = st.columns(3)
        with fc1:
            airport_input  = st.selectbox("Airport", all_airports)
            hour_of_day    = st.slider("Hour of Day", 0, 23, 12)
            day_of_week    = st.slider("Day of Week (1=Sun, 7=Sat)", 1, 7, 3)
        with fc2:
            is_weekend         = st.checkbox("Is Weekend?")
            baseline_landings  = st.number_input("Baseline Landings (historical avg)", 0.0, 100.0, 10.0, 0.5)
            rolling_avg        = st.number_input("7-Day Rolling Avg Landings", 0.0, 100.0, 10.0, 0.5)
        with fc3:
            st.markdown("&nbsp;")   # spacer

        st.subheader("🌤️ Weather Conditions")
        wc1, wc2, wc3, wc4 = st.columns(4)
        with wc1:
            temperature_c   = st.number_input("Temperature (°C)",    -30.0, 55.0,  20.0, 0.5)
            wind_speed_kmh  = st.number_input("Wind Speed (km/h)",      0.0, 200.0, 15.0, 1.0)
            wind_gust_kmh   = st.number_input("Wind Gust (km/h)",       0.0, 250.0, 20.0, 1.0)
        with wc2:
            precipitation_mm = st.number_input("Precipitation (mm)",   0.0,  50.0,  0.0, 0.1)
            visibility_km    = st.number_input("Visibility (km)",       0.0,  50.0, 10.0, 0.5)
        with wc3:
            cloud_cover_pct  = st.number_input("Cloud Cover (%)",       0.0, 100.0, 30.0, 1.0)
            pressure_hpa     = st.number_input("Pressure (hPa)",      900.0,1100.0,1013.0, 0.5)
        with wc4:
            st.markdown("**Derived flags** (auto-computed from thresholds in `feature_engineering.py`)")
            st.caption("Wind > 50 km/h → High Wind")
            st.caption("Visibility < 3 km → Low Visibility")
            st.caption("Precipitation > 5 mm → Heavy Rain")
            st.caption("Temp > 38 °C → Extreme Heat")
            st.caption("Temp < 0 °C → Freezing")

        submitted = st.form_submit_button("🔮 Run Prediction", use_container_width=True)

    if submitted:
        # Compute binary flags from thresholds (mirror feature_engineering.py)
        is_high_wind      = int(wind_speed_kmh  > 50)
        is_low_visibility = int(visibility_km   < 3)
        is_heavy_rain     = int(precipitation_mm > 5)
        is_extreme_heat   = int(temperature_c   > 38)
        is_freezing       = int(temperature_c   < 0)

        # Encode airport
        if airport_input in le.classes_:
            airport_encoded = int(le.transform([airport_input])[0])
        else:
            airport_encoded = 0
            st.warning("Airport not seen during training — using fallback encoding (0).")

        input_data = pd.DataFrame([{
            "airport_encoded":    airport_encoded,
            "temperature_c":      temperature_c,
            "wind_speed_kmh":     wind_speed_kmh,
            "wind_gust_kmh":      wind_gust_kmh,
            "precipitation_mm":   precipitation_mm,
            "visibility_km":      visibility_km,
            "cloud_cover_pct":    cloud_cover_pct,
            "pressure_hpa":       pressure_hpa,
            "hour_of_day":        hour_of_day,
            "day_of_week":        day_of_week,
            "is_weekend":         int(is_weekend),
            "is_high_wind":       is_high_wind,
            "is_low_visibility":  is_low_visibility,
            "is_heavy_rain":      is_heavy_rain,
            "is_extreme_heat":    is_extreme_heat,
            "is_freezing":        is_freezing,
            "rolling_avg_landings": rolling_avg,
            "baseline_landings":  baseline_landings,
        }])

        disrupt_prob = clf.predict_proba(input_data)[0][1]
        disrupt_pred = clf.predict(input_data)[0]
        dev_pred     = reg.predict(input_data)[0]

        st.markdown("---")
        st.subheader("📋 Prediction Results")

        r1, r2, r3, r4 = st.columns(4)
        r1.metric("🚨 Disruption Probability", f"{disrupt_prob*100:.1f}%")
        r2.metric("🔴 Disruption Predicted",
                  "YES ⚠️" if disrupt_pred == 1 else "NO ✅",
                  delta=None)
        r3.metric("📉 Predicted Landing Deviation", f"{dev_pred:+.2f}")
        r4.metric("🏙️ Airport", airport_input)

        st.markdown("---")

        # ── Gauge chart: disruption probability ───────────────────────────────
        col_g, col_flags = st.columns(2)

        with col_g:
            st.subheader("Disruption Probability Gauge")
            fig_gauge = go.Figure(go.Indicator(
                mode="gauge+number+delta",
                value=disrupt_prob * 100,
                delta={"reference": metadata.get("disruption_rate", 0.1) * 100,
                       "suffix": "% (baseline)"},
                gauge={
                    "axis": {"range": [0, 100]},
                    "bar":  {"color": PALETTE["danger"] if disrupt_prob > 0.5 else PALETTE["success"]},
                    "steps": [
                        {"range": [0,  33], "color": "rgba(44,160,44,0.25)"},
                        {"range": [33, 66], "color": "rgba(255,127,14,0.25)"},
                        {"range": [66,100], "color": "rgba(214,39,40,0.25)"},
                    ],
                    "threshold": {
                        "line": {"color": "white", "width": 3},
                        "thickness": 0.8, "value": 50,
                    },
                },
                title={"text": "Disruption Probability (%)"},
                number={"suffix": "%"},
            ))
            fig_gauge.update_layout(height=360, margin=dict(l=20, r=20, t=40, b=20))
            show(fig_gauge)

        with col_flags:
            st.subheader("Derived Weather Flags")
            flag_results = {
                "💨 High Wind (>50 km/h)":       is_high_wind,
                "🌫️ Low Visibility (<3 km)":      is_low_visibility,
                "🌧️ Heavy Rain (>5 mm)":          is_heavy_rain,
                "🌡️ Extreme Heat (>38 °C)":       is_extreme_heat,
                "🧊 Freezing (<0 °C)":            is_freezing,
            }
            for flag_name, flag_val in flag_results.items():
                color = "🔴" if flag_val else "🟢"
                status = "ACTIVE" if flag_val else "Inactive"
                st.markdown(f"{color} **{flag_name}** — {status}")

            st.markdown("---")
            st.subheader("Input Summary")
            st.json({
                "airport":          airport_input,
                "hour_of_day":      hour_of_day,
                "day_of_week":      day_of_week,
                "is_weekend":       bool(is_weekend),
                "temperature_c":    temperature_c,
                "wind_speed_kmh":   wind_speed_kmh,
                "precipitation_mm": precipitation_mm,
                "visibility_km":    visibility_km,
                "cloud_cover_pct":  cloud_cover_pct,
                "pressure_hpa":     pressure_hpa,
            })

        st.markdown("---")

        # ── Context: how does this prediction compare to historical data? ─────
        st.subheader("📊 How Does This Compare to Historical Data?")
        hist_airport = df_raw[df_raw["airport_code"] == airport_input]

        if not hist_airport.empty:
            col_c1, col_c2 = st.columns(2)

            with col_c1:
                st.markdown(f"**Disruption probability distribution — {airport_input}**")
                hist_probs = clf.predict_proba(
                    hist_airport.copy().assign(
                        airport_encoded=int(le.transform([airport_input])[0])
                        if airport_input in le.classes_ else 0,
                        **{c: hist_airport[c].fillna(0).astype(int)
                           for c in FLAG_COLS + ["is_weekend"]},
                        rolling_avg_landings=hist_airport["rolling_avg_landings"].fillna(0),
                    ).dropna(subset=FEATURES)[FEATURES]
                )[:, 1]
                fig_hist_prob = px.histogram(
                    x=hist_probs, nbins=40,
                    color_discrete_sequence=[PALETTE["primary"]],
                    labels={"x": "Disruption Probability"},
                )
                fig_hist_prob.add_vline(
                    x=disrupt_prob, line_dash="dash",
                    line_color=PALETTE["danger"],
                    annotation_text=f"Your input: {disrupt_prob*100:.1f}%",
                    annotation_position="top right",
                )
                fig_hist_prob.update_layout(height=320, margin=dict(l=0, r=0, t=20, b=0))
                show(fig_hist_prob)

            with col_c2:
                st.markdown(f"**Landing deviation distribution — {airport_input}**")
                fig_hist_dev = px.histogram(
                    hist_airport, x="landing_deviation", nbins=40,
                    color_discrete_sequence=[PALETTE["primary"]],
                    labels={"landing_deviation": "Landing Deviation"},
                )
                fig_hist_dev.add_vline(
                    x=dev_pred, line_dash="dash",
                    line_color=PALETTE["warning"],
                    annotation_text=f"Your prediction: {dev_pred:+.2f}",
                    annotation_position="top right",
                )
                fig_hist_dev.update_layout(height=320, margin=dict(l=0, r=0, t=20, b=0))
                show(fig_hist_dev)
        else:
            st.info("No historical data available for the selected airport to compare against.")