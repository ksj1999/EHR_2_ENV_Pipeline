"""Respiratory Risk Streaming Dashboard.

Streamlit app that visualizes the output of the Spark+Kafka risk-scoring
pipeline (stored in Snowflake). Three pages, switched via sidebar nav:

  - Overview        — population-level risk distribution, transitions, map,
                      chronic-condition breakdown, recent alerts/events.
  - Weather & AQI   — pollutant trends with WHO/EPA threshold bands,
                      threshold-violation cards, per-region drill-down.
  - Patient Explorer — per-patient gauge + verdict + 7-day trend.

Data source: Snowflake (key-pair auth). Connection details are read from
environment variables — see the README "Configuration" section.
Caches: TTLs of 60–300s on most queries; 24h on the static MA town GeoJSON.
"""

import decimal
import os
import warnings

import plotly.express as px
import plotly.graph_objects as go
import snowflake.connector
import pandas as pd
import streamlit as st

warnings.filterwarnings("ignore", category=DeprecationWarning, module="boto3")


# ── Connection ────────────────────────────────────────────────────────────────

SF_ACCOUNT     = os.environ.get("SNOWFLAKE_ACCOUNT")
SF_USER        = os.environ.get("SNOWFLAKE_USER")
SF_DATABASE    = os.environ.get("SNOWFLAKE_DATABASE")
SF_SCHEMA      = os.environ.get("SNOWFLAKE_SCHEMA", "PUBLIC")
SF_WAREHOUSE   = os.environ.get("SNOWFLAKE_WAREHOUSE")
SF_PRIVATE_KEY = os.environ.get("SNOWFLAKE_PRIVATE_KEY_FILE")

_REQUIRED_VARS = {
    "SNOWFLAKE_ACCOUNT":           SF_ACCOUNT,
    "SNOWFLAKE_USER":              SF_USER,
    "SNOWFLAKE_DATABASE":          SF_DATABASE,
    "SNOWFLAKE_WAREHOUSE":         SF_WAREHOUSE,
    "SNOWFLAKE_PRIVATE_KEY_FILE":  SF_PRIVATE_KEY,
}


@st.cache_resource
def get_conn():
    missing = [name for name, value in _REQUIRED_VARS.items() if not value]
    if missing:
        st.error(
            "Missing required environment variable(s): "
            + ", ".join(missing)
            + ". See the README \"Configuration\" section."
        )
        st.stop()
    return snowflake.connector.connect(
        account=SF_ACCOUNT,
        user=SF_USER,
        private_key_file=SF_PRIVATE_KEY,
        database=SF_DATABASE,
        schema=SF_SCHEMA,
        warehouse=SF_WAREHOUSE,
    )


def run_query(sql: str, params: tuple = ()) -> pd.DataFrame:
    for attempt in range(2):
        try:
            cur = get_conn().cursor()
            try:
                cur.execute(sql, params)
                cols = [d[0].lower() for d in cur.description]
                frame = pd.DataFrame(cur.fetchall(), columns=cols)
                # Snowflake returns NUMBER columns as decimal.Decimal;
                # convert to float so Altair/Streamlit charts work.
                for col in frame.columns:
                    sample = frame[col].dropna()
                    if not sample.empty and isinstance(sample.iloc[0], decimal.Decimal):
                        frame[col] = frame[col].astype(float)
                return frame
            finally:
                cur.close()
        except snowflake.connector.errors.DatabaseError:
            if attempt == 0:
                get_conn.clear()
            else:
                raise


_CHICAGO = "America/Chicago"


def _to_chicago(ts):
    ts = pd.Timestamp(ts)
    if ts.tzinfo is None:
        ts = ts.tz_localize("UTC")
    return ts.tz_convert(_CHICAGO)


def fmt_ts(value) -> str:
    if pd.isna(value):
        return "n/a"
    return _to_chicago(value).strftime("%Y-%m-%d %H:%M CT")


def chicago_col(series: pd.Series) -> pd.Series:
    dt = pd.to_datetime(series, errors="coerce")
    if dt.dt.tz is None:
        dt = dt.dt.tz_localize("UTC")
    return dt.dt.tz_convert(_CHICAGO).dt.strftime("%Y-%m-%d %H:%M:%S")


# ── Queries ───────────────────────────────────────────────────────────────────

@st.cache_data(ttl=60, show_spinner=False)
def load_sidebar_metrics():
    return run_query("""
        SELECT
          (SELECT COUNT(*)        FROM respiratory_risk_scores)      AS total_events,
          (SELECT MAX(event_time) FROM respiratory_risk_scores)      AS latest_event,
          (SELECT COUNT(*)        FROM patient_respiratory_features) AS total_patients,
          (SELECT COUNT(*)        FROM respiratory_alerts)           AS total_alerts,
          (SELECT MAX(alert_time) FROM respiratory_alerts)           AS latest_alert
    """)


@st.cache_data(ttl=60, show_spinner=False)
def load_risk_distribution():
    return run_query("""
        SELECT final_risk_level, COUNT(*) AS count
        FROM   respiratory_risk_scores_current
        GROUP  BY final_risk_level
    """)


@st.cache_data(ttl=60, show_spinner=False)
def load_recent_events():
    return run_query("""
        SELECT event_time, kafka_timestamp, location_id,
               aqi, pm25, ozone, temperature_c,
               weather_risk_score, final_risk_score, final_risk_level
        FROM   respiratory_risk_scores
        ORDER  BY event_time DESC
        LIMIT  200
    """)


@st.cache_data(ttl=120, show_spinner=False)
def load_pollutant_trend_24h(location_id: str = ""):
    """Hourly avg PM2.5, ozone, AQI. If location_id is given, filter to that
    location only; otherwise return cross-location average."""
    if location_id:
        return run_query("""
            SELECT DATE_TRUNC('hour', event_time) AS hour,
                   ROUND(AVG(pm25),  2)           AS avg_pm25,
                   ROUND(AVG(ozone), 2)           AS avg_ozone,
                   ROUND(AVG(aqi),   1)           AS avg_aqi
            FROM   respiratory_risk_scores
            WHERE  event_time >= DATEADD(hour, -24, CURRENT_TIMESTAMP())
              AND  location_id = %s
            GROUP  BY 1
            ORDER  BY 1
        """, (location_id,))
    return run_query("""
        SELECT DATE_TRUNC('hour', event_time) AS hour,
               ROUND(AVG(pm25),  2)           AS avg_pm25,
               ROUND(AVG(ozone), 2)           AS avg_ozone,
               ROUND(AVG(aqi),   1)           AS avg_aqi
        FROM   respiratory_risk_scores
        WHERE  event_time >= DATEADD(hour, -24, CURRENT_TIMESTAMP())
        GROUP  BY 1
        ORDER  BY 1
    """)


@st.cache_data(ttl=300, show_spinner=False)
def load_locations_with_cities():
    """Distinct (location_id, city) pairs for locations that actually have
    recent events (vs the full dim_location, which contains many synthetic
    address-derived locations without any weather coverage)."""
    return run_query("""
        SELECT l.location_id, l.city
        FROM   dim_location l
        WHERE  l.city IS NOT NULL
          AND  l.location_id IN (
              SELECT DISTINCT location_id
              FROM respiratory_risk_scores_current
          )
        ORDER  BY l.city
    """)


@st.cache_data(ttl=120, show_spinner=False)
def load_threshold_violations():
    """Per-location 24h averages, then count locations exceeding each guideline."""
    return run_query("""
        WITH per_loc AS (
            SELECT location_id,
                   AVG(pm25)  AS avg_pm25,
                   AVG(ozone) AS avg_ozone,
                   AVG(aqi)   AS avg_aqi
            FROM   respiratory_risk_scores
            WHERE  event_time >= DATEADD(hour, -24, CURRENT_TIMESTAMP())
            GROUP  BY location_id
        )
        SELECT COUNT(*)                                          AS total_locations,
               SUM(CASE WHEN avg_pm25  > 15  THEN 1 ELSE 0 END) AS pm25_over_who,
               SUM(CASE WHEN avg_ozone > 70  THEN 1 ELSE 0 END) AS ozone_over_epa,
               SUM(CASE WHEN avg_aqi   > 100 THEN 1 ELSE 0 END) AS aqi_unhealthy
        FROM   per_loc
    """)


@st.cache_data(ttl=120, show_spinner=False)
def load_risk_weighted_exposure(limit: int = 15):
    """AQI × distinct patient count per location — locations where conditions
    impact the most patients."""
    return run_query(f"""
        WITH env AS (
            SELECT location_id,
                   ROUND(AVG(aqi),  1) AS avg_aqi,
                   ROUND(AVG(pm25), 1) AS avg_pm25
            FROM   respiratory_risk_scores
            WHERE  event_time >= DATEADD(hour, -24, CURRENT_TIMESTAMP())
            GROUP  BY location_id
        ),
        pat AS (
            SELECT location_id, COUNT(DISTINCT patient_id) AS patient_count
            FROM   respiratory_risk_scores_current
            GROUP  BY location_id
        )
        SELECT e.location_id, e.avg_aqi, e.avg_pm25,
               COALESCE(p.patient_count, 0)                       AS patient_count,
               ROUND(e.avg_aqi * COALESCE(p.patient_count, 0), 1) AS exposure_score
        FROM   env e
        LEFT  JOIN pat p ON e.location_id = p.location_id
        WHERE  COALESCE(p.patient_count, 0) > 0
        ORDER  BY exposure_score DESC
        LIMIT  {limit}
    """)


@st.cache_data(ttl=120, show_spinner=False)
def load_weather_by_location():
    return run_query("""
        SELECT
          location_id,
          ROUND(AVG(aqi),           1) AS avg_aqi,
          MAX(aqi)                     AS max_aqi,
          ROUND(AVG(pm25),          1) AS avg_pm25,
          ROUND(AVG(ozone),         1) AS avg_ozone,
          ROUND(AVG(temperature_c), 1) AS avg_temp_c,
          ROUND(AVG(humidity),      1) AS avg_humidity,
          COUNT(*)                     AS events
        FROM   respiratory_risk_scores
        WHERE  event_time >= DATEADD(hour, -24, CURRENT_TIMESTAMP())
        GROUP  BY location_id
        ORDER  BY avg_aqi DESC
    """)


@st.cache_data(ttl=60, show_spinner=False)
def load_alerts():
    return run_query("""
        SELECT alert_time, patient_id, location_id,
               age, has_asthma, has_copd, uses_oxygen,
               ehr_risk_score, weather_risk_score, final_risk_score,
               aqi, pm25, ozone
        FROM   respiratory_alerts
        ORDER  BY alert_time DESC
        LIMIT  500
    """)


@st.cache_data(ttl=60, show_spinner=False)
def load_patients(risk_levels: tuple, city: str):
    if not risk_levels:
        return pd.DataFrame()
    placeholders = ", ".join(["%s"] * len(risk_levels))
    params = list(risk_levels)
    city_clause = ""
    if city and city != "All":
        city_clause = "AND p.city = %s"
        params.append(city)
    return run_query(f"""
        SELECT r.patient_id, r.location_id, r.final_risk_level,
               r.final_risk_score, r.weather_risk_score, r.aqi,
               p.age, p.gender, p.city, p.state,
               p.has_asthma, p.has_copd, p.uses_oxygen,
               p.ehr_risk_score, p.ehr_risk_level
        FROM   respiratory_risk_scores_current  r
        JOIN   patient_respiratory_features     p ON r.patient_id = p.patient_id
        WHERE  r.final_risk_level IN ({placeholders})
               {city_clause}
        ORDER  BY r.final_risk_score DESC
        LIMIT  1000
    """, tuple(params))


@st.cache_data(ttl=60, show_spinner=False)
def load_patients_by_id_search(search: str, limit: int = 50):
    """ID-only lookup. Ignores risk-level / city filters."""
    pattern = f"%{search.strip()}%"
    return run_query(f"""
        SELECT r.patient_id, r.location_id, r.final_risk_level,
               r.final_risk_score, r.weather_risk_score, r.aqi,
               p.age, p.gender, p.city, p.state,
               p.has_asthma, p.has_copd, p.uses_oxygen,
               p.ehr_risk_score, p.ehr_risk_level
        FROM   respiratory_risk_scores_current  r
        JOIN   patient_respiratory_features     p ON r.patient_id = p.patient_id
        WHERE  r.patient_id ILIKE %s
        ORDER  BY r.final_risk_score DESC
        LIMIT  {limit}
    """, (pattern,))


@st.cache_data(ttl=60, show_spinner=False)
def load_map_data():
    # Joins the small dim_location table (one row per location) instead of the
    # 1.5M-row patient table — same result, much cheaper.
    return run_query("""
        SELECT
          r.location_id,
          l.city,
          l.state,
          ROUND(AVG(r.final_risk_score), 2)                                AS avg_risk_score,
          CASE
            WHEN AVG(r.final_risk_score) >= 18 THEN 'high'
            WHEN AVG(r.final_risk_score) >= 8  THEN 'medium'
            ELSE 'low'
          END                                                               AS dominant_risk,
          COUNT(*)                                                          AS patient_count,
          SUM(CASE WHEN r.final_risk_level = 'high'   THEN 1 ELSE 0 END)  AS high_count,
          SUM(CASE WHEN r.final_risk_level = 'medium' THEN 1 ELSE 0 END)  AS medium_count,
          SUM(CASE WHEN r.final_risk_level = 'low'    THEN 1 ELSE 0 END)  AS low_count
        FROM   respiratory_risk_scores_current r
        JOIN   dim_location                    l ON r.location_id = l.location_id
        WHERE  l.state IS NOT NULL AND l.city IS NOT NULL
        GROUP  BY r.location_id, l.city, l.state
        ORDER  BY avg_risk_score DESC
    """)


@st.cache_data(ttl=3600, show_spinner=False)
def load_geocode_cache() -> pd.DataFrame:
    import json
    cache_path = "/mnt/synthea_data/cache/open_meteo_geocode_cache.json"
    try:
        with open(cache_path) as f:
            raw = json.load(f)
        rows = [
            {"location_id": loc_id, "lat": v["latitude"], "lon": v["longitude"]}
            for loc_id, v in raw.items()
        ]
        return pd.DataFrame(rows)
    except Exception:
        return pd.DataFrame(columns=["location_id", "lat", "lon"])


@st.cache_data(ttl=60, show_spinner=False)
def load_patient_history(patient_id: str):
    return run_query("""
        SELECT event_time, kafka_timestamp,
               aqi, pm25, ozone, temperature_c, humidity,
               weather_risk_score, final_risk_score, final_risk_level
        FROM   respiratory_risk_scores
        WHERE  patient_id = %s
        ORDER  BY event_time DESC
        LIMIT  200
    """, (patient_id,))


@st.cache_data(ttl=60, show_spinner=False)
def load_risk_transitions_24h():
    """For each patient: compare latest risk level vs latest 24h+ ago."""
    return run_query("""
        WITH latest AS (
            SELECT patient_id, final_risk_level AS now_level
            FROM   respiratory_risk_scores_current
        ),
        prior AS (
            SELECT patient_id, final_risk_level AS prev_level
            FROM (
                SELECT patient_id, final_risk_level,
                       ROW_NUMBER() OVER (PARTITION BY patient_id
                                          ORDER BY event_time DESC) rn
                FROM   respiratory_risk_scores
                WHERE  event_time < DATEADD(hour, -24, CURRENT_TIMESTAMP())
            )
            WHERE rn = 1
        )
        SELECT COALESCE(p.prev_level, 'new') AS from_level,
               l.now_level                   AS to_level,
               COUNT(*)                      AS patient_count
        FROM   latest l
        LEFT JOIN prior p ON l.patient_id = p.patient_id
        GROUP BY 1, 2
    """)


@st.cache_data(ttl=60, show_spinner=False)
def load_risk_trend_24h():
    """Hourly count of distinct patients at each risk level over the last 24h
    (using each patient's latest event within each hour bucket)."""
    return run_query("""
        WITH hourly_latest AS (
            SELECT DATE_TRUNC('hour', event_time)        AS hour,
                   patient_id,
                   final_risk_level,
                   ROW_NUMBER() OVER (
                     PARTITION BY DATE_TRUNC('hour', event_time), patient_id
                     ORDER BY event_time DESC
                   ) AS rn
            FROM   respiratory_risk_scores
            WHERE  event_time >= DATEADD(hour, -24, CURRENT_TIMESTAMP())
        )
        SELECT hour, final_risk_level,
               COUNT(DISTINCT patient_id) AS patients
        FROM   hourly_latest
        WHERE  rn = 1
        GROUP  BY 1, 2
        ORDER  BY 1
    """)


@st.cache_data(ttl=60, show_spinner=False)
def load_risk_by_condition():
    return run_query("""
        SELECT
          CASE
            WHEN p.has_asthma = 1 AND p.has_copd = 1 THEN 'Asthma + COPD'
            WHEN p.has_asthma = 1                    THEN 'Asthma'
            WHEN p.has_copd   = 1                    THEN 'COPD'
            ELSE                                          'Neither'
          END                                                       AS condition,
          r.final_risk_level,
          COUNT(*)                                                  AS patient_count,
          ROUND(AVG(r.final_risk_score), 2)                         AS avg_score
        FROM   respiratory_risk_scores_current  r
        JOIN   patient_respiratory_features     p ON r.patient_id = p.patient_id
        GROUP  BY 1, 2
        ORDER  BY 1, 2
    """)


@st.cache_data(ttl=60, show_spinner=False)
def load_risk_velocity_top(hours_back: int = 6, limit: int = 15):
    """Patients whose final_risk_score has risen the most over `hours_back` hours."""
    return run_query(f"""
        WITH latest AS (
            SELECT patient_id, location_id,
                   final_risk_score AS now_score,
                   final_risk_level AS now_level
            FROM   respiratory_risk_scores_current
        ),
        prior AS (
            SELECT patient_id, final_risk_score AS prev_score
            FROM (
                SELECT patient_id, final_risk_score,
                       ROW_NUMBER() OVER (PARTITION BY patient_id
                                          ORDER BY event_time DESC) rn
                FROM   respiratory_risk_scores
                WHERE  event_time < DATEADD(hour, -{hours_back}, CURRENT_TIMESTAMP())
            )
            WHERE rn = 1
        )
        SELECT l.patient_id, l.location_id, l.now_level,
               ROUND(l.now_score, 2)                       AS now_score,
               ROUND(p.prev_score, 2)                      AS prev_score,
               ROUND(l.now_score - p.prev_score, 2)        AS delta_score
        FROM   latest l
        JOIN   prior  p ON l.patient_id = p.patient_id
        WHERE  p.prev_score IS NOT NULL
        ORDER  BY delta_score DESC
        LIMIT  {limit}
    """)


# ── UI Theme & Helpers ────────────────────────────────────────────────────────

def _plotly_theme():
    """Shared plotly layout for visual consistency across charts."""
    return dict(
        font=dict(family="Inter, system-ui, -apple-system, sans-serif",
                  color="#0f172a", size=12),
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
        margin=dict(l=10, r=10, t=10, b=10),
        xaxis=dict(gridcolor="#e2e8f0", zerolinecolor="#e2e8f0", linecolor="#cbd5e1"),
        yaxis=dict(gridcolor="#e2e8f0", zerolinecolor="#e2e8f0", linecolor="#cbd5e1"),
        legend=dict(bgcolor="rgba(255,255,255,0.6)", bordercolor="#e2e8f0", borderwidth=0),
    )


def themed(fig, height=None):
    """Apply the shared theme to a plotly figure."""
    layout = _plotly_theme()
    if height is not None:
        layout["height"] = height
    fig.update_layout(**layout)
    return fig


def metric_card(container, label, value, accent=None, delta=None):
    """Render a styled metric card inside `container` (use st or a column)."""
    accent_class = f" {accent}" if accent in ("high", "medium", "low", "info") else ""
    delta_html = f'<div class="metric-delta">{delta}</div>' if delta else ""
    container.markdown(
        f'<div class="metric-card{accent_class}">'
        f'<div class="metric-label">{label}</div>'
        f'<div class="metric-value">{value}</div>'
        f'{delta_html}'
        f'</div>',
        unsafe_allow_html=True,
    )


def empty_state(_icon, message):
    st.markdown(
        f'<div class="empty-state"><div class="empty-message">{message}</div></div>',
        unsafe_allow_html=True,
    )


def section_header(title, subtitle=None):
    sub = f'<div class="section-subtitle">{subtitle}</div>' if subtitle else ""
    st.markdown(
        f'<div class="section-header">'
        f'<div class="section-title">{title}</div>{sub}'
        f'</div>',
        unsafe_allow_html=True,
    )


def page_header(eyebrow, title, subtitle, status_items):
    """status_items: list of (label, value, color) tuples."""
    pills = "".join(
        f'<div class="page-pill"><span class="pulse-dot {c}"></span>'
        f'<span class="page-pill-label">{l}</span>'
        f'<span class="page-pill-value">{v}</span></div>'
        for l, v, c in status_items
    )
    st.markdown(
        f'<div class="page-header">'
        f'<div class="page-header-text">'
        f'<div class="page-header-eyebrow">{eyebrow}</div>'
        f'<div class="page-header-title">{title}</div>'
        f'<div class="page-header-subtitle">{subtitle}</div>'
        f'</div>'
        f'<div class="page-header-status">{pills}</div>'
        f'</div>',
        unsafe_allow_html=True,
    )


def sidebar_pill(label, value, color="green"):
    st.sidebar.markdown(
        f'<div class="sidebar-pill">'
        f'<span class="pulse-dot {color}"></span>'
        f'<div class="sidebar-pill-text">'
        f'<div class="sidebar-pill-label">{label}</div>'
        f'<div class="sidebar-pill-value">{value}</div>'
        f'</div></div>',
        unsafe_allow_html=True,
    )


# ── Render ────────────────────────────────────────────────────────────────────

_RISK_COLOR = {"high": "#dc2626", "medium": "#f59e0b", "low": "#16a34a"}

# Health thresholds (for "what's driving your risk")
# PM2.5 µg/m³  → WHO 24h guideline 15, EPA 24h NAAQS 35
# Ozone ppb    → EPA 8h NAAQS 70
# AQI          → 100 = moderate, 150 = unhealthy for sensitive groups
_ENV_THRESHOLDS = {
    "pm25":  {"label": "PM2.5",     "unit": "µg/m³", "ok": 15, "bad": 35},
    "ozone": {"label": "Ozone",     "unit": "ppb",   "ok": 50, "bad": 70},
    "aqi":   {"label": "AQI",       "unit": "",      "ok": 50, "bad": 100},
}


def _patient_top_drivers(row):
    """Rank env factors by how far above their 'ok' threshold they sit. Returns top 2 (label, value, severity, unit)."""
    drivers = []
    for col, t in _ENV_THRESHOLDS.items():
        val = row.get(col)
        if val is None or pd.isna(val):
            continue
        val = float(val)
        if val >= t["bad"]:
            sev = "high"
        elif val >= t["ok"]:
            sev = "medium"
        else:
            sev = "low"
        # Normalize to "fraction over ok threshold" so we can compare across factors
        rank = (val - t["ok"]) / max(t["bad"] - t["ok"], 1)
        drivers.append((rank, t["label"], val, sev, t["unit"]))
    drivers.sort(reverse=True)
    return [(label, val, sev, unit) for _, label, val, sev, unit in drivers[:2]]


def _patient_recommendations(level: str, top_driver_label: str):
    if level == "high":
        return [
            f"Stay indoors when {top_driver_label} is elevated; avoid outdoor exertion 12–4pm.",
            "Have your rescue inhaler accessible at all times.",
            "Contact your provider if symptoms worsen (shortness of breath, wheezing, chest tightness).",
        ]
    if level == "medium":
        return [
            f"Limit prolonged outdoor activity while {top_driver_label} is elevated.",
            "Take controller medications as prescribed.",
            "Monitor symptoms — escalate if they worsen tonight.",
        ]
    return [
        "Conditions are favorable today — usual activity is fine.",
        "Continue your normal medication routine.",
        "Re-check this dashboard tomorrow morning.",
    ]


@st.cache_data(ttl=86400, show_spinner=False)
def load_ma_towns_geojson():
    import json, os
    path = "/mnt/synthea_data/cache/ma_towns.geojson"
    if not os.path.exists(path):
        return None
    with open(path) as f:
        return json.load(f)


def _make_zoom_toggle(state_var, town_var, thresh=9):
    from branca.element import MacroElement
    from jinja2 import Template

    class _Impl(MacroElement):
        _template = Template("""
            {% macro script(this, kwargs) %}
            (function() {
                var m  = {{ this._parent.get_name() }};
                var sl = {{ this.sl }};
                var tl = {{ this.tl }};
                var thresh = {{ this.thresh }};
                function sync() {
                    var z = m.getZoom();
                    if (z < thresh) {
                        if (!m.hasLayer(sl)) m.addLayer(sl);
                        if (m.hasLayer(tl))  m.removeLayer(tl);
                    } else {
                        if (!m.hasLayer(tl))  m.addLayer(tl);
                        if (m.hasLayer(sl))   m.removeLayer(sl);
                    }
                }
                m.on('zoomend', sync);
                sync();
            })();
            {% endmacro %}
        """)
        def __init__(self):
            super().__init__()
            self.sl = state_var
            self.tl = town_var
            self.thresh = thresh

    return _Impl()


def render_risk_map():
    import folium
    import branca.colormap as cm
    import copy
    from streamlit_folium import st_folium

    df = load_map_data()
    if df.empty:
        st.info("No location data available for the map.")
        return

    ma_geojson = load_ma_towns_geojson()
    if not ma_geojson:
        st.info("Town boundary data not found. Run the one-time setup command on EC2.")
        return

    # Normalize city names to match Census town NAME property (title case)
    df["city_norm"] = df["city"].str.strip().str.title()
    city_score = dict(zip(df["city_norm"], df["avg_risk_score"].astype(float)))
    city_info  = df.set_index("city_norm").to_dict("index")
    overall_avg = float(df["avg_risk_score"].mean())

    colormap = cm.LinearColormap(
        colors=["#16a34a", "#fef08a", "#f59e0b", "#dc2626"],
        vmin=0, vmax=25,
        caption="Avg Risk Score (0–25)",
    )

    def state_style_fn(feature):
        return {"fillColor": colormap(overall_avg), "color": "#6b7280",
                "weight": 1.5, "fillOpacity": 0.6}

    def town_style_fn(feature):
        name  = feature["properties"].get("NAME", "")
        score = city_score.get(name)
        if score is None:
            return {"fillColor": "#e5e7eb", "color": "#9ca3af", "weight": 0.5, "fillOpacity": 0.25}
        return {"fillColor": colormap(score), "color": "#6b7280", "weight": 0.8, "fillOpacity": 0.7}

    # Dissolve all town polygons into a single MA state outline
    try:
        from shapely.ops import unary_union
        from shapely.geometry import shape, mapping
        shapes = [shape(f["geometry"]) for f in ma_geojson["features"] if f.get("geometry")]
        state_shape = unary_union(shapes)
        state_geojson = {"type": "FeatureCollection", "features": [{
            "type": "Feature",
            "geometry": mapping(state_shape),
            "properties": {
                "NAME": "Massachusetts",
                "avg_risk_score": str(round(overall_avg, 2)),
                "patient_count": str(int(df["patient_count"].sum())),
            },
        }]}
        has_state = True
    except Exception:
        has_state = False

    # Embed risk data into town GeoJSON properties for tooltip
    geojson_data = copy.deepcopy(ma_geojson)
    for feature in geojson_data["features"]:
        name  = feature["properties"].get("NAME", "")
        row   = city_info.get(name, {})
        props = feature["properties"]
        if row:
            props["avg_risk_score"] = str(round(float(row["avg_risk_score"]), 2))
            props["dominant_risk"]  = str(row["dominant_risk"]).title()
            props["patient_count"]  = str(int(row["patient_count"]))
            props["high_count"]     = str(int(row["high_count"]))
            props["medium_count"]   = str(int(row["medium_count"]))
            props["low_count"]      = str(int(row["low_count"]))
        else:
            props["avg_risk_score"] = "No data"
            props["dominant_risk"]  = "—"
            props["patient_count"]  = "—"
            props["high_count"]     = "—"
            props["medium_count"]   = "—"
            props["low_count"]      = "—"

    # zoom_start=9 → show towns; zoom out below 9 → show state
    m = folium.Map(location=[42.1, -71.5], zoom_start=9, tiles="CartoDB positron")
    colormap.add_to(m)

    if has_state:
        state_layer = folium.GeoJson(
            state_geojson,
            name="MA State",
            style_function=state_style_fn,
            tooltip=folium.GeoJsonTooltip(
                fields=["NAME", "avg_risk_score", "patient_count"],
                aliases=["State", "Avg Score", "Patients"],
            ),
            show=False,
        )
        state_layer.add_to(m)

    town_layer = folium.GeoJson(
        geojson_data,
        name="Risk by Town",
        style_function=town_style_fn,
        tooltip=folium.GeoJsonTooltip(
            fields=["NAME", "avg_risk_score", "dominant_risk", "patient_count",
                    "high_count", "medium_count", "low_count"],
            aliases=["City / Town", "Avg Score", "Risk Level", "Patients",
                     "High", "Medium", "Low"],
        ),
        show=True,
    )
    town_layer.add_to(m)

    # Zoom toggle: below zoom 9 → state layer, 9+ → town layer
    if has_state:
        _make_zoom_toggle(
            state_layer.get_name(), town_layer.get_name(), thresh=9
        ).add_to(m)

    st_folium(m, use_container_width=True, height=520, returned_objects=[])

    st.subheader("Location Risk Breakdown")
    display_df = df[[
        "city", "state", "dominant_risk", "avg_risk_score",
        "patient_count", "high_count", "medium_count", "low_count",
    ]].rename(columns={
        "dominant_risk":  "Risk Level",
        "avg_risk_score": "Avg Score",
        "patient_count":  "Patients",
        "high_count":     "High",
        "medium_count":   "Medium",
        "low_count":      "Low",
    })
    st.dataframe(display_df, use_container_width=True, hide_index=True)


def render_sidebar_status():
    st.sidebar.markdown('<div class="nav-label">Pipeline Status</div>',
                        unsafe_allow_html=True)
    try:
        m = load_sidebar_metrics().iloc[0]
        if pd.notna(m.latest_event):
            mins = int((pd.Timestamp.now(tz=_CHICAGO) - _to_chicago(m.latest_event))
                       .total_seconds() // 60)
            ev_color = "green" if mins < 10 else "amber" if mins < 60 else "red"
        else:
            mins, ev_color = None, "gray"
        if pd.notna(m.latest_alert):
            a_mins = int((pd.Timestamp.now(tz=_CHICAGO) - _to_chicago(m.latest_alert))
                         .total_seconds() // 60)
            a_color = "amber" if a_mins < 60 else "gray"
        else:
            a_color = "gray"

        sidebar_pill("Latest Event", fmt_ts(m.latest_event), ev_color)
        sidebar_pill("Latest Alert", fmt_ts(m.latest_alert), a_color)
        sidebar_pill("Total Events",   f"{int(m.total_events):,}",   "green")
        sidebar_pill("Total Patients", f"{int(m.total_patients):,}", "green")
        sidebar_pill("Total Alerts",   f"{int(m.total_alerts):,}",
                     "amber" if int(m.total_alerts) else "green")
    except Exception as exc:
        st.sidebar.error(f"Snowflake connection error: {exc}")


def render_overview():
    dist        = load_risk_distribution()
    exposure    = load_risk_weighted_exposure(limit=10)
    recent      = load_recent_events()
    transitions = load_risk_transitions_24h()
    trend       = load_risk_trend_24h()
    by_cond     = load_risk_by_condition()
    velocity    = load_risk_velocity_top(hours_back=6, limit=10)

    counts = dist.set_index("final_risk_level")["count"].to_dict() if not dist.empty else {}
    high   = int(counts.get("high",   0))
    medium = int(counts.get("medium", 0))
    low    = int(counts.get("low",    0))

    # Movement metrics from transition matrix
    newly_high = improved = stable_high = 0
    if not transitions.empty:
        for _, r in transitions.iterrows():
            f, t, n = r["from_level"], r["to_level"], int(r["patient_count"])
            if t == "high" and f != "high":
                newly_high += n
            if f == "high" and t != "high":
                improved += n
            if f == "high" and t == "high":
                stable_high += n

    section_header("Patient Overview", "Live counts and last-24h risk transitions")
    c1, c2, c3, c4, c5, c6 = st.columns(6)
    metric_card(c1, "Current Patients", f"{high + medium + low:,}", "info")
    metric_card(c2, "HIGH",   f"{high:,}",   "high")
    metric_card(c3, "MEDIUM", f"{medium:,}", "medium")
    metric_card(c4, "LOW",    f"{low:,}",    "low")
    metric_card(c5, "Newly HIGH (24h)", f"{newly_high:,}", "high",
                delta="patients to outreach")
    metric_card(c6, "Improved from HIGH", f"{improved:,}", "low",
                delta="dropped out of HIGH")

    st.divider()
    left, right = st.columns(2)
    with left:
        section_header("Risk Level Distribution")
        if dist.empty:
            empty_state("📊", "No risk data yet.")
        else:
            order = ["high", "medium", "low"]
            dist_sorted = (dist.set_index("final_risk_level")
                               .reindex(order).dropna().reset_index())
            fig = px.pie(
                dist_sorted, names="final_risk_level", values="count",
                color="final_risk_level", color_discrete_map=_RISK_COLOR,
                hole=0.55,
            )
            fig.update_traces(textposition="inside", textinfo="percent+label",
                              marker=dict(line=dict(color="white", width=2)))
            themed(fig, height=320)
            fig.update_layout(legend_title_text="Risk")
            st.plotly_chart(fig, use_container_width=True)
    with right:
        section_header("Risk-Weighted Exposure",
                       "AQI × patient count — clinical priority ranking")
        if exposure.empty:
            empty_state("", "No exposure data yet.")
        else:
            ex = exposure.iloc[::-1]
            fig = px.bar(
                ex, x="exposure_score", y="location_id", orientation="h",
                color="exposure_score",
                color_continuous_scale=["#16a34a", "#f59e0b", "#dc2626"],
                hover_data={"avg_aqi": True, "avg_pm25": True,
                            "patient_count": True, "exposure_score": ":.1f",
                            "location_id": False},
                labels={"exposure_score": "Exposure Score", "location_id": ""},
            )
            themed(fig, height=320)
            fig.update_layout(coloraxis_showscale=False)
            st.plotly_chart(fig, use_container_width=True)

    st.divider()
    section_header("Geographic Risk Map", "Zoom in for towns · zoom out for state view")
    render_risk_map()

    # ── Hospital / clinical block ────────────────────────────────────────────
    st.divider()
    section_header("Risk Movement (last 24h)",
                   "Patient counts at each risk level, hourly")
    if trend.empty:
        empty_state("⏱️", "No event history in the last 24h yet.")
    else:
        trend_df = trend.copy()
        trend_df["hour"] = pd.to_datetime(trend_df["hour"], utc=True, errors="coerce")
        trend_df["hour"] = trend_df["hour"].dt.tz_convert(_CHICAGO)
        fig = px.line(
            trend_df, x="hour", y="patients", color="final_risk_level",
            category_orders={"final_risk_level": ["high", "medium", "low"]},
            color_discrete_map=_RISK_COLOR, markers=True,
            labels={"hour": "Hour (CT)", "patients": "Patients",
                    "final_risk_level": "Risk"},
        )
        themed(fig, height=320)
        fig.update_layout(legend_title_text="Risk", legend=dict(orientation="h", y=-0.2))
        fig.update_traces(line=dict(width=2.5), marker=dict(size=6))
        st.plotly_chart(fig, use_container_width=True)

    st.divider()
    left2, right2 = st.columns([1.1, 1])
    with left2:
        section_header("Risk by Chronic Condition", "Patient cohort breakdown")
        if by_cond.empty:
            empty_state("🩺", "No condition data available.")
        else:
            cond_pivot = (by_cond
                          .pivot(index="condition", columns="final_risk_level",
                                 values="patient_count")
                          .fillna(0).astype(int)
                          .reindex(columns=["high", "medium", "low"], fill_value=0))
            fig = px.bar(
                cond_pivot, barmode="stack",
                color_discrete_map=_RISK_COLOR,
                labels={"value": "Patients", "condition": "Condition", "final_risk_level": "Risk"},
            )
            themed(fig, height=320)
            fig.update_layout(legend_title_text="Risk", legend=dict(orientation="h", y=-0.2))
            st.plotly_chart(fig, use_container_width=True)

    with right2:
        section_header("Fastest-Rising Patients (6h)", "Largest score increases")
        if velocity.empty:
            empty_state("📈", "No history available yet.")
        else:
            display = velocity.rename(columns={
                "patient_id":  "Patient",
                "location_id": "Location",
                "now_level":   "Now",
                "now_score":   "Score Now",
                "prev_score":  "Score 6h ago",
                "delta_score": "Δ",
            })
            st.dataframe(display, use_container_width=True, hide_index=True)

    st.divider()
    alerts = load_alerts()
    section_header("Recent Activity", "High-risk alerts and most recent stream events")

    if alerts.empty:
        c1, c2, c3 = st.columns(3)
        metric_card(c1, "Alerts on Record", "0", "low")
        metric_card(c2, "With Asthma",       "0", "low")
        metric_card(c3, "Using Oxygen",      "0", "low")
    else:
        c1, c2, c3 = st.columns(3)
        metric_card(c1, "Alerts on Record",
                    f"{len(alerts):,}", "info", delta="latest 500")
        metric_card(c2, "With Asthma",
                    f"{int((alerts['has_asthma'] == 1).sum()):,}", "high")
        metric_card(c3, "Using Oxygen",
                    f"{int((alerts['uses_oxygen'] == 1).sum()):,}", "high")

    a_tab, e_tab = st.tabs(["High-Risk Alerts", "Recent Events"])
    with a_tab:
        if alerts.empty:
            empty_state("", "No HIGH-risk alerts on record.")
        else:
            alerts_show = alerts.copy()
            alerts_show["alert_time"] = chicago_col(alerts_show["alert_time"])
            st.dataframe(alerts_show, use_container_width=True, hide_index=True)
    with e_tab:
        if recent.empty:
            empty_state("", "No recent events.")
        else:
            recent_show = recent.copy()
            recent_show["event_time"]      = chicago_col(recent_show["event_time"])
            recent_show["kafka_timestamp"] = chicago_col(recent_show["kafka_timestamp"])
            st.dataframe(recent_show, use_container_width=True, hide_index=True)


def render_weather():
    weather  = load_weather_by_location()
    viol     = load_threshold_violations()
    locs     = load_locations_with_cities()

    if weather.empty:
        empty_state("", "No weather data in the last 24 hours.")
        return

    # ── Threshold violations ────────────────────────────────────────────────
    section_header("Air Quality vs Health Thresholds",
                   "Locations exceeding WHO/EPA guidelines (last 24h)")
    if viol.empty or int(viol.iloc[0]["total_locations"]) == 0:
        empty_state("", "No location data available.")
    else:
        v = viol.iloc[0]
        total   = int(v["total_locations"])
        pm25_n  = int(v["pm25_over_who"])
        ozone_n = int(v["ozone_over_epa"])
        aqi_n   = int(v["aqi_unhealthy"])

        def severity(n):
            if total == 0:
                return "info"
            pct = n / total
            if pct >= 0.5:
                return "high"
            if pct >= 0.2:
                return "medium"
            return "low"

        def pct_text(n):
            return f"{n / total * 100:.0f}% of locations" if total else ""

        c1, c2, c3, c4 = st.columns(4)
        metric_card(c1, "Locations Monitored", f"{total:,}", "info")
        metric_card(c2, "PM2.5 > WHO (15 µg/m³)",
                    f"{pm25_n:,}", severity(pm25_n), delta=pct_text(pm25_n))
        metric_card(c3, "Ozone > EPA (70 ppb)",
                    f"{ozone_n:,}", severity(ozone_n), delta=pct_text(ozone_n))
        metric_card(c4, "AQI > 100 (Unhealthy)",
                    f"{aqi_n:,}", severity(aqi_n), delta=pct_text(aqi_n))

    # ── 24h pollutant trends ────────────────────────────────────────────────
    st.divider()
    section_header("24h Pollutant Trends",
                   "Hourly averages with green / amber / red health bands")

    # Region selector (search-as-you-type by default in st.selectbox)
    region_label = "All locations (population average)"
    if locs.empty:
        loc_options = [region_label]
        loc_lookup  = {region_label: ""}
    else:
        loc_lookup = {region_label: ""}
        for _, r in locs.iterrows():
            label = f"{r['city']} — {r['location_id']}"
            loc_lookup[label] = str(r["location_id"])
        loc_options = list(loc_lookup.keys())

    selected_label = st.selectbox(
        "Region", loc_options, key="trend_region",
        help="Type to search. Choose a city to see its specific trend.",
    )
    selected_location = loc_lookup[selected_label]
    trend = load_pollutant_trend_24h(selected_location)

    if trend.empty:
        empty_state("", f"No trend data for {selected_label} in the last 24h.")
    else:
        trend_df = trend.copy()
        trend_df["hour"] = pd.to_datetime(trend_df["hour"], utc=True, errors="coerce")
        trend_df["hour"] = trend_df["hour"].dt.tz_convert(_CHICAGO)

        specs = [
            ("avg_pm25",  "PM2.5 (µg/m³)", 15,  35,  "#0ea5e9"),
            ("avg_ozone", "Ozone (ppb)",   50,  70,  "#a855f7"),
            ("avg_aqi",   "AQI",           50,  100, "#0d9488"),
        ]
        cols = st.columns(3)
        for col, (yc, label, ok, bad, line_color) in zip(cols, specs):
            with col:
                st.markdown(f'<div class="chart-label">{label}</div>',
                            unsafe_allow_html=True)
                data_max = max(float(trend_df[yc].max()), bad * 1.25)
                fig = go.Figure()
                fig.add_hrect(y0=0,   y1=ok,       fillcolor="#dcfce7", line_width=0, opacity=0.45)
                fig.add_hrect(y0=ok,  y1=bad,      fillcolor="#fef3c7", line_width=0, opacity=0.45)
                fig.add_hrect(y0=bad, y1=data_max, fillcolor="#fee2e2", line_width=0, opacity=0.45)
                fig.add_trace(go.Scatter(
                    x=trend_df["hour"], y=trend_df[yc],
                    mode="lines+markers",
                    line=dict(color=line_color, width=2.5),
                    marker=dict(size=5, color=line_color),
                ))
                themed(fig, height=240)
                fig.update_layout(
                    showlegend=False,
                    yaxis=dict(range=[0, data_max], title=None),
                    xaxis=dict(title=None),
                )
                st.plotly_chart(fig, use_container_width=True)

    # ── Existing AQI/PM2.5 snapshot ─────────────────────────────────────────
    st.divider()
    section_header("Air Quality Snapshot", "Last 24h, top 15 locations by AQI")
    left, right = st.columns(2)
    with left:
        st.markdown('<div class="chart-label">Average AQI</div>',
                    unsafe_allow_html=True)
        top = weather.head(15).iloc[::-1]
        fig = px.bar(top, x="avg_aqi", y="location_id", orientation="h",
                     color="avg_aqi",
                     color_continuous_scale=["#16a34a", "#f59e0b", "#dc2626"],
                     range_color=[0, 150],
                     labels={"avg_aqi": "Avg AQI", "location_id": ""})
        themed(fig, height=380)
        fig.update_layout(coloraxis_showscale=False)
        st.plotly_chart(fig, use_container_width=True)
    with right:
        st.markdown('<div class="chart-label">Average PM2.5 (µg/m³)</div>',
                    unsafe_allow_html=True)
        top = weather.head(15).iloc[::-1]
        fig = px.bar(top, x="avg_pm25", y="location_id", orientation="h",
                     color="avg_pm25",
                     color_continuous_scale=["#16a34a", "#f59e0b", "#dc2626"],
                     range_color=[0, 50],
                     labels={"avg_pm25": "Avg PM2.5 (µg/m³)", "location_id": ""})
        themed(fig, height=380)
        fig.update_layout(coloraxis_showscale=False)
        st.plotly_chart(fig, use_container_width=True)

    st.divider()
    section_header("Location Weather Summary (last 24h)")
    st.dataframe(weather, use_container_width=True, hide_index=True)


def render_patient_explorer():
    f1, f2, f3 = st.columns((1.5, 1, 1))
    search       = f1.text_input(
        "Search patient ID",
        value="",
        help="When filled, looks up patients by ID directly — risk level and city filters are ignored.",
    )
    level_filter = f2.multiselect(
        "Risk levels", ["high", "medium", "low"], default=["high"],
        disabled=bool(search.strip()),
    )

    # Load patients to populate city filter (cached)
    all_patients = load_patients(("high", "medium", "low"), "All")
    city_options = (
        sorted(all_patients["city"].dropna().unique().tolist())
        if not all_patients.empty else []
    )
    city_filter = f3.selectbox(
        "City", ["All"] + city_options, disabled=bool(search.strip()),
    )

    if search.strip():
        # ID lookup — bypass filters entirely
        patients = load_patients_by_id_search(search.strip())
        if patients.empty:
            st.warning(f"No patient found matching ID '{search}'.")
            return
        st.caption(
            f"{len(patients):,} patient(s) matching '{search}' "
            "(filters ignored for ID search)"
        )
    else:
        patients = load_patients(
            tuple(level_filter) if level_filter else ("high",), city_filter
        )
        if patients.empty:
            st.warning("No patients match the current filters.")
            return
        st.caption(f"{len(patients):,} patients shown (up to 1,000 per query)")

    pid_list = patients["patient_id"].tolist()
    default_ix = 0
    if search and search in pid_list:
        default_ix = pid_list.index(search)

    def patient_label(pid):
        row = patients.loc[patients["patient_id"] == pid]
        if row.empty:
            return pid
        return f"{pid} | {row['city'].iloc[0]} | {row['final_risk_level'].iloc[0]}"

    selected_id = st.selectbox(
        "Patient", patients["patient_id"].tolist(),
        index=default_ix, format_func=patient_label,
    )

    p = patients.loc[patients["patient_id"] == selected_id].iloc[0]
    history = load_patient_history(selected_id)

    flags = [
        name for name, col in [("Asthma", "has_asthma"), ("COPD", "has_copd")]
        if int(p.get(col, 0)) == 1
    ]

    level = str(p["final_risk_level"]).lower()
    score = float(p["final_risk_score"])
    drivers = _patient_top_drivers(p)

    # ── Risk gauge + plain-language verdict ──────────────────────────────────
    section_header("Personal Risk Status", "Today's risk and what's driving it")
    g_left, g_right = st.columns([1, 1.4])
    with g_left:
        gauge = go.Figure(go.Indicator(
            mode="gauge+number",
            value=score,
            domain={"x": [0, 1], "y": [0, 1]},
            number={"font": {"size": 38, "color": _RISK_COLOR.get(level, "#374151")}},
            title={"text": f"<b>{level.upper()}</b>",
                   "font": {"size": 14, "color": _RISK_COLOR.get(level, "#374151")}},
            gauge={
                "axis": {"range": [0, 30], "tickwidth": 1, "tickcolor": "#94a3b8"},
                "bar":  {"color": _RISK_COLOR.get(level, "#6b7280"), "thickness": 0.25},
                "bgcolor": "rgba(0,0,0,0)",
                "borderwidth": 0,
                "steps": [
                    {"range": [0,  8],  "color": "#dcfce7"},
                    {"range": [8,  18], "color": "#fef3c7"},
                    {"range": [18, 30], "color": "#fee2e2"},
                ],
                "threshold": {
                    "line": {"color": "#0f172a", "width": 3},
                    "thickness": 0.85, "value": score,
                },
            },
        ))
        themed(gauge, height=260)
        gauge.update_layout(margin=dict(l=10, r=10, t=40, b=10))
        st.plotly_chart(gauge, use_container_width=True)

    with g_right:
        if drivers:
            top_label, top_val, top_sev, top_unit = drivers[0]
            sev_phrase = {"high": "is high", "medium": "is elevated",
                          "low":  "is within normal range"}[top_sev]
            verdict_msg = (f"Today your risk is <b>{level.upper()}</b>. "
                           f"{top_label} {sev_phrase} "
                           f"({top_val:.1f} {top_unit}).")
        else:
            top_label = "air quality"
            verdict_msg = f"Today your risk is <b>{level.upper()}</b>."

        st.markdown(
            f'<div class="metric-card {level}" style="margin-bottom:0.75rem">'
            f'<div class="metric-label">Verdict</div>'
            f'<div style="font-size:1.05rem;font-weight:500;color:var(--text);'
            f'margin-top:0.35rem;line-height:1.4">{verdict_msg}</div>'
            f'</div>',
            unsafe_allow_html=True,
        )

        if drivers:
            st.markdown('<div class="metric-label" style="margin-bottom:0.4rem">'
                        "What's driving it</div>", unsafe_allow_html=True)
            cols = st.columns(len(drivers))
            for i, (label, val, sev, unit) in enumerate(drivers):
                metric_card(cols[i], label, f"{val:.1f} {unit}".strip(), sev,
                            delta=sev.upper())

        st.markdown('<div class="metric-label" style="margin:0.8rem 0 0.4rem">'
                    "Recommended actions</div>", unsafe_allow_html=True)
        for r in _patient_recommendations(level, top_label):
            st.markdown(f"- {r}")

    st.divider()
    section_header("Patient Snapshot")
    m = st.columns(6)
    metric_card(m[0], "Final Score",   int(score), level)
    metric_card(m[1], "Risk Level",    level.title(), level)
    metric_card(m[2], "EHR Score",     int(p["ehr_risk_score"]), "info")
    metric_card(m[3], "Weather Score", int(p["weather_risk_score"]), "info")
    metric_card(m[4], "Age",           int(p["age"]) if pd.notna(p["age"]) else "n/a", "info")
    metric_card(m[5], "Diagnoses",     ", ".join(flags) if flags else "None",
                "high" if flags else "info")

    st.divider()
    left, right = st.columns(2)
    with left:
        section_header("Patient Profile")
        profile = pd.DataFrame([
            ("Patient ID", selected_id),
            ("Age",        p["age"]),
            ("Gender",     p["gender"]),
            ("City",       p["city"]),
            ("State",      p["state"]),
            ("EHR Level",  p["ehr_risk_level"]),
            ("Oxygen",     "Yes" if int(p.get("uses_oxygen", 0)) else "No"),
        ], columns=["Field", "Value"])
        profile["Value"] = profile["Value"].astype(str)
        st.dataframe(profile, use_container_width=True, hide_index=True)

    with right:
        section_header("Current Risk Snapshot")
        snap = pd.DataFrame([
            ("Location",     p["location_id"]),
            ("AQI",          p["aqi"]),
            ("EHR Score",    p["ehr_risk_score"]),
            ("Weather Score",p["weather_risk_score"]),
            ("Final Score",  p["final_risk_score"]),
            ("Risk Level",   p["final_risk_level"]),
        ], columns=["Field", "Value"])
        snap["Value"] = snap["Value"].astype(str)
        st.dataframe(snap, use_container_width=True, hide_index=True)

    if not history.empty:
        st.divider()
        section_header("7-Day Risk Trend", "Score history with risk-level bands")
        trend = history.copy()
        trend["event_time"] = pd.to_datetime(trend["event_time"], utc=True, errors="coerce")
        cutoff = pd.Timestamp.now(tz="UTC") - pd.Timedelta(days=7)
        trend = trend[trend["event_time"] >= cutoff].sort_values("event_time")
        if trend.empty:
            trend = history.copy()
            trend["event_time"] = pd.to_datetime(trend["event_time"], utc=True, errors="coerce")
            trend = trend.sort_values("event_time")

        fig = go.Figure()
        fig.add_hrect(y0=18, y1=30, fillcolor="#fee2e2", line_width=0, opacity=0.5)
        fig.add_hrect(y0=8,  y1=18, fillcolor="#fef3c7", line_width=0, opacity=0.5)
        fig.add_hrect(y0=0,  y1=8,  fillcolor="#dcfce7", line_width=0, opacity=0.5)
        fig.add_trace(go.Scatter(
            x=trend["event_time"], y=trend["final_risk_score"],
            mode="lines+markers", name="Final Score",
            line=dict(color="#1f2937", width=2), marker=dict(size=4),
        ))
        fig.add_trace(go.Scatter(
            x=trend["event_time"], y=trend["weather_risk_score"],
            mode="lines", name="Weather Score",
            line=dict(color="#0ea5e9", width=1.5, dash="dot"),
        ))
        themed(fig, height=300)
        fig.update_layout(
            yaxis=dict(title="Score", range=[0, 30], gridcolor="#e2e8f0"),
            xaxis=dict(title=None, gridcolor="#e2e8f0"),
            legend=dict(orientation="h", y=-0.2),
        )
        st.plotly_chart(fig, use_container_width=True)

        section_header("Recent Events (latest 200)")
        history["event_time"]      = chicago_col(history["event_time"])
        history["kafka_timestamp"] = chicago_col(history["kafka_timestamp"])
        st.dataframe(history, use_container_width=True, hide_index=True)


# ── Main ──────────────────────────────────────────────────────────────────────

_DASHBOARD_CSS = """
<style>
:root {
    --bg-1: #f8fafc;
    --bg-2: #eef4f2;
    --surface: #ffffff;
    --border: #e2e8f0;
    --text: #0f172a;
    --text-muted: #64748b;
    --accent: #0d9488;
    --accent-dark: #0f766e;
    --high: #dc2626;
    --medium: #f59e0b;
    --low: #16a34a;
    --info: #0ea5e9;
    --shadow-sm: 0 1px 2px rgba(15, 23, 42, 0.04), 0 1px 3px rgba(15, 23, 42, 0.05);
    --shadow-md: 0 2px 4px rgba(15, 23, 42, 0.04), 0 4px 12px rgba(15, 23, 42, 0.06);
}

.stApp {
    background:
        radial-gradient(circle at top left,  rgba(13, 148, 136, 0.10), transparent 35%),
        radial-gradient(circle at top right, rgba(245, 158, 11, 0.08), transparent 30%),
        linear-gradient(180deg, #f7f9fa 0%, #fcfbf7 55%, #eef4f2 100%);
}
/* Remove Streamlit's default top toolbar so our page header sits flush */
header[data-testid="stHeader"] { display: none; }
[data-testid="stToolbar"] { display: none; }
.block-container { padding-top: 1.25rem; padding-bottom: 2rem; }

/* Section heading */
.section-header { margin: 0.5rem 0 1rem; }
.section-title {
    font-size: 0.78rem; font-weight: 700; text-transform: uppercase;
    letter-spacing: 0.08em; color: var(--text-muted);
    border-left: 3px solid var(--accent); padding-left: 0.6rem; line-height: 1.1;
}
.section-subtitle {
    font-size: 0.85rem; color: var(--text-muted);
    margin-top: 0.25rem; padding-left: 0.85rem;
}

/* Page header (slim, sits flush with content) */
.page-header {
    display: flex; justify-content: space-between; align-items: flex-end;
    flex-wrap: wrap; gap: 1rem;
    padding: 0.25rem 0 0.95rem; margin-bottom: 1.4rem;
    border-bottom: 1px solid var(--border);
}
.page-header-text { min-width: 0; }
.page-header-eyebrow {
    font-size: 0.66rem; font-weight: 700; text-transform: uppercase;
    letter-spacing: 0.1em; color: var(--accent-dark); margin-bottom: 0.15rem;
}
.page-header-title {
    font-size: 1.5rem; font-weight: 700; line-height: 1.15; color: var(--text);
}
.page-header-subtitle {
    color: var(--text-muted); font-size: 0.85rem; margin-top: 0.2rem;
}
.page-header-status { display: flex; gap: 0.45rem; flex-wrap: wrap; }
.page-pill {
    display: flex; align-items: center; gap: 0.45rem;
    background: var(--surface); border: 1px solid var(--border);
    padding: 0.32rem 0.7rem; border-radius: 999px;
    font-size: 0.78rem; box-shadow: var(--shadow-sm);
}
.page-pill-label {
    color: var(--text-muted); font-weight: 500;
    text-transform: uppercase; letter-spacing: 0.04em; font-size: 0.65rem;
}
.page-pill-value { color: var(--text); font-weight: 600; }

/* Pulse dot */
.pulse-dot {
    display: inline-block; width: 9px; height: 9px; border-radius: 50%;
    background: var(--low);
}
.pulse-dot.green  { background: #4ade80; box-shadow: 0 0 0 0 rgba(74,222,128,0.7); animation: pulse-g 2s infinite; }
.pulse-dot.amber  { background: #fbbf24; box-shadow: 0 0 0 0 rgba(251,191,36,0.7); animation: pulse-a 2s infinite; }
.pulse-dot.red    { background: #f87171; box-shadow: 0 0 0 0 rgba(248,113,113,0.7); animation: pulse-r 2s infinite; }
.pulse-dot.gray   { background: #94a3b8; }
@keyframes pulse-g { 70% { box-shadow: 0 0 0 8px rgba(74,222,128,0);} 100%{box-shadow:0 0 0 0 rgba(74,222,128,0);} }
@keyframes pulse-a { 70% { box-shadow: 0 0 0 8px rgba(251,191,36,0);} 100%{box-shadow:0 0 0 0 rgba(251,191,36,0);} }
@keyframes pulse-r { 70% { box-shadow: 0 0 0 8px rgba(248,113,113,0);} 100%{box-shadow:0 0 0 0 rgba(248,113,113,0);} }

/* Metric cards */
.metric-card {
    background: var(--surface); border: 1px solid var(--border); border-left: 4px solid var(--accent);
    border-radius: 10px; padding: 0.85rem 1rem; box-shadow: var(--shadow-sm); height: 100%;
}
.metric-card.high   { border-left-color: var(--high); }
.metric-card.medium { border-left-color: var(--medium); }
.metric-card.low    { border-left-color: var(--low); }
.metric-card.info   { border-left-color: var(--info); }
.metric-label {
    font-size: 0.7rem; font-weight: 600; text-transform: uppercase;
    letter-spacing: 0.06em; color: var(--text-muted);
}
.metric-value {
    font-size: 1.55rem; font-weight: 700; color: var(--text); margin-top: 0.2rem;
    line-height: 1.15;
}
.metric-delta {
    font-size: 0.78rem; color: var(--text-muted); margin-top: 0.3rem;
}

/* Sidebar */
.sidebar-pill {
    display: flex; align-items: center; gap: 0.6rem;
    background: rgba(255,255,255,0.65); border: 1px solid var(--border);
    border-radius: 10px; padding: 0.55rem 0.75rem; margin-bottom: 0.5rem;
    box-shadow: var(--shadow-sm);
}
.sidebar-pill-text { line-height: 1.2; flex: 1; min-width: 0; }
.sidebar-pill-label {
    font-size: 0.65rem; font-weight: 600; text-transform: uppercase;
    letter-spacing: 0.06em; color: var(--text-muted);
}
.sidebar-pill-value {
    font-size: 0.9rem; font-weight: 600; color: var(--text);
    overflow: hidden; text-overflow: ellipsis; white-space: nowrap;
}

/* Tabs */
.stTabs [data-baseweb="tab-list"] { gap: 0.4rem; border-bottom: 1px solid var(--border); }
.stTabs [data-baseweb="tab"] {
    height: 2.6rem; padding: 0 1.1rem; background: transparent;
    border-radius: 8px 8px 0 0; font-weight: 500; color: var(--text-muted);
}
.stTabs [aria-selected="true"] {
    background: rgba(13, 148, 136, 0.08); color: var(--accent-dark) !important;
    font-weight: 600;
}

/* Empty state */
.empty-state {
    text-align: center; padding: 2.5rem 1rem; color: var(--text-muted);
    border: 1px dashed var(--border); border-radius: 10px; background: rgba(255,255,255,0.4);
}
.empty-icon { font-size: 2rem; margin-bottom: 0.4rem; }
.empty-message { font-size: 0.9rem; }

/* Subtler dividers */
[data-testid="stDivider"] { margin: 1.5rem 0 1rem; opacity: 0.5; }

/* Chart label (small subtitle above a chart) */
.chart-label {
    font-size: 0.78rem; font-weight: 600; color: var(--text-muted);
    text-transform: uppercase; letter-spacing: 0.05em;
    margin: 0.25rem 0 0.5rem;
}

/* Sidebar nav (radio styled as nav menu) */
section[data-testid="stSidebar"] [data-testid="stRadio"] > div {
    gap: 0.35rem;
}
section[data-testid="stSidebar"] [data-testid="stRadio"] label {
    background: rgba(255, 255, 255, 0.55);
    border: 1px solid var(--border);
    border-radius: 10px;
    padding: 0.55rem 0.85rem;
    margin: 0;
    cursor: pointer;
    transition: background 0.15s ease, border-color 0.15s ease;
    box-shadow: var(--shadow-sm);
    font-weight: 500;
    color: var(--text);
}
section[data-testid="stSidebar"] [data-testid="stRadio"] label:hover {
    background: rgba(13, 148, 136, 0.08);
    border-color: rgba(13, 148, 136, 0.4);
}
section[data-testid="stSidebar"] [data-testid="stRadio"] label > div:first-child {
    display: none !important;
}
section[data-testid="stSidebar"] [data-testid="stRadio"] label:has(input:checked) {
    background: rgba(13, 148, 136, 0.14);
    border-color: var(--accent);
    color: var(--accent-dark);
    font-weight: 600;
    box-shadow: 0 1px 4px rgba(13, 148, 136, 0.15);
}
section[data-testid="stSidebar"] .nav-label {
    font-size: 0.65rem; font-weight: 700; text-transform: uppercase;
    letter-spacing: 0.08em; color: var(--text-muted);
    margin: 0.5rem 0 0.5rem; padding-left: 0.25rem;
}
</style>
"""


def main():
    st.set_page_config(page_title="Respiratory Risk Dashboard", layout="wide")
    st.markdown(_DASHBOARD_CSS, unsafe_allow_html=True)

    # ── Sidebar: brand + navigation + status ────────────────────────────────
    st.sidebar.markdown(
        '<div style="font-size:1.05rem;font-weight:700;color:var(--text);'
        'padding:0.25rem 0 0.1rem">Respiratory Risk</div>'
        '<div style="font-size:0.75rem;color:var(--text-muted);'
        'padding-bottom:0.4rem">Streaming pipeline · Snowflake</div>',
        unsafe_allow_html=True,
    )

    nav_options = {
        "Overview":          "overview",
        "Weather & AQI":     "weather",
        "Patient Explorer":  "patients",
    }
    st.sidebar.markdown('<div class="nav-label">Navigation</div>',
                        unsafe_allow_html=True)
    nav_choice = st.sidebar.radio(
        "Navigation", list(nav_options.keys()),
        label_visibility="collapsed",
    )
    page = nav_options[nav_choice]

    st.sidebar.markdown("<br>", unsafe_allow_html=True)
    render_sidebar_status()

    # Live pipeline status pills (shown on every page)
    try:
        m_row = load_sidebar_metrics().iloc[0]
        latest = m_row.latest_event
        if pd.notna(latest):
            ts = _to_chicago(latest)
            mins_ago = int((pd.Timestamp.now(tz=_CHICAGO) - ts).total_seconds() // 60)
            freshness = f"{mins_ago}m ago" if mins_ago < 60 else f"{mins_ago // 60}h ago"
            dot = "green" if mins_ago < 10 else "amber" if mins_ago < 60 else "red"
        else:
            freshness, dot = "no events", "gray"
        status = [
            ("Pipeline", freshness, dot),
            ("Patients", f"{int(m_row.total_patients):,}", "green"),
            ("Events",   f"{int(m_row.total_events):,}",   "green"),
            ("Alerts",   f"{int(m_row.total_alerts):,}",   "amber" if int(m_row.total_alerts) else "green"),
        ]
    except Exception:
        status = [("Pipeline", "unknown", "gray")]

    page_meta = {
        "overview": ("Dashboard", "Overview",
                     "Live patient counts, risk transitions, and recent alerts."),
        "weather":  ("Environmental", "Weather & AQI",
                     "Air-quality conditions across monitored locations (last 24h)."),
        "patients": ("Clinical", "Patient Explorer",
                     "Per-patient risk drivers and recommended actions."),
    }
    eyebrow, title, subtitle = page_meta[page]
    page_header(eyebrow, title, subtitle, status)

    if page == "overview":
        render_overview()
    elif page == "weather":
        render_weather()
    elif page == "patients":
        render_patient_explorer()


if __name__ == "__main__":
    main()
