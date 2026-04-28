# Respiratory Risk Streaming Pipeline

A dual-latency data pipeline that joins synthetic EHR records (Synthea) with
live weather and air-quality data (Open-Meteo) to produce per-patient
respiratory risk scores in real time. Built for CSE 5114.

- **Batch layer**: Spark builds a static patient-vulnerability feature table
  from Synthea CSV exports.
- **Streaming layer**: Kafka ingests weather/AQI events; Spark Structured
  Streaming joins them to the patient features and emits scored events.
- **Warehouse layer**: Spark Streaming writes scored events directly to S3 via
  `s3a://`; Airflow `COPY INTO`s them into Snowflake every 10 minutes and an
  alert task generates HIGH-risk notifications.
- **Visualization layer**: Streamlit dashboard reads from Snowflake and
  shows population, environmental, and per-patient views.

---

## Table of Contents

1. [Architecture](#architecture)
2. [Data Flow](#data-flow)
3. [Component Settings](#component-settings)
   - [Kafka](#kafka)
   - [Spark — batch (EHR features)](#spark--batch-ehr-features)
   - [Spark — streaming (risk scoring)](#spark--streaming-risk-scoring)
   - [Snowflake](#snowflake)
   - [Airflow](#airflow)
   - [Streamlit dashboard](#streamlit-dashboard)
4. [Snowflake Schema](#snowflake-schema)
5. [Risk Scoring Logic](#risk-scoring-logic)
6. [City Matching (MA Municipality Dictionary)](#city-matching-ma-municipality-dictionary)
7. [Configuration & Secrets](#configuration--secrets)
8. [Setup](#setup)
9. [Running the Pipeline](#running-the-pipeline)
10. [Project Layout](#project-layout)

---

## Architecture

```
                  ┌──────────────────────────┐
                  │  Synthea CSV (S3, ~290GB)│
                  │  patients/conditions     │
                  │  encounters/medications  │
                  └────────────┬─────────────┘
                               │ Spark batch (build_patient_features.py)
                               │   • EHR scoring (cap 0–25)
                               │   • MA city dictionary validation
                               ▼
                ┌────────────────────────────┐
                │  Two parquet outputs on S3 │
                │  ─────────────────────────  │
                │  (a) ..._features          │
                │      ~1.58M patients       │
                │  (b) ..._features_cohort   │
                │      ~600K patients,       │
                │      respiratory only      │
                └────────────────────────────┘
                               │
                               │       ┌─── F.broadcast() join ───┐
                               ▼       ▼                          │
   ┌──────────────┐   ┌────────────────────┐                      │
   │ Open-Meteo   │ ─►│ Kafka topic        │                      │
   │ weather/AQI  │   │ environment_raw    │ ─► Spark Streaming ──┤
   │ (every 10min)│   │ (3 partitions)     │    on location_id    │
   └──────────────┘   └────────────────────┘                      │
                                                                  ▼
                                                  ┌─────────────────────────┐
                                                  │ Scored events (parquet) │
                                                  │ written DIRECTLY to S3  │
                                                  │ via s3a:// (no EBS hop) │
                                                  │ + final_risk_score      │
                                                  │ + final_risk_level      │
                                                  └────────┬────────────────┘
                                                           │
                                  Airflow DAG every 10 min:
                                    1. COPY INTO respiratory_risk_scores
                                    2. COPY INTO patient_respiratory_features
                                    3. CREATE OR REPLACE dim_location
                                    4. INSERT new HIGH-risk into alerts
                                    5. Email summary if new alerts
                                                           ▼
                                ┌──────────────────────────────────────┐
                                │ Snowflake (warehouse + DB.PUBLIC)    │
                                │  respiratory_risk_scores       (fact)│
                                │  patient_respiratory_features  (dim) │
                                │  dim_location                  (dim) │
                                │  respiratory_alerts            (fact)│
                                │  respiratory_risk_scores_current (V) │
                                └──────────────┬───────────────────────┘
                                               │
                                               ▼
                                       ┌──────────────┐
                                       │  Streamlit   │
                                       │  dashboard   │
                                       └──────────────┘
```

---

## Data Flow

| # | Stage | Latency | Output |
|---|-------|---------|--------|
| 1 | Synthea EHR → patient features + cohort | One-shot batch (~10 min) | Two parquet files on S3 |
| 2 | Open-Meteo → Kafka producer | 10-minute pull cycle | JSON in `environment_raw` |
| 3 | Kafka → Spark Streaming (broadcast join) → scored events | ~1–2 s per microbatch | Parquet directly on S3 |
| 4 | S3 → Snowflake (`COPY INTO`) + alert generation + dim refresh | 10-minute Airflow cycle | Snowflake tables refreshed |
| 5 | Snowflake → Streamlit | 60–300 s cache TTL | Browser |

The streaming path writes directly to S3 (`s3a://...`) rather than through
EBS — the previous EBS staging tasks were removed from the Airflow DAG
because they had become a no-op.

---

## Component Settings

### Kafka

Single-broker local setup. Runtime data goes on the EBS volume so it survives
restarts but is isolated from the OS root disk.

| Setting | Value | Why |
|---|---|---|
| Broker port | `9093` | Default 9092 conflicts with other services we ran during development |
| Zookeeper port | `2182` | Same reason |
| Data dir | `/mnt/synthea_data/kafka_runtime/kafka-logs` | EBS-backed, persistent across reboots |
| Topic | `environment_raw` | Single topic for all weather/AQI events |
| Partitions | `3` | Allows Spark to parallelize ingestion across 3 task slots |
| Replication factor | `1` | Single broker; no replication possible |
| Log retention | `168 hours (7 days)` | Lets Spark replay from `earliest` if needed |
| Segment size | `1 GB` | Kafka default |
| Producer key | `location_id` | Ensures events for the same city land on the same partition (preserves ordering per location) |

Topic creation: [`scripts/create_kafka_topic.sh`](scripts/create_kafka_topic.sh)
Broker startup: [`scripts/start_kafka_services.sh`](scripts/start_kafka_services.sh)

### Spark — batch (EHR features)

[`spark_jobs/build_patient_features.py`](spark_jobs/build_patient_features.py)
processes the full Synthea export and produces two outputs.

| Setting | Value | Why |
|---|---|---|
| Input | `s3a://synthea-full-bucket/raw/synthea_csv/output_*/output_*/csv/*.csv` | 12 Synthea export shards |
| Primary output | `s3a://.../features/patient_respiratory_features` (parquet) | Loaded into Snowflake for the dashboard's per-patient queries |
| **Cohort output** | `s3a://.../features/patient_respiratory_features_cohort` (parquet) | Pre-filtered to patients with any respiratory condition / med — the streaming join's broadcast input |
| `spark.sql.shuffle.partitions` | `200` | Default; works for the full dataset |
| Lookback window | `90 days` (configurable) | Recent encounters/meds carry more weight than ancient history |
| **City validation** | Filter against 351-municipality MA dictionary | Drops malformed addresses where the legacy regex captured street names ("Fletcher Isle New Bedford") into `city` |
| S3 packages | `org.apache.hadoop:hadoop-aws:3.3.4` | S3A filesystem support |
| Credentials | `DefaultAWSCredentialsProviderChain` | Uses EC2 instance role; no keys in code |
| Trigger | One-shot (`spark-submit` then exit) | Re-run when EHR data changes |

The job uses `.cache()` on the final dataframe before computing
high/medium/low counts so the three count operations don't force three
separate S3 re-scans.

**City validation rationale:** Legacy Synthea ADDRESS strings sometimes
captured the street name into the `city` field via greedy regex (e.g.
"Fletcher Isle New Bedford"), inflating `dim_location` to 782K rows. The
job now validates every parsed `city` against `MA_MUNICIPALITIES` (a hardcoded
set of all 351 incorporated MA cities/towns sourced from the Massachusetts
Secretary of the Commonwealth). Rows whose `city` doesn't match a real
municipality are dropped before the location_id is built. After this
filter `dim_location` is bounded at ≤351 rows.

### Spark — streaming (risk scoring)

[`spark_jobs/stream_risk_scores.py`](spark_jobs/stream_risk_scores.py) is
the real-time scorer.

| Setting | Value | Why |
|---|---|---|
| Source | Kafka topic `environment_raw`, `startingOffsets=latest` | Tail mode — process only new events |
| Sink | Parquet on S3 (`s3a://.../processed/respiratory_patient_features`) | Direct write avoids EBS→S3 sync hop |
| Output mode | `append` | Each event is independent; no aggregation across triggers |
| Trigger | Default (microbatch as fast as Kafka delivers) | The producer publishes every 10 min, so the stream idles between polls |
| Checkpoint | `/mnt/synthea_data/checkpoints/respiratory_patient_features` | EBS-backed; required for exactly-once Kafka offsets |
| Watermark | None | We don't need event-time aggregation; weather events score independently |
| Join | `inner` join Kafka stream ↔ pre-filtered cohort with `broadcast()` hint | See "Streaming Join Optimization" below |
| Packages | `spark-sql-kafka-0-10_2.12:3.5.1`, `hadoop-aws:3.3.4` | Kafka source + S3 access |
| Output schema | event_id, event_time, location_id, weather metrics, patient_id, weather_risk_score, final_risk_score, final_risk_level | One row per (patient × weather event) |

The job is restart-safe: if you `pkill` and re-run, Spark resumes from the
checkpoint at the last committed Kafka offset.

#### Streaming Join Optimization

In the original implementation the join's right side was the **raw**
patient table (~4.78 M rows from the full Synthea export, before the
data-quality cleanup described in [City Matching](#city-matching-ma-municipality-dictionary)).
At that size, the table exceeded Spark's default broadcast threshold
(10 MB), so Spark planned a sort-merge join with a 200-way shuffle on
**every** microbatch — the unoptimized baseline measured ~10 s per
microbatch on as few as 48 input events. Two complementary optimizations
bring per-batch cost down to ~1–2 s with no shuffle on the patient side:

1. **Cohort pre-filter (batch layer).** `build_patient_features.py` writes
   a second parquet at `…_cohort` containing only patients with any
   chronic respiratory condition (asthma, COPD spectrum, recent
   pneumonia/respiratory infection) or active respiratory medication
   (oxygen, steroid, inhaler, leukotriene modifier). After the MA city
   dictionary filter plus this respiratory-only filter, the cohort is
   ~600 K rows — small enough to broadcast cleanly to every executor.

2. **Broadcast hash join (streaming layer).** `stream_risk_scores.py`
   reads from the cohort parquet, calls `.cache()` and then `.count()` to
   materialize the cache once at startup, and applies an explicit
   `F.broadcast(patient_df)` hint on the join. This forces Spark to ship
   the cohort once to every executor and join locally, eliminating the
   per-microbatch shuffle of the patient side entirely.

Combined, the join cost shifts from O(microbatch + cohort) per batch
(sort-merge + shuffle) to O(microbatch) per batch with a one-time
O(cohort) broadcast at job start. Empirically (measured directly in our
running stream), per-batch task fan-out dropped from 200 sort-merge tasks
to 3 (one per Kafka input partition), and per-batch latency dropped from
~9.9 s (batch 244, 48 input rows) to ~2.8 s (batch 252, 15 input rows).
Per-row cost is similar across both runs (~200 ms/row); the win is the
elimination of fixed shuffle and scheduling overhead.

### Snowflake

| Setting | Value | Why |
|---|---|---|
| Auth | Key-pair (RSA `.p8`) | Snowflake account uses key-pair auth; no passwords in code |
| Warehouse | `XS` (configurable) | Adequate for COPY INTO + dashboard queries on this volume |
| Storage integration | `s3_synthea_int` → `synthea-full-bucket` | Lets Snowflake read S3 directly via STAGE without re-uploading |
| Stage format | Parquet | Same format Spark writes; column-pruning works |
| Load pattern | `COPY INTO ... FILE_FORMAT=(TYPE=PARQUET) MATCH_BY_COLUMN_NAME=CASE_INSENSITIVE FORCE=FALSE` | Idempotent; skips files already loaded |
| Pattern filter | `'.*\.parquet'` | Excludes the 0-byte `_SUCCESS` markers Spark writes |
| Time zone | UTC throughout | Display conversion to Chicago time happens in the dashboard |

### Airflow

[`dags/respiratory_pipeline.py`](dags/respiratory_pipeline.py) — the batch
orchestration layer.

| Setting | Value |
|---|---|
| Schedule | `*/10 * * * *` (every 10 minutes) |
| Catchup | `False` |
| Retries | 2 with 2-min delay |
| Connection | `snowflake_default` (key-pair, configured once via `airflow connections add`) |
| Variables used | `GMAIL_SENDER`, `GMAIL_RECIPIENT`, `GMAIL_APP_PASSWORD`, `DASHBOARD_URL` |

Tasks (in order):

```
load_risk_scores ─────────────────────────┐
                                           ├── generate_alerts ── send_email ── row_count_check
load_patient_features ── refresh_dim_location ┘
```

| Task | What it does |
|---|---|
| `load_risk_scores_to_snowflake` | `COPY INTO respiratory_risk_scores` from the S3 stage Spark writes to |
| `load_patient_features_to_snowflake` | `COPY INTO patient_respiratory_features` |
| `refresh_dim_location` | `CREATE OR REPLACE TABLE dim_location` from the patient table — keeps the location dim in sync |
| `generate_high_risk_alerts` | INSERT new HIGH patients into `respiratory_alerts`, deduped against last 24h |
| `send_alert_email` | Gmail SMTP summary (top locations + worst cases) — only fires if new alerts exist |
| `row_count_check` | UNION-ALL row counts across the three main tables (logged) |

> **Note:** Earlier versions of the DAG had `sync_risk_scores_to_s3` and
> `sync_location_manifest_to_s3` BashOperators that ran `aws s3 sync` from
> EBS to S3 before the COPY INTO. Those were removed: Spark Structured
> Streaming now writes directly to S3 via the `s3a://` filesystem, so the
> intermediate sync step was a no-op.

### Streamlit dashboard

[`dashboard/streamlit_app.py`](dashboard/streamlit_app.py)

| Setting | Value | Why |
|---|---|---|
| Port | `8501` (Streamlit default) | |
| Cache TTLs | 60 s (most queries), 120 s (weather), 300 s (location list), 24 h (MA town GeoJSON) | Balance freshness vs Snowflake compute cost |
| Connection | Cached `@st.cache_resource` singleton with auto-reconnect on `DatabaseError` | One Snowflake connection per Streamlit process |
| Pages | Overview, Weather & AQI, Patient Explorer (sidebar nav, no top tabs) | |

Required environment variables (see [Configuration](#configuration--secrets)):

```
SNOWFLAKE_ACCOUNT
SNOWFLAKE_USER
SNOWFLAKE_DATABASE
SNOWFLAKE_WAREHOUSE
SNOWFLAKE_PRIVATE_KEY_FILE
SNOWFLAKE_SCHEMA   (optional, defaults to PUBLIC)
```

---

## Snowflake Schema

A bounded star: one fact table, two dimension tables, plus a view and an
alerts table. Current row counts live alongside each table.

### `respiratory_risk_scores` (fact, append-only)

One row per scored Kafka event. **Current: ~3.5 M rows, growing.**

| Column | Type | Notes |
|---|---|---|
| `event_id` | STRING | Producer-generated unique id |
| `event_time` | TIMESTAMP_NTZ | UTC; weather observation time |
| `kafka_timestamp` | TIMESTAMP_NTZ | When Kafka received the event |
| `location_id` | STRING | FK → `dim_location.location_id` (lowercased `<city>_<state>`) |
| `patient_id` | STRING | FK → `patient_respiratory_features.patient_id` |
| `temperature_c`, `pm25`, `pm10`, `ozone`, `nitrogen_dioxide`, `aqi`, `humidity`, `wind_speed` | NUMBER | Raw weather/AQI |
| `weather_risk_score` | NUMBER | 0–15 typical |
| `final_risk_score` | NUMBER | 0–30 typical (cap depends on EHR) |
| `final_risk_level` | STRING | `low` / `medium` / `high` |

### `patient_respiratory_features` (dimension)

One row per patient with a valid MA municipality address. Rebuilt by
`build_patient_features.py`. **Current: ~1.58 M rows** (down from the raw
4.78 M after dropping non-MA patients and rows whose parsed `city` failed
dictionary validation). Patients without any respiratory condition or
medication still appear here — their condition / medication flags are
just all zero and their `ehr_risk_score` is driven by age alone. The
streaming join's right-hand side is the smaller `_cohort` parquet
(~600 K rows) which additionally filters to patients with at least one
respiratory flag — see [Streaming Join Optimization](#streaming-join-optimization).

| Column | Type | Notes |
|---|---|---|
| `patient_id` | STRING | PK |
| `age`, `gender`, `race`, `ethnicity` | demographics | |
| `city`, `state`, `zip`, `lat`, `lon`, `location_id` | location | `state = "MA"` always; `lat`/`lon` may be NULL on legacy Synthea exports |
| `has_asthma`, `has_copd`, `has_emphysema`, `has_chronic_bronchitis`, `recent_pneumonia`, `recent_respiratory_infection` | 0/1 flags | |
| `asthma_emergency_admission_count`, `emergency_encounter_count`, `hospital_admission_count`, `asthma_followup_count`, `respiratory_reason_count` | INT | within lookback (90 d), each capped per term |
| `uses_oxygen`, `uses_steroid`, `uses_inhaler`, `uses_respiratory_med` | 0/1 flags | "active" — start ≤ ref_date AND (stop is NULL or within lookback) |
| `age_score`, `condition_score`, `encounter_score`, `medication_score` | INT | EHR sub-scores |
| `ehr_risk_score` | INT | Capped at 25 |
| `ehr_risk_level` | STRING | `low` (<6) / `medium` (6–11) / `high` (≥12) |
| `last_updated` | DATE | Reference date used in computation |

### `dim_location` (dimension)

One row per location. **Hard-bounded at ≤ 351 rows** (the count of MA
municipalities). **Current: 348.** Refreshed on every Airflow run via
`refresh_dim_location` (`CREATE OR REPLACE` from
`patient_respiratory_features`). Lets queries that need city/state for a
location join a small dim instead of going through the 1.58 M-row patient
table.

| Column | Type | Notes |
|---|---|---|
| `location_id` | STRING | PK — lowercased `<city>_<state>` |
| `city` | STRING | Always a valid MA municipality (dictionary-validated) |
| `state` | STRING | Always `MA` in current data |
| `zip` | STRING | Representative ZIP for the city |
| `lat`, `lon` | NUMBER | May be NULL on legacy Synthea exports |

### `respiratory_risk_scores_current` (view)

Latest event per patient — used by the dashboard for "now" queries.

```sql
CREATE OR REPLACE VIEW respiratory_risk_scores_current AS
SELECT *
FROM (
  SELECT *, ROW_NUMBER() OVER (PARTITION BY patient_id ORDER BY event_time DESC) AS rn
  FROM respiratory_risk_scores
) WHERE rn = 1;
```

### `respiratory_alerts` (alerts log, append-only)

Inserted by the Airflow `generate_high_risk_alerts` task. Patients are
deduped against any alert in the last 24 hours. **Current: ~43 K rows.**

| Column | Type | Notes |
|---|---|---|
| `patient_id`, `location_id` | STRING | FKs to the patient and location dims |
| `alert_time` | TIMESTAMP_NTZ | `CURRENT_TIMESTAMP()` at insert (UTC) |
| `event_time` | TIMESTAMP_NTZ | The triggering weather event |
| `final_risk_score`, `ehr_risk_score`, `weather_risk_score` | NUMBER | Snapshot at alert time |
| `aqi`, `pm25`, `ozone` | NUMBER | Triggering conditions snapshot |
| `age`, `has_asthma`, `has_copd`, `uses_oxygen` | dim snapshot | Denormalized from the patient row at insert time, used by the Gmail summary so it doesn't have to join back to the patient table |

---

## Risk Scoring Logic

Four signals combine into one score:

```
final = ehr_baseline                              # patient vulnerability (0-25, capped)
      + weather_load                              # current environment (0-15)
      + min(ehr × weather / 20, 12)               # interaction (vulnerable + bad day amplify)
      + has_asthma  × ozone_score                 # asthma sensitivity bonus
      + copd_flag   × particulate_score           # COPD sensitivity bonus
```

Each component is independently capped to keep the score interpretable.

### EHR baseline (0 – 25, capped) — patient vulnerability

Rebuilt by `build_patient_features.py` from Synthea conditions + encounters
+ medications within a 90-day lookback window.

| Component | Max | How it's computed |
|---|---|---|
| Age | 3 | Tiered: 50 → +1, 65 → +2, 75 → +3 |
| COPD spectrum | 4 | `greatest()` of COPD / emphysema / chronic bronchitis flags — never triple-counted |
| Asthma | 3 | Single flag |
| Recent infection | 3 (capped) | Sum of pneumonia (3) + recent respiratory infection (1), then `least(_, 3)` |
| Encounter history | 18 (capped) | ED admissions × 5 (cap 8), other emergencies × 2 (cap 4), hospital admits × 2 (cap 4), asthma follow-up × 1 (cap 2) |
| Medication intensity | 7 | Oxygen × 4 + steroid × 2 + inhaler × 1 |
| **Total cap** | **25** | Hard `least(sum, 25)` to prevent extreme outliers from skewing the population |

EHR levels (used in dashboard's per-patient view): `low < 6`, `medium 6–11`,
`high ≥ 12`.

### Weather components (0 – 15 typical) — acute environmental load

Computed per Kafka event in `stream_risk_scores.py`. Thresholds anchored to
EPA AQI breakpoints and WHO PM2.5 guidelines.

| Component | Max | Threshold logic |
|---|---|---|
| Particulate | 6 | `max(aqi_score, pm25_score)` — avoids double-counting (AQI is often derived from PM2.5). Tiers: WHO 24h (15 µg/m³) → +1; EPA 24h NAAQS (35 µg/m³) → +3; severe (>250 µg/m³) → +6 |
| Ozone | 5 | EPA 8-h NAAQS (70 ppb) → +1; tiers up to +5 at 200+ ppb |
| Humidity | 2 | Both extremes aggravate symptoms: <25 % → +1, >70 % → +1, >85 % → +2 |
| Temperature | 2 | Heat (>32 °C +1, >38 °C +2) and cold-air bronchospasm (<0 °C +1, <−10 °C +2) |
| **Total** | **15** | Sum of components |

### Final levels

| Range | Level |
|---|---|
| `final < 8` | low |
| `8 ≤ final < 18` | medium |
| `final ≥ 18` | high |

### Worked examples

| Patient profile | EHR | Weather | + Interaction | + Bonus | Final | Level |
|---|---|---|---|---|---|---|
| Healthy, clean day | 0 | 0 | 0 | 0 | **0** | low |
| Healthy, polluted day | 0 | 8 | 0 | 0 | **8** | medium |
| Stable COPD, clean day | 12 | 0 | 0 | 0 | **12** | medium |
| Stable COPD, polluted day | 12 | 8 | 4.8 | 6 | **~30** | high |
| Asthma + bad ozone day | 5 | 6 | 1.5 | 3 | **~16** | medium |

> **Note on grounding:** The pollutant thresholds are aligned with EPA AQI
> breakpoints and WHO PM2.5 guidelines. The point weights and combination
> formula are heuristic — not validated against real exacerbation outcomes.
> This is a clinical scoring rubric for a streaming-pipeline demo, not a
> validated decision-support tool.

---

## City Matching (MA Municipality Dictionary)

Synthea's legacy 17-column patient export packs city, state, and ZIP into a
single ADDRESS string. The original regex used a greedy capture
(`[A-Za-z .'\-]+`) which silently swallowed the street name into the city
field — addresses like `"123 Fletcher Isle New Bedford MA 02742 US"` produced
`city = "Fletcher Isle New Bedford"` and `location_id = "fletcher_isle_new_bedford_ma"`.

The result was a `dim_location` table with **781,902 rows** of mostly
fictitious "cities" that were really street-name fragments — nearly every
address became its own location.

### The fix: two-step validation

**Step 1 — non-greedy regex with a constrained city group:**

```python
# build_patient_features.py
addr_pattern = (
    r"^.+? ((?:(?:New|North|South|East|West|Fall|Great)\s)?"
    r"[A-Z][a-z]+) ([A-Z]{2}) ([0-9]{5}) US$"
)
```

This still produces some false positives ("Fall Hingham" instead of just
"Hingham" when a street name happens to end in "Fall"), so step 2 is
required.

**Step 2 — dictionary lookup against `MA_MUNICIPALITIES`:**

A hardcoded `frozenset` of all 351 incorporated MA cities and towns
(sourced from the Massachusetts Secretary of the Commonwealth):

```python
MA_MUNICIPALITIES = [
    "Abington", "Acton", ..., "Boston", ..., "Fall River",
    "New Bedford", "North Andover", ..., "Worcester", "Yarmouth",
]
```

After the regex, every parsed `city` is normalized with `F.initcap()` and
checked against this set. Rows whose city isn't a real MA municipality are
**dropped** before `location_id` is built:

```python
patients_clean = (
    patients_clean
    .withColumn("city", F.initcap(F.col("city")))
    .filter((F.col("state") == "MA") & F.col("city").isin(MA_MUNICIPALITIES))
)
```

### Why this matters

- **`dim_location` is now hard-bounded at ≤ 351 rows** (currently 348 — i.e. 348 of 351 MA municipalities have at least one Synthea patient).
- **No more orphan events** — every `respiratory_risk_scores.location_id` joins to a real `dim_location` entry. Verified: 0 orphan events.
- **Dashboard dropdowns finite and meaningful** — the Weather tab's region selector shows ~85 cities (those with ≥ 1000 patients) instead of 781,902 garbage entries.
- **Defensible documentation** — "city values are validated against the official Massachusetts municipality list" is concrete and citable, not a hand-wavy regex.

### Limitation

The dictionary is MA-only. Synthea exports for other states would need
separate per-state lists. Easy to extend (just add another `frozenset`)
but not done here since the project is MA-scoped end-to-end.

---

## Configuration & Secrets

**Nothing sensitive lives in this repo.** Snowflake credentials, Gmail
credentials, and the EC2 host are all read from environment variables or
Airflow Variables at runtime.

### Environment variables (dashboard)

```bash
export SNOWFLAKE_ACCOUNT="<your-account-id>"
export SNOWFLAKE_USER="<your-snowflake-user>"
export SNOWFLAKE_DATABASE="<your-database>"
export SNOWFLAKE_WAREHOUSE="<your-warehouse>"
export SNOWFLAKE_PRIVATE_KEY_FILE="/path/to/rsa_key.p8"
# optional: export SNOWFLAKE_SCHEMA="PUBLIC"
```

The dashboard refuses to start if any required variable is missing — it
shows an error pointing back at this section.

### Airflow Variables

```bash
airflow connections add 'snowflake_default' \
  --conn-type 'snowflake' \
  --conn-login '<snowflake-user>' \
  --conn-extra '{"account": "<your-account-id>", "database": "<db>", "warehouse": "<wh>", "private_key_file": "/path/to/rsa_key.p8"}'

airflow variables set GMAIL_SENDER       "<your-gmail-address>"
airflow variables set GMAIL_RECIPIENT    "<recipient-gmail-address>"
airflow variables set GMAIL_APP_PASSWORD "<your-16-char-app-password>"
airflow variables set DASHBOARD_URL      "http://<EC2_PUBLIC_IP>:8501"
```

Get a Gmail app password at: myaccount.google.com → Security →
2-Step Verification → App passwords.

---

## Setup

### Prerequisites

- AWS EC2 (Amazon Linux 2023; tested on `t3.large` with attached EBS)
- Java 11+ (for Kafka and Spark)
- Python 3.9+
- Spark 3.5.1, Kafka 3.x, Airflow 3.x
- Snowflake account with key-pair auth set up
- S3 bucket with the Synthea CSV exports

### Python dependencies

The dashboard / producer side:

```bash
pip install -r requirements.txt
# Plus (used by the dashboard but not pinned):
pip install snowflake-connector-python pandas branca shapely
```

For the one-time MA town boundary download (used by the dashboard's risk
map):

```bash
pip install geopandas
python3 -c "
import geopandas as gpd
gdf = gpd.read_file('https://www2.census.gov/geo/tiger/GENZ2022/shp/cb_2022_25_cousub_500k.zip')
gdf[['NAME', 'geometry']].to_file('/mnt/synthea_data/cache/ma_towns.geojson', driver='GeoJSON')
"
```

### EBS volume

The pipeline expects an EBS volume mounted at `/mnt/synthea_data/`. After a
reboot:

```bash
sudo mount /dev/nvme1n1 /mnt/synthea_data
```

### One-time setup

You generally don't need to run these manually — `./scripts/start_pipeline.sh`
auto-builds anything missing on first launch. The individual scripts exist
for when you want to rebuild a specific artifact in isolation:

```bash
# Rebuild patient features + cohort parquet (~10 min on full dataset).
# Re-run after changing the EHR scoring or the MA city dictionary.
./scripts/run_build_patient_features.sh

# Rebuild the location manifest (sub-minute).
# Re-run after the patient baseline changes.
./scripts/run_build_location_manifest.sh

# Create the Kafka topic (after Kafka is running). Idempotent.
./scripts/create_kafka_topic.sh
```

After re-running `build_patient_features.py`, also wipe and re-load the
Snowflake table so the dictionary cleanup propagates:

```sql
TRUNCATE TABLE patient_respiratory_features;
COPY INTO patient_respiratory_features
  FROM @patient_features_stage/
  PATTERN = '.*part-.*\.parquet'
  MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
  FILE_FORMAT = (TYPE = PARQUET)
  FORCE = TRUE;
```

The next Airflow run will rebuild `dim_location` from the cleaned table
automatically (`refresh_dim_location` task).

---

## Running the Pipeline

### One-shot launch

```bash
./scripts/start_pipeline.sh
```

**Self-bootstrapping** — safe to run on a fresh EC2 session, after a reboot,
or on top of an already-running pipeline. The script:

1. **Mounts EBS** if not mounted (`/mnt/synthea_data`)
2. **Validates `.env`** exists and strips Windows CRLF if present
3. **Auto-builds the location manifest** if missing (`build_location_manifest.py`, ~1 min)
4. **Auto-builds the patient features + cohort parquet** if missing on S3 (`build_patient_features.py`, ~10 min)
5. **Auto-downloads the MA towns GeoJSON** for the dashboard map if missing (~30 s)
6. **Kills stale processes** from any previous run (Spark stream, producer, streamlit, kafka, zookeeper, airflow)
7. **Sets up a fresh tmux session** named `pipeline` with five windows
8. **Launches all five components** in the right order

| Window | Service |
|---|---|
| `kafka` | ZooKeeper + Kafka broker (creates topic) |
| `producer` | weather_producer (open-meteo mode, 10-min cycle) |
| `stream` | Spark Structured Streaming risk scorer (broadcast join) |
| `dashboard` | Streamlit (sources `.env` for Snowflake creds) |
| `airflow` | `airflow standalone` |

Typical first-run on a fresh instance: ~12–15 min (most of it the cohort
build). Subsequent runs (everything already in place): ~2 min.

```bash
tmux attach -t pipeline   # attach
# Ctrl+B then 0..4 to switch windows
# Ctrl+B then d to detach (leaves everything running)
```

### Manual / step-by-step

```bash
sudo mount /dev/nvme1n1 /mnt/synthea_data

./scripts/start_kafka_services.sh
./scripts/create_kafka_topic.sh
./scripts/run_continuous_weather_puller.sh   # producer (T2)
./scripts/run_stream_risk_scores.sh          # spark stream (T3)
./scripts/run_dashboard.sh                   # streamlit (T4)
airflow standalone                           # airflow (T5)
```

### Stopping safely

```bash
# Spark stream — required before re-running, otherwise the checkpoint lock
# triggers SparkConcurrentModificationException
pkill -f stream_risk_scores.py && sleep 3

# Producer
pkill -f weather_producer.py
```

---

## Project Layout

```
.
├── README.md
├── requirements.txt
├── .gitignore
├── .streamlit/
│   └── config.toml                # theme only; no secrets
├── dags/
│   └── respiratory_pipeline.py    # Airflow DAG (every 10 min)
├── dashboard/
│   └── streamlit_app.py           # Streamlit UI (3 pages)
├── producers/
│   └── weather_producer.py        # Kafka producer (simulated | open-meteo)
├── spark_jobs/
│   ├── build_patient_baseline.py  # simple baseline → location_manifest input
│   ├── build_location_manifest.py # per-location summary for the producer
│   ├── build_patient_features.py  # full EHR feature build + EHR scoring
│   └── stream_risk_scores.py      # Kafka → join → score → parquet
└── scripts/
    ├── start_pipeline.sh          # one-shot tmux launcher
    ├── start_kafka_services.sh    # ZooKeeper + Kafka
    ├── create_kafka_topic.sh
    ├── run_*.sh                   # individual component launchers
    └── extract_synthea_*.sh       # one-time data prep
```
