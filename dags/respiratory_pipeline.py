"""Airflow DAG: respiratory_risk_pipeline.

Runs every 10 minutes. Syncs Spark Streaming output (EBS → S3), loads the
new parquet files into Snowflake via COPY INTO, generates HIGH-risk alerts
for patients not already alerted in the last 24h, and emails a summary if
new alerts fired.

Schedule: */10 * * * *  (cron)  — see `schedule=` in the DAG block below.
Connections used:
  - snowflake_default  (key-pair auth)
  - GMAIL_*            Airflow Variables for SMTP credentials
"""

import smtplib
from datetime import datetime, timedelta
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

from airflow import DAG
from airflow.models import Variable
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.standard.operators.python import PythonOperator

default_args = {
    "owner": "ec2-user",
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
    "email_on_failure": False,
}

with DAG(
    dag_id="respiratory_risk_pipeline",
    default_args=default_args,
    description=(
        "Batch layer: syncs Spark Streaming output from EBS to S3, "
        "loads risk scores and EHR features into Snowflake, generates alerts."
    ),
    schedule="*/10 * * * *",
    start_date=datetime(2026, 4, 23),
    catchup=False,
    tags=["respiratory", "streaming", "snowflake", "s3"],
) as dag:

    # -----------------------------------------------------------------------
    # Task 1 — Sync streaming risk scores from EBS to S3
    # -----------------------------------------------------------------------
    sync_risk_scores = BashOperator(
        task_id="sync_risk_scores_to_s3",
        bash_command=(
            "mkdir -p /mnt/synthea_data/output/respiratory_patient_features && "
            "aws s3 sync "
            "/mnt/synthea_data/output/respiratory_patient_features "
            "s3://synthea-full-bucket/processed/respiratory_patient_features "
            '--exclude "_spark_metadata*"'
        ),
    )

    # -----------------------------------------------------------------------
    # Task 2 — Sync location manifest from EBS to S3
    # -----------------------------------------------------------------------
    sync_location_manifest = BashOperator(
        task_id="sync_location_manifest_to_s3",
        bash_command=(
            "aws s3 sync "
            "/mnt/synthea_data/output/location_manifest "
            "s3://synthea-full-bucket/processed/location_manifest "
            '--exclude "_SUCCESS"'
        ),
    )

    # -----------------------------------------------------------------------
    # Task 3 — COPY new risk score Parquet files into Snowflake
    # -----------------------------------------------------------------------
    load_risk_scores = SQLExecuteQueryOperator(
        task_id="load_risk_scores_to_snowflake",
        conn_id="snowflake_default",
        sql="""
            COPY INTO respiratory_risk_scores
              FROM @synthea_stage/respiratory_patient_features/
              PATTERN = '.*\\.parquet'
              MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
              FILE_FORMAT = (TYPE = PARQUET)
              FORCE = FALSE;
        """,
    )

    # -----------------------------------------------------------------------
    # Task 4 — COPY EHR patient features into Snowflake (no-op when unchanged)
    # -----------------------------------------------------------------------
    load_patient_features = SQLExecuteQueryOperator(
        task_id="load_patient_features_to_snowflake",
        conn_id="snowflake_default",
        sql="""
            COPY INTO patient_respiratory_features
              FROM @patient_features_stage/
              PATTERN = '.*part-.*\\.parquet'
              MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
              FILE_FORMAT = (TYPE = PARQUET)
              FORCE = FALSE;
        """,
    )

    # -----------------------------------------------------------------------
    # Task 5 — Generate alerts for HIGH risk patients not alerted in 24h
    # -----------------------------------------------------------------------
    generate_alerts = SQLExecuteQueryOperator(
        task_id="generate_high_risk_alerts",
        conn_id="snowflake_default",
        sql="""
            INSERT INTO respiratory_alerts (
              patient_id, alert_time, event_time, location_id,
              final_risk_score, ehr_risk_score, weather_risk_score,
              aqi, pm25, ozone,
              age, has_asthma, has_copd, uses_oxygen
            )
            SELECT
              r.patient_id,
              CURRENT_TIMESTAMP()   AS alert_time,
              r.event_time,
              r.location_id,
              r.final_risk_score,
              p.ehr_risk_score,
              r.weather_risk_score,
              r.aqi,
              r.pm25,
              r.ozone,
              p.age,
              p.has_asthma,
              p.has_copd,
              p.uses_oxygen
            FROM respiratory_risk_scores_current r
            JOIN patient_respiratory_features p ON r.patient_id = p.patient_id
            WHERE r.final_risk_level = 'high'
              AND r.patient_id NOT IN (
                SELECT patient_id
                FROM respiratory_alerts
                WHERE alert_time >= DATEADD(hour, -24, CURRENT_TIMESTAMP())
              );
        """,
    )

    # -----------------------------------------------------------------------
    # Task 6 — Send summary email if new alerts were generated this run
    # -----------------------------------------------------------------------
    def send_alert_email():
        import snowflake.connector

        sender    = Variable.get("GMAIL_SENDER")
        recipient = Variable.get("GMAIL_RECIPIENT")
        password  = Variable.get("GMAIL_APP_PASSWORD")

        conn = snowflake.connector.connect(
            account="UNB02139",
            user="KANGAROO",
            private_key_file="/home/ec2-user/.snowflake_keys/rsa_key.p8",
            database="KANGAROO_DB",
            warehouse="KANGAROO_WH",
            schema="PUBLIC",
        )
        cur = conn.cursor()

        # New alerts from the last 15 minutes
        cur.execute("""
            SELECT COUNT(*) FROM respiratory_alerts
            WHERE alert_time >= DATEADD(minute, -15, CURRENT_TIMESTAMP())
        """)
        new_count = cur.fetchone()[0]

        if new_count == 0:
            print("No new alerts this run — skipping email.")
            conn.close()
            return

        # Top locations by patient count
        cur.execute("""
            SELECT location_id, COUNT(*) AS patients,
                   ROUND(AVG(final_risk_score), 1) AS avg_score,
                   ROUND(AVG(aqi), 0) AS avg_aqi
            FROM respiratory_alerts
            WHERE alert_time >= DATEADD(minute, -15, CURRENT_TIMESTAMP())
            GROUP BY location_id
            ORDER BY patients DESC
            LIMIT 5
        """)
        top_locations = cur.fetchall()

        # Worst individual cases
        cur.execute("""
            SELECT patient_id, location_id, final_risk_score,
                   ehr_risk_score, weather_risk_score, aqi, age,
                   has_asthma, has_copd, uses_oxygen
            FROM respiratory_alerts
            WHERE alert_time >= DATEADD(minute, -15, CURRENT_TIMESTAMP())
            ORDER BY final_risk_score DESC
            LIMIT 5
        """)
        worst = cur.fetchall()
        conn.close()

        # Build email body
        now_ct = datetime.utcnow().strftime("%Y-%m-%d %H:%M UTC")

        loc_lines = "\n".join(
            f"  {r[0]:<30} {r[1]:>4} patients   avg score {r[2]}   avg AQI {int(r[3])}"
            for r in top_locations
        )

        def flags(row):
            parts = []
            if row[7]: parts.append("asthma")
            if row[8]: parts.append("COPD")
            if row[9]: parts.append("O2")
            return "+".join(parts) if parts else "none"

        case_lines = "\n".join(
            f"  {r[0]}  score {r[2]} (EHR {r[3]} + weather {r[4]})  AQI {r[5]}  age {r[6]}  [{flags(r)}]"
            for r in worst
        )

        body = f"""
{new_count} new HIGH-risk respiratory patient(s) detected at {now_ct}

TOP LOCATIONS
{loc_lines}

WORST CASES
{case_lines}

Dashboard: http://3.136.179.41:8501
        """.strip()

        msg = MIMEMultipart()
        msg["From"]    = sender
        msg["To"]      = recipient
        msg["Subject"] = f"[RESPIRATORY ALERT] {new_count} new HIGH-risk patients — {now_ct}"
        msg.attach(MIMEText(body, "plain"))

        with smtplib.SMTP("smtp.gmail.com", 587) as server:
            server.starttls()
            server.login(sender, password)
            server.sendmail(sender, recipient, msg.as_string())

        print(f"Alert email sent: {new_count} new HIGH-risk patients.")

    send_email = PythonOperator(
        task_id="send_alert_email",
        python_callable=send_alert_email,
    )

    # -----------------------------------------------------------------------
    # Task 7 — Row count check
    # -----------------------------------------------------------------------
    row_count_check = SQLExecuteQueryOperator(
        task_id="row_count_check",
        conn_id="snowflake_default",
        sql="""
            SELECT 'respiratory_risk_scores'      AS table_name, COUNT(*) AS total_rows FROM respiratory_risk_scores
            UNION ALL
            SELECT 'patient_respiratory_features' AS table_name, COUNT(*) AS total_rows FROM patient_respiratory_features
            UNION ALL
            SELECT 'respiratory_alerts'           AS table_name, COUNT(*) AS total_rows FROM respiratory_alerts;
        """,
    )

    # -----------------------------------------------------------------------
    # Dependencies
    #
    #   sync_risk_scores ──┐
    #                      ├──► load_risk_scores ──┐
    #   sync_location  ────┘                       ├──► generate_alerts ──► send_email ──► row_count_check
    #                         load_patient_features ┘
    # -----------------------------------------------------------------------
    [sync_risk_scores, sync_location_manifest] >> load_risk_scores
    [load_risk_scores, load_patient_features] >> generate_alerts
    generate_alerts >> send_email >> row_count_check
