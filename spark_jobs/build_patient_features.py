"""Build patient_respiratory_features parquet from Synthea EHR exports.

Reads patients/conditions/encounters/medications CSV shards from S3, derives
per-patient feature flags within a configurable lookback window (default 90d),
and computes an EHR baseline risk score (0–25, capped). Output is written as
Parquet for the streaming risk-scoring job to join against.
"""

import argparse
from datetime import date

from pyspark.sql import SparkSession, functions as F


def parse_args():
    parser = argparse.ArgumentParser(
        description="Build patient respiratory risk features from raw Synthea EHR tables."
    )
    parser.add_argument(
        "--input-dir",
        required=True,
        help="S3 root containing output_*/output_*/csv/ subdirectories.",
    )
    parser.add_argument(
        "--output-dir",
        required=True,
        help="Output path for PATIENT_RESPIRATORY_FEATURES parquet.",
    )
    parser.add_argument(
        "--lookback-days",
        type=int,
        default=90,
        help="Days to look back for recent encounters and conditions (default 90).",
    )
    parser.add_argument(
        "--reference-date",
        default=None,
        help="Reference date for lookback window (YYYY-MM-DD). "
             "Defaults to max encounter date in dataset.",
    )
    return parser.parse_args()


def read_table(spark, input_dir, table_name):
    path = f"{input_dir.rstrip('/')}/*/*/csv/{table_name}.csv"
    return (
        spark.read
        .option("header", True)
        .option("inferSchema", True)
        .csv(path)
    )


def resolve_date_col(df, *candidates):
    for col in candidates:
        if col in df.columns:
            return col
    raise ValueError(
        f"Expected one of {candidates} in columns: {df.columns}"
    )


def build_condition_features(conditions_df, reference_date, lookback_days):
    date_col = resolve_date_col(conditions_df, "START", "DATE")

    df = (
        conditions_df
        .withColumn("_desc", F.upper(F.coalesce(F.col("DESCRIPTION"), F.lit(""))))
        .withColumn(
            "_is_recent",
            F.datediff(F.lit(reference_date), F.to_date(F.col(date_col))) <= lookback_days,
        )
    )

    return (
        df.groupBy("PATIENT")
        .agg(
            F.max(F.when(F.col("_desc").rlike("ASTHMA"), 1).otherwise(0))
             .alias("has_asthma"),
            F.max(F.when(F.col("_desc").rlike("CHRONIC OBSTRUCTIVE|\\bCOPD\\b|EMPHYSEMA"), 1).otherwise(0))
             .alias("has_copd"),
            F.max(F.when(F.col("_desc").rlike("EMPHYSEMA"), 1).otherwise(0))
             .alias("has_emphysema"),
            F.max(F.when(F.col("_desc").rlike("CHRONIC BRONCHITIS"), 1).otherwise(0))
             .alias("has_chronic_bronchitis"),
            F.max(F.when(F.col("_desc").rlike("PNEUMONIA") & F.col("_is_recent"), 1).otherwise(0))
             .alias("recent_pneumonia"),
            F.max(
                F.when(
                    F.col("_desc").rlike("SINUSITIS|RESPIRATORY INFECTION") & F.col("_is_recent"),
                    1,
                ).otherwise(0)
            ).alias("recent_respiratory_infection"),
        )
    )


def build_encounter_features(encounters_df, reference_date, lookback_days):
    date_col = resolve_date_col(encounters_df, "DATE", "START")

    df = (
        encounters_df
        .withColumn("_desc", F.upper(F.coalesce(F.col("DESCRIPTION"), F.lit(""))))
        .withColumn("_reason", F.upper(F.coalesce(F.col("REASONDESCRIPTION"), F.lit(""))))
        .withColumn(
            "_is_recent",
            F.datediff(F.lit(reference_date), F.to_date(F.col(date_col))) <= lookback_days,
        )
        .filter(F.col("_is_recent"))
    )

    return (
        df.groupBy("PATIENT")
        .agg(
            F.count_if(F.col("_desc").rlike("EMERGENCY HOSPITAL ADMISSION FOR ASTHMA"))
             .alias("asthma_emergency_admission_count"),
            F.count_if(F.col("_desc").rlike("EMERGENCY"))
             .alias("emergency_encounter_count"),
            F.count_if(F.col("_desc").rlike("INPATIENT|HOSPITAL ADMISSION"))
             .alias("hospital_admission_count"),
            F.count_if(F.col("_desc").rlike("ASTHMA FOLLOW"))
             .alias("asthma_followup_count"),
            F.count_if(
                F.col("_reason").rlike("ASTHMA|SINUSITIS|BRONCHITIS|PNEUMONIA|RESPIRATORY")
            ).alias("respiratory_reason_count"),
        )
    )


def build_medication_features(medications_df, reference_date, lookback_days):
    date_col = resolve_date_col(medications_df, "START", "DATE")

    # A medication is "active" if it started before the reference date AND
    # either has no stop date (ongoing) or stopped within the lookback window.
    # Filtering by start-date-only misses chronic meds prescribed years ago.
    has_stop_col = "STOP" in medications_df.columns
    stop_expr = (
        F.col("STOP").isNull() | (F.datediff(F.lit(reference_date), F.to_date(F.col("STOP"))) <= lookback_days)
        if has_stop_col
        else F.lit(True)
    )

    df = (
        medications_df
        .withColumn("_desc", F.upper(F.coalesce(F.col("DESCRIPTION"), F.lit(""))))
        .filter(F.to_date(F.col(date_col)) <= F.lit(reference_date))
        .filter(stop_expr)
    )

    return (
        df.groupBy("PATIENT")
        .agg(
            F.max(
                F.when(
                    F.col("_desc").rlike(
                        "OXYGEN|SUPPLEMENTAL OXYGEN|HOME OXYGEN|OXYGEN THERAPY"
                    ),
                    1,
                ).otherwise(0)
            ).alias("uses_oxygen"),
            F.max(
                F.when(
                    F.col("_desc").rlike(
                        "PREDNISONE|PREDNISOLONE|METHYLPREDNISOLONE|DEXAMETHASONE|HYDROCORTISONE"
                    ),
                    1,
                ).otherwise(0)
            ).alias("uses_steroid"),
            F.max(
                F.when(
                    F.col("_desc").rlike(
                        "ALBUTEROL|SALBUTAMOL|IPRATROPIUM|TIOTROPIUM|FORMOTEROL|SALMETEROL"
                        "|LEVALBUTEROL|FLUTICASONE|BUDESONIDE|BECLOMETHASONE|INHALER"
                        "|BRONCHODILATOR"
                    ),
                    1,
                ).otherwise(0)
            ).alias("uses_inhaler"),
            F.max(
                F.when(
                    F.col("_desc").rlike("THEOPHYLLINE|MONTELUKAST|ZAFIRLUKAST|ROFLUMILAST"),
                    1,
                ).otherwise(0)
            ).alias("uses_respiratory_med"),
        )
    )


def compute_ehr_risk_score(df):
    """Patient baseline vulnerability score (0–25, capped).

    Design principles:
    - COPD / emphysema / chronic-bronchitis are the same underlying spectrum;
      take the strongest flag rather than summing (no triple-counting).
    - Encounter counts are capped per term so frequent flyers don't dominate.
    - Hard cap of 25 prevents extreme outliers from skewing population stats.
    """
    # Age bands
    age_score = (
        F.when(F.col("age") >= 75, 3)
        .when(F.col("age") >= 65, 2)
        .when(F.col("age") >= 50, 1)
        .otherwise(0)
    )

    # COPD-spectrum: take max instead of sum to avoid triple-counting
    copd_spectrum = F.greatest(
        F.coalesce(F.col("has_copd"),               F.lit(0)),
        F.coalesce(F.col("has_emphysema"),          F.lit(0)),
        F.coalesce(F.col("has_chronic_bronchitis"), F.lit(0)),
    ) * 4
    asthma_score = F.coalesce(F.col("has_asthma"), F.lit(0)) * 3
    # Recent infection capped — pneumonia + recent infection rarely co-occur but cap anyway
    infection_score = F.least(
        F.coalesce(F.col("recent_pneumonia"),             F.lit(0)) * 3
        + F.coalesce(F.col("recent_respiratory_infection"), F.lit(0)) * 1,
        F.lit(3),
    )
    condition_score = copd_spectrum + asthma_score + infection_score

    # Encounter history — cap each term so frequent ED users don't get score 50+
    encounter_score = (
        F.least(F.coalesce(F.col("asthma_emergency_admission_count"), F.lit(0)) * 5, F.lit(8))
        + F.least(F.coalesce(F.col("emergency_encounter_count"),      F.lit(0)) * 2, F.lit(4))
        + F.least(F.coalesce(F.col("hospital_admission_count"),       F.lit(0)) * 2, F.lit(4))
        + F.least(F.coalesce(F.col("asthma_followup_count"),          F.lit(0)) * 1, F.lit(2))
    )

    # Medication intensity — oxygen is the strongest severity signal
    medication_score = (
        F.coalesce(F.col("uses_oxygen"),  F.lit(0)) * 4
        + F.coalesce(F.col("uses_steroid"), F.lit(0)) * 2
        + F.coalesce(F.col("uses_inhaler"), F.lit(0)) * 1
    )

    # Hard cap to keep distribution sane
    total = F.least(
        age_score + condition_score + encounter_score + medication_score,
        F.lit(25),
    )

    return (
        df
        .withColumn("age_score", age_score)
        .withColumn("condition_score", condition_score)
        .withColumn("encounter_score", encounter_score)
        .withColumn("medication_score", medication_score)
        .withColumn("ehr_risk_score", total)
        .withColumn(
            "ehr_risk_level",
            F.when(F.col("ehr_risk_score") >= 12, F.lit("high"))
            .when(F.col("ehr_risk_score") >=  6, F.lit("medium"))
            .otherwise(F.lit("low")),
        )
    )


def main():
    args = parse_args()

    spark = (
        SparkSession.builder.appName("build_patient_features")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )

    print("Reading EHR tables from S3...")
    patients_df    = read_table(spark, args.input_dir, "patients")
    conditions_df  = read_table(spark, args.input_dir, "conditions")
    encounters_df  = read_table(spark, args.input_dir, "encounters")
    medications_df = read_table(spark, args.input_dir, "medications")

    if args.reference_date:
        reference_date = date.fromisoformat(args.reference_date)
        print(f"Using provided reference date: {reference_date}")
    else:
        enc_date_col = resolve_date_col(encounters_df, "DATE", "START")
        reference_date = (
            encounters_df
            .agg(F.max(F.to_date(F.col(enc_date_col))).alias("max_date"))
            .collect()[0]["max_date"]
        )
        print(f"Using max encounter date as reference: {reference_date}")

    print(f"Lookback window: {args.lookback_days} days from {reference_date}")

    condition_features  = build_condition_features(conditions_df, reference_date, args.lookback_days)
    encounter_features  = build_encounter_features(encounters_df, reference_date, args.lookback_days)
    medication_features = build_medication_features(medications_df, reference_date, args.lookback_days)

    # Synthea ships in two formats:
    # Modern (has CITY/STATE/ZIP/LAT/LON columns directly)
    # Legacy (17-col: city/state/zip embedded in ADDRESS string)
    addr_pattern = r".*? ([A-Za-z .'\-]+) ([A-Z]{2}) ([0-9]{5}) US$"
    id_col = "Id" if "Id" in patients_df.columns else "ID"

    if "CITY" in patients_df.columns:
        patients_clean = (
            patients_df
            .select(
                F.col(id_col).alias("patient_id"),
                F.floor(
                    F.months_between(F.lit(reference_date), F.to_date(F.col("BIRTHDATE"))) / 12
                ).cast("int").alias("age"),
                F.col("GENDER").alias("gender"),
                F.col("RACE").alias("race"),
                F.col("ETHNICITY").alias("ethnicity"),
                F.col("CITY").alias("city"),
                F.col("STATE").alias("state"),
                F.col("ZIP").cast("string").alias("zip"),
                F.col("LAT").cast("double").alias("lat"),
                F.col("LON").cast("double").alias("lon"),
            )
        )
    else:
        # Legacy 17-column format: parse city/state/zip from ADDRESS
        patients_clean = (
            patients_df
            .select(
                F.col(id_col).alias("patient_id"),
                F.floor(
                    F.months_between(F.lit(reference_date), F.to_date(F.col("BIRTHDATE"))) / 12
                ).cast("int").alias("age"),
                F.col("GENDER").alias("gender"),
                F.col("RACE").alias("race"),
                F.col("ETHNICITY").alias("ethnicity"),
                F.regexp_extract(F.col("ADDRESS"), addr_pattern, 1).alias("city"),
                F.regexp_extract(F.col("ADDRESS"), addr_pattern, 2).alias("state"),
                F.regexp_extract(F.col("ADDRESS"), addr_pattern, 3).alias("zip"),
                F.lit(None).cast("double").alias("lat"),
                F.lit(None).cast("double").alias("lon"),
            )
        )

    patients_clean = patients_clean.withColumn(
        "location_id",
        F.concat_ws(
            "_",
            F.regexp_replace(F.lower(F.coalesce(F.col("city"), F.lit("unknown"))), r"[^a-z0-9]+", "_"),
            F.regexp_replace(F.lower(F.coalesce(F.col("state"), F.lit("unknown"))), r"[^a-z0-9]+", "_"),
        ),
    )

    features_df = (
        patients_clean
        .join(condition_features.withColumnRenamed("PATIENT", "patient_id"),  "patient_id", "left")
        .join(encounter_features.withColumnRenamed("PATIENT", "patient_id"),  "patient_id", "left")
        .join(medication_features.withColumnRenamed("PATIENT", "patient_id"), "patient_id", "left")
        .fillna(0, subset=[
            "has_asthma", "has_copd", "has_emphysema", "has_chronic_bronchitis",
            "recent_pneumonia", "recent_respiratory_infection",
            "asthma_emergency_admission_count", "emergency_encounter_count",
            "hospital_admission_count", "asthma_followup_count", "respiratory_reason_count",
            "uses_oxygen", "uses_steroid", "uses_inhaler", "uses_respiratory_med",
        ])
    )

    scored_df = compute_ehr_risk_score(features_df).withColumn(
        "last_updated", F.lit(str(reference_date))
    )

    final_df = scored_df.select(
        "patient_id", "age", "gender", "race", "ethnicity",
        "city", "state", "zip", "lat", "lon", "location_id",
        "has_asthma", "has_copd", "has_emphysema", "has_chronic_bronchitis",
        "recent_pneumonia", "recent_respiratory_infection",
        "asthma_emergency_admission_count", "emergency_encounter_count",
        "hospital_admission_count", "asthma_followup_count", "respiratory_reason_count",
        "uses_oxygen", "uses_steroid", "uses_inhaler", "uses_respiratory_med",
        "age_score", "condition_score", "encounter_score", "medication_score",
        "ehr_risk_score", "ehr_risk_level", "last_updated",
    )

    final_df.write.mode("overwrite").parquet(args.output_dir)

    # Re-read from disk so the level-count summary doesn't trigger 3 full
    # re-scans of S3 (each .count() on `final_df` would otherwise re-execute
    # the whole DAG).
    summary_df = spark.read.parquet(args.output_dir).cache()
    total  = summary_df.count()
    high   = summary_df.filter(F.col("ehr_risk_level") == "high").count()
    medium = summary_df.filter(F.col("ehr_risk_level") == "medium").count()
    low    = summary_df.filter(F.col("ehr_risk_level") == "low").count()

    print(f"Wrote {total:,} patient feature rows to {args.output_dir}")
    print(f"  HIGH:   {high:,}  ({100*high/total:.1f}%)")
    print(f"  MEDIUM: {medium:,}  ({100*medium/total:.1f}%)")
    print(f"  LOW:    {low:,}  ({100*low/total:.1f}%)")

    spark.stop()


if __name__ == "__main__":
    main()
