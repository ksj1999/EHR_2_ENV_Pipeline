"""Build the simple patient baseline parquet from Synthea CSV exports.

Produces a small per-patient table (demographics, location_id, a few chronic
condition flags, a coarse baseline_risk number). Used as the input to
build_location_manifest.py. The richer feature set used by the streaming
risk scorer lives in build_patient_features.py.
"""

import argparse
import glob
import os

from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import StringType
from pyspark.sql.utils import AnalysisException


def parse_args():
    parser = argparse.ArgumentParser(
        description="Build a patient baseline table from Synthea CSV exports."
    )
    parser.add_argument(
        "--input-dir",
        required=True,
        help="Directory containing Synthea CSV exports like patients.csv and conditions.csv.",
    )
    parser.add_argument(
        "--output-dir",
        required=True,
        help="Directory where the patient baseline Parquet output will be written.",
    )
    return parser.parse_args()


def read_csv(spark, path):
    return (
        spark.read.option("header", True)
        .option("inferSchema", True)
        .csv(path)
    )


def find_input_paths(input_dir, file_name):
    direct_path = os.path.join(input_dir, file_name)
    if os.path.exists(direct_path):
        return [direct_path]

    recursive_matches = sorted(
        glob.glob(os.path.join(input_dir, "**", file_name), recursive=True)
    )

    if recursive_matches:
        return recursive_matches

    raise FileNotFoundError(
        f"Could not find {file_name} under {input_dir}. "
        "Place the Synthea CSV export in the input directory or a nested folder."
    )


def require_columns(df, name, required_columns):
    missing = [column for column in required_columns if column not in df.columns]
    if missing:
        raise ValueError(f"{name} is missing required columns: {', '.join(missing)}")


def create_location_id(city_col, state_col):
    cleaned_city = F.regexp_replace(F.lower(F.coalesce(city_col, F.lit("unknown"))), r"[^a-z0-9]+", "_")
    cleaned_state = F.regexp_replace(F.lower(F.coalesce(state_col, F.lit("unknown"))), r"[^a-z0-9]+", "_")
    return F.concat_ws("_", cleaned_city, cleaned_state)


def build_patients_df(spark, patients_paths, patients_csv_df):
    modern_columns = {
        "Id",
        "BIRTHDATE",
        "GENDER",
        "RACE",
        "ETHNICITY",
        "CITY",
        "STATE",
        "COUNTY",
        "ZIP",
        "LAT",
        "LON",
    }
    legacy_columns = {"ID", "BIRTHDATE", "RACE", "ETHNICITY", "GENDER", "BIRTHPLACE", "ADDRESS"}

    if modern_columns.issubset(set(patients_csv_df.columns)):
        return (
            patients_csv_df.select(
                F.col("Id").alias("patient_id"),
                F.to_date("BIRTHDATE").alias("birth_date"),
                F.col("GENDER").alias("gender"),
                F.col("RACE").alias("race"),
                F.col("ETHNICITY").alias("ethnicity"),
                F.col("CITY").alias("city"),
                F.col("STATE").alias("state"),
                F.col("COUNTY").alias("county"),
                F.col("ZIP").cast("string").alias("zip"),
                F.col("LAT").cast("double").alias("lat"),
                F.col("LON").cast("double").alias("lon"),
            )
        )

    if legacy_columns.issubset(set(patients_csv_df.columns)):
        uuid_pattern = r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12},"
        address_pattern = r".*? ([A-Za-z .'-]+) ([A-Z]{2}) ([0-9]{5}) US$"

        raw_patients_df = spark.read.text(patients_paths)
        parsed_parts = F.split(F.col("value"), ",", -1)

        legacy_df = (
            raw_patients_df.filter(F.col("value").rlike(uuid_pattern))
            .withColumn("parts", parsed_parts)
            .filter(F.size(F.col("parts")) == 17)
            .select(
                F.element_at("parts", 1).alias("patient_id"),
                F.to_date(F.element_at("parts", 2)).alias("birth_date"),
                F.element_at("parts", 15).alias("gender"),
                F.element_at("parts", 13).alias("race"),
                F.element_at("parts", 14).alias("ethnicity"),
                F.regexp_extract(F.element_at("parts", 17), address_pattern, 1).alias("city"),
                F.regexp_extract(F.element_at("parts", 17), address_pattern, 2).alias("state"),
                F.lit(None).cast(StringType()).alias("county"),
                F.regexp_extract(F.element_at("parts", 17), address_pattern, 3).alias("zip"),
                F.lit(None).cast("double").alias("lat"),
                F.lit(None).cast("double").alias("lon"),
            )
        )

        return legacy_df

    raise ValueError(
        "patients.csv schema is not recognized. "
        f"Columns found: {', '.join(patients_csv_df.columns)}"
    )


def build_condition_flags(conditions_df):
    condition_text = F.upper(F.coalesce(F.col("DESCRIPTION"), F.lit("")))

    return (
        conditions_df.select("PATIENT", condition_text.alias("condition_text"))
        .groupBy("PATIENT")
        .agg(
            F.max(F.when(F.col("condition_text").contains("ASTHMA"), 1).otherwise(0)).alias("has_asthma"),
            F.max(
                F.when(
                    F.col("condition_text").contains("CHRONIC OBSTRUCTIVE")
                    | F.col("condition_text").contains("COPD")
                    | F.col("condition_text").contains("EMPHYSEMA"),
                    1,
                ).otherwise(0)
            ).alias("has_copd"),
            F.max(
                F.when(
                    F.col("condition_text").contains("HEART FAILURE")
                    | F.col("condition_text").contains("CONGESTIVE HEART FAILURE"),
                    1,
                ).otherwise(0)
            ).alias("has_heart_failure"),
            F.max(
                F.when(F.col("condition_text").contains("HYPERTENSION"), 1).otherwise(0)
            ).alias("has_hypertension"),
        )
    )


def main():
    args = parse_args()

    spark = (
        SparkSession.builder.appName("build_patient_baseline")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )

    patients_paths = find_input_paths(args.input_dir.rstrip("/"), "patients.csv")
    conditions_paths = find_input_paths(args.input_dir.rstrip("/"), "conditions.csv")

    print(f"Found {len(patients_paths)} patients.csv file(s)")
    for path in patients_paths:
        print(f"  patients: {path}")

    print(f"Found {len(conditions_paths)} conditions.csv file(s)")
    for path in conditions_paths:
        print(f"  conditions: {path}")

    try:
        patients_df = read_csv(spark, patients_paths)
        conditions_df = read_csv(spark, conditions_paths)
    except (AnalysisException, FileNotFoundError) as exc:
        raise FileNotFoundError(
            "Could not read Synthea files. Expected patients.csv and conditions.csv "
            f"under {args.input_dir}."
        ) from exc

    require_columns(conditions_df, "conditions.csv", ["PATIENT", "DESCRIPTION"])

    condition_flags_df = build_condition_flags(conditions_df)
    patients_clean_df = (
        build_patients_df(spark, patients_paths, patients_df)
        .withColumn(
            "age",
            F.floor(F.months_between(F.current_date(), F.col("birth_date")) / F.lit(12)),
        )
        .withColumn("location_id", create_location_id(F.col("city"), F.col("state")))
    )

    baseline_df = (
        patients_clean_df.join(
            condition_flags_df,
            patients_clean_df.patient_id == condition_flags_df.PATIENT,
            "left",
        )
        .drop("PATIENT")
        .fillna(
            {
                "has_asthma": 0,
                "has_copd": 0,
                "has_heart_failure": 0,
                "has_hypertension": 0,
            }
        )
        .withColumn(
            "baseline_risk",
            F.round(
                F.lit(0.05)
                + F.when(F.col("age") >= 65, 0.12).otherwise(0.0)
                + F.when(F.col("has_asthma") == 1, 0.18).otherwise(0.0)
                + F.when(F.col("has_copd") == 1, 0.25).otherwise(0.0)
                + F.when(F.col("has_heart_failure") == 1, 0.22).otherwise(0.0)
                + F.when(F.col("has_hypertension") == 1, 0.10).otherwise(0.0),
                3,
            ),
        )
    )

    baseline_df.write.mode("overwrite").parquet(args.output_dir)

    row_count = baseline_df.count()
    print(f"Wrote {row_count} patient baseline rows to {args.output_dir}")

    spark.stop()


if __name__ == "__main__":
    main()
