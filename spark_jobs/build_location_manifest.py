"""Build the location manifest CSV consumed by the weather producer.

Groups the patient baseline by (location_id, city, state) and emits one row
per location with patient_count and a representative zip/lat/lon. The Kafka
producer iterates this manifest to fetch weather/AQI per location.
"""

import argparse

from pyspark.sql import SparkSession, functions as F


def parse_args():
    parser = argparse.ArgumentParser(
        description="Build a location manifest from the patient baseline Parquet."
    )
    parser.add_argument("--input-dir", required=True, help="Patient baseline Parquet directory.")
    parser.add_argument("--output-dir", required=True, help="Output directory for CSV manifest.")
    return parser.parse_args()


def main():
    args = parse_args()

    spark = (
        SparkSession.builder.appName("build_location_manifest")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )

    baseline_df = spark.read.parquet(args.input_dir)

    manifest_df = (
        baseline_df.filter(F.col("location_id").isNotNull())
        .groupBy("location_id", "city", "state")
        .agg(
            F.first("zip").alias("zip"),
            F.first("lat").alias("lat"),
            F.first("lon").alias("lon"),
            F.count("*").alias("patient_count"),
        )
        .orderBy(F.desc("patient_count"), F.asc("location_id"))
    )

    manifest_df.coalesce(1).write.mode("overwrite").option("header", True).csv(args.output_dir)

    print(f"Wrote {manifest_df.count()} locations to {args.output_dir}")

    spark.stop()


if __name__ == "__main__":
    main()
