"""Quick inspector for the patient_baseline parquet output.

Prints row count, schema, and 10 sample rows. Useful for sanity-checking
the output of build_patient_baseline.py without spinning up the dashboard.
"""

import argparse

from pyspark.sql import SparkSession


def parse_args():
    parser = argparse.ArgumentParser(
        description="Read and preview the patient baseline Parquet output."
    )
    parser.add_argument(
        "--input-dir",
        required=True,
        help="Directory containing the patient baseline Parquet files.",
    )
    return parser.parse_args()


def main():
    args = parse_args()

    spark = (
        SparkSession.builder.appName("read_patient_baseline")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )

    df = spark.read.parquet(args.input_dir)

    print(f"Reading Parquet from {args.input_dir}")
    print(f"Row count: {df.count()}")
    print("Schema:")
    df.printSchema()
    print("Sample rows:")
    df.orderBy("patient_id").show(10, truncate=False)

    spark.stop()


if __name__ == "__main__":
    main()
