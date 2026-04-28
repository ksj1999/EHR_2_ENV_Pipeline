"""Streaming respiratory risk scorer.

Reads weather events from Kafka (`environment_raw` by default), joins them to
the static patient_respiratory_features parquet on `location_id`, and writes
per-patient per-event risk scores back to Parquet (which is later synced to
S3 → Snowflake by the Airflow DAG).

Final score combines four signals:
  EHR baseline + weather components + EHR×weather interaction
                                    + condition-specific sensitivity bonus
See compute_weather_components() for thresholds and main() for the assembly.
"""

import argparse

from pyspark.sql import SparkSession, functions as F, types as T


def parse_args():
    parser = argparse.ArgumentParser(
        description="Read weather events from Kafka, join to EHR features, and write risk scores."
    )
    parser.add_argument("--bootstrap-servers", default="localhost:9092")
    parser.add_argument("--topic", default="environment_raw")
    parser.add_argument(
        "--features-dir",
        required=True,
        help="Path to patient_respiratory_features parquet (output of build_patient_features.py).",
    )
    parser.add_argument("--output-dir", required=True)
    parser.add_argument("--checkpoint-dir", required=True)
    parser.add_argument("--starting-offsets", default="latest")
    parser.add_argument(
        "--available-now",
        action="store_true",
        help="Process available records and stop.",
    )
    return parser.parse_args()


def compute_weather_components():
    """Component scores for each environmental factor.

    Returned as a dict so the caller can use them for condition-specific
    weighting (asthma → ozone, COPD → particulates) without re-deriving them.

    Notes on de-duplication:
    - AQI and PM2.5 are highly correlated (AQI is often computed from PM2.5).
      We take the MAX of the two as a single 'particulate_score' to avoid
      double-counting the same dust/smoke event.
    """
    aqi = F.coalesce(F.col("aqi"), F.lit(0))
    aqi_score = (
        F.when(aqi > 300, 6)   # hazardous
        .when(aqi > 200, 5)    # very unhealthy
        .when(aqi > 150, 4)    # unhealthy
        .when(aqi > 100, 3)    # unhealthy for sensitive groups
        .when(aqi >  75, 2)    # upper moderate
        .when(aqi >  50, 1)    # moderate
        .otherwise(0)
    )

    pm25 = F.coalesce(F.col("pm25"), F.lit(0.0))
    pm25_score = (
        F.when(pm25 > 250, 6)
        .when(pm25 > 150, 5)
        .when(pm25 >  55, 4)
        .when(pm25 >  35, 3)   # EPA 24h NAAQS
        .when(pm25 >  25, 2)
        .when(pm25 >  15, 1)   # WHO 24h guideline
        .otherwise(0)
    )

    # Single particulate signal — take max so we don't double-count the same event
    particulate_score = F.greatest(aqi_score, pm25_score)

    ozone = F.coalesce(F.col("ozone"), F.lit(0.0))
    ozone_score = (
        F.when(ozone > 200, 5)
        .when(ozone > 125, 4)
        .when(ozone > 105, 3)
        .when(ozone >  85, 2)
        .when(ozone >  70, 1)  # EPA 8h NAAQS
        .otherwise(0)
    )

    humidity = F.coalesce(F.col("humidity"), F.lit(50.0))
    humidity_score = (
        F.when(humidity > 85, 2)
        .when(humidity > 70, 1)
        .when(humidity < 25, 1)
        .otherwise(0)
    )

    temp = F.coalesce(F.col("temperature_c"), F.lit(20.0))
    temp_score = (
        F.when(temp >  38, 2)
        .when(temp >  32, 1)
        .when(temp < -10, 2)
        .when(temp <   0, 1)
        .otherwise(0)
    )

    return {
        "particulate_score": particulate_score,
        "ozone_score":       ozone_score,
        "humidity_score":    humidity_score,
        "temp_score":        temp_score,
    }


def compute_weather_risk_score(components=None):
    """Total weather risk: 0 (clean) up to ~15 (severe pollution + extreme weather)."""
    if components is None:
        components = compute_weather_components()
    return (
        components["particulate_score"]
        + components["ozone_score"]
        + components["humidity_score"]
        + components["temp_score"]
    )


def main():
    args = parse_args()

    spark = (
        SparkSession.builder.appName("stream_risk_scores")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )

    # Load the (pre-filtered) respiratory cohort and cache + materialize so
    # the broadcast happens once at startup rather than on every microbatch.
    features_df = spark.read.parquet(args.features_dir).cache()
    cohort_size = features_df.count()
    print(f"Loaded respiratory cohort: {cohort_size:,} patients "
          f"(broadcasting to executors for stream join)")

    weather_schema = T.StructType(
        [
            T.StructField("event_id", T.StringType()),
            T.StructField("event_time", T.StringType()),
            T.StructField("location_id", T.StringType()),
            T.StructField("city", T.StringType()),
            T.StructField("state", T.StringType()),
            T.StructField("zip", T.StringType()),
            T.StructField("lat", T.DoubleType()),
            T.StructField("lon", T.DoubleType()),
            T.StructField("temperature_c", T.DoubleType()),
            T.StructField("pm25", T.DoubleType()),
            T.StructField("pm10", T.DoubleType()),
            T.StructField("ozone", T.DoubleType()),
            T.StructField("nitrogen_dioxide", T.DoubleType()),
            T.StructField("aqi", T.IntegerType()),
            T.StructField("humidity", T.DoubleType()),
            T.StructField("wind_speed", T.DoubleType()),
            T.StructField("source", T.StringType()),
        ]
    )

    kafka_df = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", args.bootstrap_servers)
        .option("subscribe", args.topic)
        .option("startingOffsets", args.starting_offsets)
        .load()
    )

    parsed_weather_df = (
        kafka_df.select(
            F.from_json(F.col("value").cast("string"), weather_schema).alias("weather"),
            F.col("timestamp").alias("kafka_timestamp"),
        )
        .select("weather.*", "kafka_timestamp")
        .withColumn("event_time", F.to_timestamp("event_time"))
        .filter(F.col("location_id").isNotNull())
    )

    weather_df = parsed_weather_df.alias("weather")
    patient_df = features_df.alias("patient")

    # Explicit broadcast hint: forces a broadcast hash join instead of
    # sort-merge with shuffle. The cohort is small enough to fit in
    # executor memory, so each microbatch's events are joined locally
    # against an in-memory copy of the dim — no per-batch shuffle.
    joined_df = weather_df.join(F.broadcast(patient_df), on="location_id", how="inner")

    # Weather components — kept separate so we can apply condition-specific weighting
    components = compute_weather_components()
    weather_risk = compute_weather_risk_score(components)
    ehr_risk = F.coalesce(F.col("patient.ehr_risk_score"), F.lit(0))

    # Condition-specific environmental sensitivity bonus:
    # Asthma patients are particularly aggravated by ozone.
    # COPD-spectrum patients are particularly aggravated by particulates.
    asthma_flag = F.coalesce(F.col("patient.has_asthma"), F.lit(0))
    copd_flag = F.greatest(
        F.coalesce(F.col("patient.has_copd"),               F.lit(0)),
        F.coalesce(F.col("patient.has_emphysema"),          F.lit(0)),
        F.coalesce(F.col("patient.has_chronic_bronchitis"), F.lit(0)),
    )
    sensitivity_bonus = (
        asthma_flag * components["ozone_score"]
        + copd_flag * components["particulate_score"]
    )

    # Interaction term — bad weather hits already-vulnerable patients harder.
    # Healthy person on bad day: small interaction. COPD patient on bad day: large.
    # Capped at 12 to keep the distribution interpretable.
    interaction = F.least((ehr_risk * weather_risk) / F.lit(20.0), F.lit(12.0))

    final_score = ehr_risk + weather_risk + interaction + sensitivity_bonus

    # New thresholds reflecting the updated range (typical max ~30, hard max ~65)
    scored_df = (
        joined_df
        .withColumn("weather_risk_score", weather_risk)
        .withColumn("final_risk_score", final_score)
        .withColumn(
            "final_risk_level",
            F.when(F.col("final_risk_score") >= 18, F.lit("high"))
            .when(F.col("final_risk_score") >=  8, F.lit("medium"))
            .otherwise(F.lit("low")),
        )
        .select(
            F.col("weather.event_id").alias("event_id"),
            F.col("weather.event_time").alias("event_time"),
            F.col("weather.kafka_timestamp").alias("kafka_timestamp"),
            F.col("location_id"),
            F.col("weather.temperature_c").alias("temperature_c"),
            F.col("weather.pm25").alias("pm25"),
            F.col("weather.pm10").alias("pm10"),
            F.col("weather.ozone").alias("ozone"),
            F.col("weather.nitrogen_dioxide").alias("nitrogen_dioxide"),
            F.col("weather.aqi").alias("aqi"),
            F.col("weather.humidity").alias("humidity"),
            F.col("weather.wind_speed").alias("wind_speed"),
            F.col("patient.patient_id").alias("patient_id"),
            F.col("weather_risk_score"),
            F.col("final_risk_score"),
            F.col("final_risk_level"),
        )
    )

    writer = (
        scored_df.writeStream.format("parquet")
        .option("path", args.output_dir)
        .option("checkpointLocation", args.checkpoint_dir)
        .outputMode("append")
    )

    if args.available_now:
        query = writer.trigger(availableNow=True).start()
    else:
        query = writer.start()

    query.awaitTermination()
    spark.stop()


if __name__ == "__main__":
    main()
