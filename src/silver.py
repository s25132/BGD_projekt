from pyspark.sql import functions as F
from sqlalchemy import text

from raw import get_raw_batches
from spark_utils import get_spark


def get_silver_batches(engine) -> set[int]:
    with engine.connect() as conn:
        rows = conn.execute(
            text("""
                SELECT batch_no
                FROM silver.batch_log
                ORDER BY batch_no
            """)
        ).fetchall()
    return {int(row[0]) for row in rows}


def build_silver_spark(engine, jdbc_url, db_properties, load_mode="incremental"):
    raw_batches = get_raw_batches(engine)
    silver_batches = get_silver_batches(engine)

    if load_mode == "incremental":
        batches_to_process = raw_batches - silver_batches
    else:
        print("FULL mode → processing ALL batches")
        batches_to_process = raw_batches

    batches_to_process = sorted(batches_to_process)

    if not batches_to_process:
        print("No new batches to process in SILVER")
        return

    spark = get_spark("silver-layer")

    for batch_no in batches_to_process:
        print(f"Processing batch {batch_no} with Spark...")

        batch_query = f"""
        (
            SELECT
                batch_no,
                source_file,
                file_hash,
                id,
                vendor_id,
                pickup_datetime,
                dropoff_datetime,
                passenger_count,
                pickup_longitude,
                pickup_latitude,
                dropoff_longitude,
                dropoff_latitude,
                store_and_fwd_flag,
                trip_duration
            FROM raw.transactions_raw
            WHERE batch_no = {batch_no}
        ) AS raw_batch
        """

        batch_df = spark.read.jdbc(
            url=jdbc_url,
            table=batch_query,
            properties=db_properties
        )

        df = (
            batch_df
            .withColumn("id", F.trim(F.col("id").cast("string")))
            .withColumn("vendor_id", F.expr("try_cast(vendor_id as int)"))
            .withColumn("pickup_datetime", F.try_to_timestamp(F.col("pickup_datetime")))
            .withColumn("dropoff_datetime", F.try_to_timestamp(F.col("dropoff_datetime")))
            .withColumn("passenger_count", F.expr("try_cast(passenger_count as int)"))
            .withColumn("pickup_longitude", F.expr("try_cast(pickup_longitude as double)"))
            .withColumn("pickup_latitude", F.expr("try_cast(pickup_latitude as double)"))
            .withColumn("dropoff_longitude", F.expr("try_cast(dropoff_longitude as double)"))
            .withColumn("dropoff_latitude", F.expr("try_cast(dropoff_latitude as double)"))
            .withColumn("store_and_fwd_flag", F.upper(F.trim(F.col("store_and_fwd_flag").cast("string"))))
            .withColumn("trip_duration", F.expr("try_cast(trip_duration as int)"))
        )

        df = (
            df
            .withColumn(
                "validation_error",
                F.concat(
                    F.when(
                        F.col("id").isNull() | (F.col("id") == ""),
                        F.lit("missing id; ")
                    ).otherwise(F.lit("")),

                    F.when(
                        F.col("vendor_id").isNull(),
                        F.lit("bad vendor_id; ")
                    ).otherwise(F.lit("")),

                    F.when(
                        F.col("pickup_datetime").isNull(),
                        F.lit("bad pickup_datetime; ")
                    ).otherwise(F.lit("")),

                    F.when(
                        F.col("dropoff_datetime").isNull(),
                        F.lit("bad dropoff_datetime; ")
                    ).otherwise(F.lit("")),

                    F.when(
                        F.col("dropoff_datetime") < F.col("pickup_datetime"),
                        F.lit("dropoff before pickup; ")
                    ).otherwise(F.lit("")),

                    F.when(
                        F.col("passenger_count").isNull() | (F.col("passenger_count") <= 0),
                        F.lit("bad passenger_count; ")
                    ).otherwise(F.lit("")),

                    F.when(
                        F.col("pickup_longitude").isNull() |
                        F.col("pickup_latitude").isNull() |
                        F.col("dropoff_longitude").isNull() |
                        F.col("dropoff_latitude").isNull(),
                        F.lit("bad coordinates; ")
                    ).otherwise(F.lit("")),

                    F.when(
                        ~F.col("store_and_fwd_flag").isin("Y", "N"),
                        F.lit("bad store_and_fwd_flag; ")
                    ).otherwise(F.lit("")),

                    F.when(
                        F.col("trip_duration").isNull() | (F.col("trip_duration") <= 0),
                        F.lit("bad trip_duration; ")
                    ).otherwise(F.lit(""))
                )
            )
            .withColumn("is_valid", F.col("validation_error") == "")
            .withColumn("updated_at", F.current_timestamp())
        )

        df = df.select(
            "id",
            "batch_no",
            "source_file",
            "file_hash",
            "vendor_id",
            "pickup_datetime",
            "dropoff_datetime",
            "passenger_count",
            "pickup_longitude",
            "pickup_latitude",
            "dropoff_longitude",
            "dropoff_latitude",
            "store_and_fwd_flag",
            "trip_duration",
            "is_valid",
            "validation_error",
            "updated_at"
        )

        stage_table = f"silver.transactions_clean_stage_{batch_no}"

        df.write.mode("overwrite").jdbc(
            url=jdbc_url,
            table=stage_table,
            properties=db_properties
        )

        with engine.begin() as conn:
            conn.execute(text(f"""
                INSERT INTO silver.transactions_clean (
                    id,
                    batch_no,
                    source_file,
                    file_hash,
                    vendor_id,
                    pickup_datetime,
                    dropoff_datetime,
                    passenger_count,
                    pickup_longitude,
                    pickup_latitude,
                    dropoff_longitude,
                    dropoff_latitude,
                    store_and_fwd_flag,
                    trip_duration,
                    is_valid,
                    validation_error,
                    updated_at
                )
                SELECT
                    id,
                    batch_no,
                    source_file,
                    file_hash,
                    vendor_id,
                    pickup_datetime,
                    dropoff_datetime,
                    passenger_count,
                    pickup_longitude,
                    pickup_latitude,
                    dropoff_longitude,
                    dropoff_latitude,
                    store_and_fwd_flag,
                    trip_duration,
                    is_valid,
                    validation_error,
                    updated_at
                FROM {stage_table}
                WHERE is_valid = TRUE
                ON CONFLICT (id) DO UPDATE
                SET
                    batch_no = EXCLUDED.batch_no,
                    source_file = EXCLUDED.source_file,
                    file_hash = EXCLUDED.file_hash,
                    vendor_id = EXCLUDED.vendor_id,
                    pickup_datetime = EXCLUDED.pickup_datetime,
                    dropoff_datetime = EXCLUDED.dropoff_datetime,
                    passenger_count = EXCLUDED.passenger_count,
                    pickup_longitude = EXCLUDED.pickup_longitude,
                    pickup_latitude = EXCLUDED.pickup_latitude,
                    dropoff_longitude = EXCLUDED.dropoff_longitude,
                    dropoff_latitude = EXCLUDED.dropoff_latitude,
                    store_and_fwd_flag = EXCLUDED.store_and_fwd_flag,
                    trip_duration = EXCLUDED.trip_duration,
                    is_valid = EXCLUDED.is_valid,
                    validation_error = EXCLUDED.validation_error,
                    updated_at = CURRENT_TIMESTAMP
            """))

            conn.execute(
                text("""
                    INSERT INTO silver.batch_log (batch_no)
                    VALUES (:batch_no)
                    ON CONFLICT (batch_no) DO NOTHING
                """),
                {"batch_no": int(batch_no)}
            )

            conn.execute(text(f"DROP TABLE IF EXISTS {stage_table}"))

        print(f"Batch {batch_no} processed successfully")

    spark.stop()
    print("SILVER Spark complete")