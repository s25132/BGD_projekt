from sqlalchemy import text


def build_gold(engine):
    with engine.begin() as conn:
        print("Building gold.dim_vendor...")
        conn.execute(text("""
            INSERT INTO gold.dim_vendor (
                vendor_id,
                updated_at
            )
            SELECT DISTINCT
                s.vendor_id,
                CURRENT_TIMESTAMP
            FROM silver.transactions_clean s
            WHERE s.is_valid = true
              AND s.vendor_id IS NOT NULL
            ON CONFLICT (vendor_id) DO UPDATE
            SET updated_at = CURRENT_TIMESTAMP
        """))

        print("Building gold.dim_date...")
        conn.execute(text("""
            INSERT INTO gold.dim_date (
                date_id,
                year,
                month,
                day
            )
            SELECT DISTINCT
                s.pickup_datetime::date AS date_id,
                EXTRACT(YEAR FROM s.pickup_datetime)::int AS year,
                EXTRACT(MONTH FROM s.pickup_datetime)::int AS month,
                EXTRACT(DAY FROM s.pickup_datetime)::int AS day
            FROM silver.transactions_clean s
            WHERE s.is_valid = true
              AND s.pickup_datetime IS NOT NULL
            ON CONFLICT (date_id) DO UPDATE
            SET
                year = EXCLUDED.year,
                month = EXCLUDED.month,
                day = EXCLUDED.day
        """))

        print("Building gold.fact_trips...")
        conn.execute(text("""
            INSERT INTO gold.fact_trips (
                id,
                vendor_id,
                date_id,
                pickup_datetime,
                dropoff_datetime,
                passenger_count,
                pickup_longitude,
                pickup_latitude,
                dropoff_longitude,
                dropoff_latitude,
                store_and_fwd_flag,
                trip_duration,
                updated_at
            )
            SELECT
                s.id,
                s.vendor_id,
                s.pickup_datetime::date AS date_id,
                s.pickup_datetime,
                s.dropoff_datetime,
                s.passenger_count,
                s.pickup_longitude,
                s.pickup_latitude,
                s.dropoff_longitude,
                s.dropoff_latitude,
                s.store_and_fwd_flag,
                s.trip_duration,
                CURRENT_TIMESTAMP
            FROM silver.transactions_clean s
            WHERE s.is_valid = true
              AND s.id IS NOT NULL
              AND s.id <> ''
            ON CONFLICT (id) DO UPDATE
            SET
                vendor_id = EXCLUDED.vendor_id,
                date_id = EXCLUDED.date_id,
                pickup_datetime = EXCLUDED.pickup_datetime,
                dropoff_datetime = EXCLUDED.dropoff_datetime,
                passenger_count = EXCLUDED.passenger_count,
                pickup_longitude = EXCLUDED.pickup_longitude,
                pickup_latitude = EXCLUDED.pickup_latitude,
                dropoff_longitude = EXCLUDED.dropoff_longitude,
                dropoff_latitude = EXCLUDED.dropoff_latitude,
                store_and_fwd_flag = EXCLUDED.store_and_fwd_flag,
                trip_duration = EXCLUDED.trip_duration,
                updated_at = CURRENT_TIMESTAMP
        """))
        
    print("GOLD complete")