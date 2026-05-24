CREATE OR REPLACE VIEW gold.eta_training AS
WITH base AS (
    SELECT
        f.*,

        ROUND(f.pickup_longitude::numeric, 2)::text
            || '_' ||
        ROUND(f.pickup_latitude::numeric, 2)::text
            AS pickup_zone,

        ROUND(f.dropoff_longitude::numeric, 2)::text
            || '_' ||
        ROUND(f.dropoff_latitude::numeric, 2)::text
            AS dropoff_zone,

        6371 * 2 * ASIN(
            SQRT(
                POWER(
                    SIN(RADIANS((f.dropoff_latitude - f.pickup_latitude) / 2)),
                    2
                ) +
                COS(RADIANS(f.pickup_latitude)) *
                COS(RADIANS(f.dropoff_latitude)) *
                POWER(
                    SIN(RADIANS((f.dropoff_longitude - f.pickup_longitude) / 2)),
                    2
                )
            )
        ) AS trip_distance_km

    FROM gold.fact_trips f
)

SELECT
    pickup_datetime,
    dropoff_datetime,
    pickup_longitude,
    pickup_latitude,
    dropoff_longitude,
    dropoff_latitude,
    passenger_count,
    trip_duration,
    vendor_id,
    store_and_fwd_flag,

    pickup_zone,
    dropoff_zone,

    trip_distance_km,

    CASE
        WHEN trip_duration > 0
        THEN trip_distance_km / (trip_duration / 3600.0)
        ELSE NULL
    END AS avg_speed,

    COUNT(*) OVER (
        PARTITION BY pickup_zone
        ORDER BY pickup_datetime
        RANGE BETWEEN INTERVAL '1 hour' PRECEDING
        AND CURRENT ROW
    ) AS trips_count_1h,

    COUNT(*) OVER (
        PARTITION BY pickup_zone
        ORDER BY pickup_datetime
        RANGE BETWEEN INTERVAL '24 hours' PRECEDING
        AND CURRENT ROW
    ) AS trips_count_24h,

    AVG(trip_duration) OVER (
        PARTITION BY pickup_zone
        ORDER BY pickup_datetime
        RANGE BETWEEN INTERVAL '1 hour' PRECEDING
        AND CURRENT ROW
    ) AS avg_trip_duration_1h,

    AVG(trip_duration) OVER (
        PARTITION BY pickup_zone
        ORDER BY pickup_datetime
        RANGE BETWEEN INTERVAL '24 hours' PRECEDING
        AND CURRENT ROW
    ) AS avg_trip_duration_24h,

    CASE
        WHEN COUNT(*) OVER (
            PARTITION BY pickup_zone
            ORDER BY pickup_datetime
            RANGE BETWEEN INTERVAL '1 hour' PRECEDING
            AND CURRENT ROW
        ) >= 100
        THEN 'high'

        WHEN COUNT(*) OVER (
            PARTITION BY pickup_zone
            ORDER BY pickup_datetime
            RANGE BETWEEN INTERVAL '1 hour' PRECEDING
            AND CURRENT ROW
        ) >= 30
        THEN 'medium'

        ELSE 'low'
    END AS congestion_level,

    EXTRACT(HOUR FROM pickup_datetime)::int AS hour,

    EXTRACT(DOW FROM pickup_datetime)::int AS day_of_week,

    EXTRACT(DOW FROM pickup_datetime)::int IN (0, 6)
        AS is_weekend,

    EXTRACT(HOUR FROM pickup_datetime)::int IN (7, 8, 9, 16, 17, 18)
        AS is_peak_hour

FROM base;



CREATE OR REPLACE VIEW gold.demand_prediction AS
WITH base AS (
    SELECT
        f.*,

        ROUND(f.pickup_longitude::numeric, 2)::text
            || '_' ||
        ROUND(f.pickup_latitude::numeric, 2)::text
            AS pickup_zone

    FROM gold.fact_trips f
)

SELECT
    pickup_datetime,
    pickup_longitude,
    pickup_latitude,
    passenger_count,
    vendor_id,
    store_and_fwd_flag,

    pickup_zone,

    COUNT(*) OVER (
        PARTITION BY pickup_zone
        ORDER BY pickup_datetime
        RANGE BETWEEN INTERVAL '1 hour' PRECEDING
        AND CURRENT ROW
    ) AS trips_count_1h,

    COUNT(*) OVER (
        PARTITION BY pickup_zone
        ORDER BY pickup_datetime
        RANGE BETWEEN INTERVAL '24 hours' PRECEDING
        AND CURRENT ROW
    ) AS trips_count_24h,

    AVG(trip_duration) OVER (
        PARTITION BY pickup_zone
        ORDER BY pickup_datetime
        RANGE BETWEEN INTERVAL '1 hour' PRECEDING
        AND CURRENT ROW
    ) AS avg_trip_duration_1h,

    CASE
        WHEN COUNT(*) OVER (
            PARTITION BY pickup_zone
            ORDER BY pickup_datetime
            RANGE BETWEEN INTERVAL '1 hour' PRECEDING
            AND CURRENT ROW
        ) >= 100
        THEN 'high'

        WHEN COUNT(*) OVER (
            PARTITION BY pickup_zone
            ORDER BY pickup_datetime
            RANGE BETWEEN INTERVAL '1 hour' PRECEDING
            AND CURRENT ROW
        ) >= 30
        THEN 'medium'

        ELSE 'low'
    END AS congestion_level,

    EXTRACT(HOUR FROM pickup_datetime)::int AS hour,

    EXTRACT(DOW FROM pickup_datetime)::int AS day_of_week,

    EXTRACT(DOW FROM pickup_datetime)::int IN (0, 6)
        AS is_weekend,

    EXTRACT(HOUR FROM pickup_datetime)::int IN (7, 8, 9, 16, 17, 18)
        AS is_peak_hour,

    COUNT(*) OVER (
        PARTITION BY pickup_zone
        ORDER BY pickup_datetime
        RANGE BETWEEN CURRENT ROW
        AND INTERVAL '1 hour' FOLLOWING
    ) - 1 AS trips_count_next_1h

FROM base;



CREATE OR REPLACE VIEW gold.traffic_analysis AS
WITH base AS (
    SELECT
        f.*,

        ROUND(f.pickup_longitude::numeric, 2)::text
            || '_' ||
        ROUND(f.pickup_latitude::numeric, 2)::text
            AS pickup_zone,

        ROUND(f.dropoff_longitude::numeric, 2)::text
            || '_' ||
        ROUND(f.dropoff_latitude::numeric, 2)::text
            AS dropoff_zone,

        6371 * 2 * ASIN(
            SQRT(
                POWER(
                    SIN(RADIANS((f.dropoff_latitude - f.pickup_latitude) / 2)),
                    2
                ) +
                COS(RADIANS(f.pickup_latitude)) *
                COS(RADIANS(f.dropoff_latitude)) *
                POWER(
                    SIN(RADIANS((f.dropoff_longitude - f.pickup_longitude) / 2)),
                    2
                )
            )
        ) AS trip_distance_km

    FROM gold.fact_trips f
)

SELECT
    pickup_datetime,
    dropoff_datetime,
    pickup_longitude,
    pickup_latitude,
    dropoff_longitude,
    dropoff_latitude,
    trip_duration,
    passenger_count,

    pickup_zone,
    dropoff_zone,

    trip_distance_km,

    CASE
        WHEN trip_duration > 0
        THEN trip_distance_km / (trip_duration / 3600.0)
        ELSE NULL
    END AS avg_speed,

    COUNT(*) OVER (
        PARTITION BY pickup_zone
        ORDER BY pickup_datetime
        RANGE BETWEEN INTERVAL '1 hour' PRECEDING
        AND CURRENT ROW
    ) AS trips_count_1h,

    COUNT(*) OVER (
        PARTITION BY pickup_zone
        ORDER BY pickup_datetime
        RANGE BETWEEN INTERVAL '24 hours' PRECEDING
        AND CURRENT ROW
    ) AS trips_count_24h,

    AVG(trip_duration) OVER (
        PARTITION BY pickup_zone
        ORDER BY pickup_datetime
        RANGE BETWEEN INTERVAL '1 hour' PRECEDING
        AND CURRENT ROW
    ) AS avg_trip_duration_1h,

    AVG(trip_duration) OVER (
        PARTITION BY pickup_zone
        ORDER BY pickup_datetime
        RANGE BETWEEN INTERVAL '24 hours' PRECEDING
        AND CURRENT ROW
    ) AS avg_trip_duration_24h,

    CASE
        WHEN COUNT(*) OVER (
            PARTITION BY pickup_zone
            ORDER BY pickup_datetime
            RANGE BETWEEN INTERVAL '1 hour' PRECEDING
            AND CURRENT ROW
        ) >= 100
        THEN 'high'

        WHEN COUNT(*) OVER (
            PARTITION BY pickup_zone
            ORDER BY pickup_datetime
            RANGE BETWEEN INTERVAL '1 hour' PRECEDING
            AND CURRENT ROW
        ) >= 30
        THEN 'medium'

        ELSE 'low'
    END AS congestion_level,

    EXTRACT(HOUR FROM pickup_datetime)::int AS hour,

    EXTRACT(DOW FROM pickup_datetime)::int AS day_of_week,

    EXTRACT(DOW FROM pickup_datetime)::int IN (0, 6)
        AS is_weekend,

    EXTRACT(HOUR FROM pickup_datetime)::int IN (7, 8, 9, 16, 17, 18)
        AS is_peak_hour

FROM base;