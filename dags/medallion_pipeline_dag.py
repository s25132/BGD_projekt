import os
import time

import boto3
from airflow.sdk import dag, task, get_current_context
from sqlalchemy import create_engine
from sqlalchemy.exc import OperationalError

from src.raw import (
    load_raw,
    mark_file_as_loaded,
    is_file_already_loaded,
    compute_file_hash,
)
from src.silver import build_silver_spark
from src.gold import build_gold


DB_URL = os.getenv(
    "DB_URL",
    "postgresql+psycopg2://postgres:postgres@postgres:5432/medallion"
)

JDBC_URL = os.getenv(
    "JDBC_URL",
    "jdbc:postgresql://postgres:5432/medallion"
)

CHUNK_SIZE = int(os.getenv("CHUNK_SIZE", "10000"))

DB_PROPERTIES = {
    "user": os.getenv("POSTGRES_USER", "postgres"),
    "password": os.getenv("POSTGRES_PASSWORD", "postgres"),
    "driver": "org.postgresql.Driver",
}

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadmin")
MINIO_BUCKET = os.getenv("MINIO_BUCKET", "feature-store")
MINIO_DEFAULT_KEY = os.getenv("MINIO_DEFAULT_KEY", "raw/taxi1.csv")


def get_engine():
    for _ in range(30):
        try:
            engine = create_engine(DB_URL)
            with engine.connect():
                pass
            return engine
        except OperationalError:
            print("Waiting for the database...")
            time.sleep(1)

    raise Exception("Could not connect to the database")


def download_from_minio(bucket: str, key: str) -> str:
    local_path = f"/tmp/{os.path.basename(key)}"

    print(f"Downloading from MinIO: s3://{bucket}/{key}")
    print(f"Local file path: {local_path}")

    s3 = boto3.client(
        "s3",
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY,
    )

    s3.download_file(bucket, key, local_path)

    print("Download complete")
    return local_path


@dag(
    dag_id="medallion_pipeline",
    description="Medallion pipeline with Airflow, Spark, PostgreSQL and MinIO",
    schedule=None,
    catchup=False,
    tags=["medallion", "airflow", "spark", "postgres", "minio"],
    default_args={"owner": "airflow", "retries": 1},
)
def medallion_pipeline():

    @task
    def raw_ingestion():

        context = get_current_context()
        dag_conf = context.get("dag_run").conf if context.get("dag_run") else {}

        load_mode = dag_conf.get(
                "load_mode",
                os.getenv("LOAD_MODE", "incremental")
        )

        if load_mode not in ["incremental", "full"]:
            raise ValueError(f"Invalid load_mode: {load_mode}")

        minio_bucket = dag_conf.get(
            "bucket",
            os.getenv("MINIO_BUCKET", "feature-store")
        )

        minio_key = dag_conf.get(
            "file",
             os.getenv("MINIO_DEFAULT_KEY", "raw/taxi1.csv")
        )

        csv_file = download_from_minio(minio_bucket, minio_key)

        engine = get_engine()

        file_name = os.path.basename(minio_key)
        file_hash = compute_file_hash(csv_file)

        print(f"Mode: {load_mode}")
        print(f"MinIO source: s3://{minio_bucket}/{minio_key}")
        print(f"Downloaded file: {csv_file}")

        if load_mode == "incremental":
            if is_file_already_loaded(engine, file_name, file_hash):
                print(f"File {file_name} already loaded — skipping RAW")
                return {"should_continue": False, "load_mode": load_mode}

        print(f"Starting RAW ingestion for file: {file_name}")
        load_raw(engine, csv_file, CHUNK_SIZE)
        mark_file_as_loaded(engine, file_name, file_hash)
        print("RAW ingestion complete")

        return {"should_continue": True, "load_mode": load_mode}

    @task
    def silver_build(raw_result: dict):

        should_continue = raw_result["should_continue"]
        load_mode = raw_result["load_mode"]

        if not should_continue and load_mode == "incremental":
            print("Skipping SILVER")
            return {"should_continue": False, "load_mode": load_mode}

        engine = get_engine()

        print(f"Starting SILVER build (mode={load_mode})...")
        build_silver_spark(engine, JDBC_URL, DB_PROPERTIES, load_mode)
        print("SILVER complete")

        return {"should_continue": True, "load_mode": load_mode}

    @task
    def gold_build(silver_result: dict):

        should_continue = silver_result["should_continue"]
        load_mode = silver_result["load_mode"]

        if not should_continue and load_mode == "incremental":
            print("Skipping GOLD")
            return

        engine = get_engine()

        print(f"Starting GOLD build (mode={load_mode})...")
        build_gold(engine)
        print("GOLD complete")

    raw_result = raw_ingestion()
    silver_result = silver_build(raw_result)
    gold_build(silver_result)


dag = medallion_pipeline()