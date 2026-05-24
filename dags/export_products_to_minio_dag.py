import os
import tempfile
from datetime import date

import boto3
import pandas as pd
from airflow.sdk import dag, task, get_current_context
from sqlalchemy import create_engine


DB_URL = os.getenv(
    "DB_URL",
    "postgresql+psycopg2://postgres:postgres@postgres:5432/medallion"
)

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadmin")
MINIO_BUCKET = os.getenv("MINIO_BUCKET", "feature-store")


PRODUCTS = {
    "eta_training": "gold.eta_training",
    "demand_prediction": "gold.demand_prediction",
    "traffic_analysis": "gold.traffic_analysis",
}


def get_engine():
    return create_engine(DB_URL)


def get_s3_client():
    return boto3.client(
        "s3",
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY,
    )


@dag(
    dag_id="export_products_to_minio",
    description="Export GOLD products to MinIO as Parquet files",
    schedule=None,
    catchup=False,
    tags=["gold", "products", "feature-store", "minio", "parquet"],
    default_args={"owner": "airflow", "retries": 1},
)
def export_products_to_minio():

    @task
    def export_product(product_name: str, view_name: str):
        context = get_current_context()
        dag_conf = context.get("dag_run").conf if context.get("dag_run") else {}

        max_rows = int(dag_conf.get("max_rows", 1000))
        export_date = date.today().isoformat()

        query = f"""
            SELECT *
            FROM {view_name}
            ORDER BY pickup_datetime DESC
            LIMIT {max_rows}
        """

        print(f"Exporting product: {product_name}")
        print(f"Source view: {view_name}")
        print(f"Max rows: {max_rows}")
        print(f"Export date: {export_date}")

        engine = get_engine()
        df = pd.read_sql(query, engine)

        local_file = os.path.join(
            tempfile.gettempdir(),
            f"{product_name}_{export_date}.parquet"
        )

        df.to_parquet(local_file, index=False)

        s3_key = (
            f"products/{product_name}/"
            f"export_date={export_date}/"
            f"data.parquet"
        )

        s3 = get_s3_client()
        s3.upload_file(local_file, MINIO_BUCKET, s3_key)

        print(f"Rows exported: {len(df)}")
        print(f"Uploaded to: s3://{MINIO_BUCKET}/{s3_key}")

        return {
            "product": product_name,
            "rows": len(df),
            "s3_path": f"s3://{MINIO_BUCKET}/{s3_key}",
        }

    for product_name, view_name in PRODUCTS.items():
        export_product.override(task_id=f"export_{product_name}")(
            product_name,
            view_name
        )


dag = export_products_to_minio()