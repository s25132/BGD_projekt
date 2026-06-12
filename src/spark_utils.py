import os
from pyspark.sql import SparkSession


os.environ["PYSPARK_SUBMIT_ARGS"] = (
    "--conf spark.ui.showConsoleProgress=false "
    "--conf spark.driver.extraJavaOptions='-Dlog4j2.configurationFile=/opt/airflow/log4j2.properties' "
    "--conf spark.executor.extraJavaOptions='-Dlog4j2.configurationFile=/opt/airflow/log4j2.properties' "
    "pyspark-shell"
)


def get_spark(app_name: str = "bgd-medallion-pipeline") -> SparkSession:
    spark_master = os.getenv("SPARK_MASTER", "local[*]")
    jdbc_jar = "/opt/spark/jars/postgresql-42.7.3.jar"

    spark = (
        SparkSession.builder
        .appName(app_name)
        .master(spark_master)
        .config("spark.jars", jdbc_jar)
        .config("spark.driver.extraClassPath", jdbc_jar)
        .config("spark.executor.extraClassPath", jdbc_jar)
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("ERROR")
    return spark