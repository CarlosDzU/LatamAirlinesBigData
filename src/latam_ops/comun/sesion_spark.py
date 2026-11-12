"""Creación de la SparkSession con la configuración del proyecto."""

from __future__ import annotations

import os

from pyspark.sql import SparkSession

from latam_ops.comun.config import cargar_yaml, modo


def crear_sesion(nombre: str) -> SparkSession:
    """SparkSession local[*] (modo local) o conectada al clúster (modo aws, toma spark-defaults.conf).

    Siempre: zona horaria UTC, sobrescritura dinámica de particiones (idempotencia) y AQE.
    """
    cfg = cargar_yaml("pipeline")["spark"]
    constructor = SparkSession.builder.appName(f"{cfg['app_prefijo']}-{nombre}")
    if modo() == "local":
        constructor = (
            constructor.master(os.environ.get("LATAM_SPARK_MASTER", "local[*]"))
            .config("spark.sql.shuffle.partitions", str(cfg["particiones_shuffle_local"]))
            .config("spark.driver.memory", os.environ.get("LATAM_DRIVER_MEM", "2g"))
            .config("spark.ui.showConsoleProgress", "false")
        )
    sesion = (
        constructor.config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.parquet.compression.codec", "snappy")
        .getOrCreate()
    )
    sesion.sparkContext.setLogLevel(os.environ.get("LATAM_SPARK_LOG", "WARN"))
    return sesion
