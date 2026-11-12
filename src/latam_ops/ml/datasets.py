"""Carga del dataset de modelado y particiones temporales (nunca aleatorias)."""

from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from latam_ops.comun.config import Lake, cargar_yaml


def columnas_features() -> tuple[list[str], list[str]]:
    """(numéricas, categóricas) declaradas en pipeline.yaml."""
    f = cargar_yaml("pipeline")["features"]
    return list(f["numericas"]), list(f["categoricas"])


def cargar_features(spark: SparkSession, lk: Lake, ruta: str | None = None) -> DataFrame:
    """Lee gold/features_salida. Prohíbe leer réplicas de escalabilidad (scale-test) para EDA o ML."""
    ruta = ruta or lk.ruta("gold_features")
    assert "scale-test" not in ruta, "Las réplicas de scale-test/ nunca se usan para EDA ni ML"
    num, _ = columnas_features()
    df = spark.read.parquet(ruta)
    for c in num:
        df = df.withColumn(c, F.col(c).cast("double"))
    return df


def particionar(df: DataFrame) -> dict[str, DataFrame]:
    """Particiones temporales ya etiquetadas en gold (train, valid, test, demo)."""
    return {s: df.where(F.col("split") == s) for s in ("train", "valid", "test", "demo")}


def con_peso(df: DataFrame, objetivo: str, tasa_positiva: float) -> DataFrame:
    """Peso por clase para balancear (los positivos pesan (1-p)/p)."""
    w_pos = (1 - tasa_positiva) / tasa_positiva if 0 < tasa_positiva < 1 else 1.0
    return df.withColumn("peso", F.when(F.col(objetivo) == 1, F.lit(w_pos)).otherwise(F.lit(1.0)))
