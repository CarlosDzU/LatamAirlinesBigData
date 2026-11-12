"""Utilidades compartidas por los jobs de transformación."""

from __future__ import annotations

import argparse

from pyspark.sql import DataFrame, SparkSession

from latam_ops.comun.config import cargar_yaml, periodo_meses


def argumentos(descripcion: str, argv: list[str] | None = None) -> argparse.Namespace:
    """Argumentos estándar --desde/--hasta (por defecto, el periodo completo de pipeline.yaml)."""
    per = cargar_yaml("pipeline")["periodo"]
    p = argparse.ArgumentParser(description=descripcion)
    p.add_argument("--desde", default=per["inicio"])
    p.add_argument("--hasta", default=per["fin"])
    p.add_argument("--dry-run", action="store_true")
    return p.parse_args(argv)


def filtro_meses(desde: str, hasta: str, col_anio: str = "year", col_mes: str = "month") -> str:
    """Expresión SQL que selecciona las particiones year/month del intervalo."""
    a1, m1 = map(int, desde.split("-"))
    a2, m2 = map(int, hasta.split("-"))
    return f"({col_anio} * 100 + {col_mes}) BETWEEN {a1 * 100 + m1} AND {a2 * 100 + m2}"


def mes_anterior(aaaamm: str) -> str:
    """'2024-01' -> '2023-12'."""
    a, m = map(int, aaaamm.split("-"))
    return f"{a - 1}-12" if m == 1 else f"{a}-{m - 1:02d}"


def leer_parquet_si_existe(spark: SparkSession, ruta: str) -> DataFrame | None:
    """Lee una tabla Parquet o devuelve None si todavía no existe."""
    try:
        return spark.read.parquet(ruta)
    except Exception as exc:  # AnalysisException: ruta inexistente
        if "PATH_NOT_FOUND" in str(exc) or "does not exist" in str(exc) or "Unable to infer schema" in str(exc):
            return None
        raise


def escribir_particionado(df: DataFrame, ruta: str, particiones: list[str] | None = None) -> None:
    """Escritura idempotente: sobrescribe solo las particiones presentes (partitionOverwriteMode=dynamic)."""
    if particiones:
        # Un archivo por partición: sin esto cada tarea deja un archivo en cada mes y el commit en S3
        # (copiar + borrar por archivo) se vuelve eterno. Con 200 tareas x 36 meses eran ~7 400 archivos.
        df = df.repartition(*particiones)
    escritor = df.write.mode("overwrite").format("parquet")
    if particiones:
        escritor = escritor.partitionBy(*particiones)
    escritor.save(ruta)


def meses_str(desde: str, hasta: str) -> list[str]:
    """Lista 'AAAA-MM' del intervalo."""
    return [f"{a}-{m:02d}" for a, m in periodo_meses(desde, hasta)]
