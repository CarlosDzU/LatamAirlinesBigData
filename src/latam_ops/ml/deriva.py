"""Monitoreo de deriva (PSI) de las features numéricas: train vs cada mes posterior -> gold/monitoreo_deriva.

PSI < 0.1 estable; 0.1–0.25 moderada; > 0.25 significativa (reentrenar).
Uso: python -m latam_ops.ml.deriva
"""

from __future__ import annotations

import math

from pyspark.sql import functions as F

from latam_ops.comun.config import lake
from latam_ops.comun.contratos import aplicar_contrato
from latam_ops.comun.corridas import registrar_corrida
from latam_ops.comun.sesion_spark import crear_sesion
from latam_ops.ml.datasets import cargar_features, columnas_features


def psi(esperado: list[float], observado: list[float]) -> float:
    """Population Stability Index entre dos distribuciones por cubeta (proporciones)."""
    total = 0.0
    for e, o in zip(esperado, observado, strict=True):
        e, o = max(e, 1e-6), max(o, 1e-6)
        total += (o - e) * math.log(o / e)
    return round(total, 5)


def _proporciones(df, col: str, cortes: list[float]) -> list[float]:
    expr = F.lit(len(cortes))
    for i in reversed(range(len(cortes))):
        expr = F.when(F.col(col) <= cortes[i], F.lit(i)).otherwise(expr)
    conteos = {r["b"]: r["n"] for r in df.where(F.col(col).isNotNull()).select(expr.alias("b")).groupBy("b").agg(F.count("*").alias("n")).collect()}
    total = sum(conteos.values()) or 1
    return [conteos.get(i, 0) / total for i in range(len(cortes) + 1)]


def ejecutar() -> int:
    lk = lake()
    spark = crear_sesion("deriva")
    with registrar_corrida("deriva") as info:
        df = cargar_features(spark, lk)
        train = df.where("split = 'train'").cache()
        num, _ = columnas_features()
        filas = []
        meses = [r["am"] for r in df.where("split != 'train'").select(F.format_string("%04d-%02d", "year", "month").alias("am")).distinct().collect()]
        for col in num:
            cortes = sorted(set(train.approxQuantile(col, [i / 10 for i in range(1, 10)], 0.01)))
            if not cortes:
                continue
            base = _proporciones(train, col, cortes)
            for am in sorted(meses):
                a, m = map(int, am.split("-"))
                obs = _proporciones(df.where((F.col("year") == a) & (F.col("month") == m)), col, cortes)
                valor = psi(base, obs)
                nivel = "estable" if valor < 0.1 else ("moderada" if valor < 0.25 else "significativa")
                filas.append((am, col, valor, nivel))
        out = spark.createDataFrame(filas, "anio_mes string, feature string, psi double, nivel string")
        aplicar_contrato(out, "gold", "monitoreo_deriva").coalesce(1).write.mode("overwrite").parquet(lk.ruta("gold_monitoreo"))
        info["filas_salida"] = len(filas)
        return len(filas)


def main() -> None:
    ejecutar()


if __name__ == "__main__":
    main()
